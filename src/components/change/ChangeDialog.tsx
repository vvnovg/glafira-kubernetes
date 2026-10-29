import { useMutation, useQueryClient } from "@tanstack/react-query";
import { type KeyboardEvent, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { AppError, Change, ChangePreview, Confirmation, ContainerResources, Proposal, ResourceValues } from "../../api/types";
import { trapTab } from "../../lib/focusTrap";
import { healthKey } from "../../lib/useHealth";
import { useConnection } from "../../lib/useConnection";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { changeObject, hasValues, warningText, withValues } from "./changes";

/** Текущие requests и limits контейнера из шаблона — подсказки в полях формы ресурсов. */
export type ResourceHints = Record<string, { requests: Record<string, string>; limits: Record<string, string> }>;

interface Props {
  connectionId: string;
  /** Изменение, с которого форма начинается: пустая форма, рекомендация или предложение LLM. */
  initial: Change;
  hints?: ResourceHints;
  /** Предупреждение над формой: например, низкая уверенность рекомендации. */
  notice?: string;
  /** Применяется предложение LLM: строка над формой и `proposalId` при применении. */
  proposal?: Proposal;
  onClose: () => void;
}

/**
 * Форма изменения (спец. §9) поверх экрана: поля вида изменения, «Проверить» — «было → станет», предупреждения и
 * server-side dry-run, затем подтверждение (имя prod-подключения, имя пода без контроллера) и применение с версией
 * предпросмотра. Любая правка формы сбрасывает предпросмотр. Изменившийся объект (`CONFLICT`) — «Обновить и повторить».
 */
export function ChangeDialog({ connectionId, initial, hints, notice, proposal, onClose }: Props) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const connection = useConnection(connectionId);
  const [change, setChange] = useState<Change>(initial);
  const [preview, setPreview] = useState<ChangePreview | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  const check = useMutation({ mutationFn: (c: Change) => api.previewChange(connectionId, withValues(c)), onSuccess: setPreview });
  const apply = useMutation({
    mutationFn: ({ p, c }: { p: ChangePreview; c: Confirmation }) =>
      api.applyChange(connectionId, p.change, p.version, c.confirm, p.confirmPodName ? c.typedName : null, proposal?.id ?? null),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["workload", connectionId] });
      void qc.invalidateQueries({ queryKey: ["pod", connectionId] });
      void qc.invalidateQueries({ queryKey: ["resources", connectionId] });
      void qc.invalidateQueries({ queryKey: healthKey(connectionId) });
      onClose();
    },
  });
  const busy = check.isPending || apply.isPending;
  const readOnly = connection?.readOnly === true;

  useEffect(() => {
    const returnTo = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    rootRef.current?.focus();
    return () => {
      if (returnTo?.isConnected) returnTo.focus();
    };
  }, []);

  const conflict = (apply.error as AppError | null)?.code === "CONFLICT";
  // Предпросмотр или конфликт убирают кнопку, на которой был фокус («Проверить», «Применить»): фокус уходил в
  // body, и Esc с ловушкой Tab переставали работать. Он возвращается диалогу, если его не взяло поле подтверждения.
  useEffect(() => {
    const root = rootRef.current;
    if ((preview || conflict) && root && !root.contains(document.activeElement)) root.focus();
  }, [preview, conflict]);

  const edit = (c: Change) => {
    setChange(c);
    setPreview(null);
    check.reset();
    apply.reset();
  };
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Tab") trapTab(rootRef.current, e);
    // Esc внутри подтверждения уже закрыл форму его «Отменой» (`ConfirmDialog`): второй раз `onClose` не зовётся.
    const inConfirm = e.target instanceof Element && e.target.closest(".confirm") !== null;
    if (e.key === "Escape" && !busy && !e.defaultPrevented && !inConfirm) {
      e.stopPropagation();
      onClose();
    }
  };
  // Применить можно только против той версии, которую показал предпросмотр: после CONFLICT или без версии (Rust
  // ответит CONFLICT) вместо подтверждения — «Обновить и повторить».
  const stale = conflict || (preview !== null && preview.version == null);
  const title = `${t(`change.title.${change.kind}`)}: ${changeObject(change)}`;

  return (
    <div className="modal-backdrop">
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby="change-title" tabIndex={-1} ref={rootRef} onKeyDown={onKeyDown}>
        <h3 id="change-title">{title}</h3>
        {proposal && <p className="muted">{t("health.proposals.applying", { title: proposal.title })}</p>}
        {notice && <p className="problem">{notice}</p>}
        {readOnly && <p className="problem">{t("change.readOnly")}</p>}
        <ChangeFields change={change} hints={hints} disabled={busy} onChange={edit} />
        {!preview && (
          <div className="toolbar">
            <button type="button" className="primary" disabled={busy || readOnly || !hasValues(change)} onClick={() => check.mutate(change)}>
              {t("change.preview")}
            </button>
            <button type="button" disabled={busy} onClick={onClose}>
              {t("common.cancel")}
            </button>
          </div>
        )}
        {check.error && <ErrorBanner error={check.error} />}
        {preview && <PreviewView preview={preview} />}
        {preview?.dryRun.status === "rejected" && (
          <div className="toolbar">
            <button type="button" onClick={onClose}>
              {t("common.close")}
            </button>
          </div>
        )}
        {preview && preview.dryRun.status !== "rejected" && connection && !stale && (
          <ConfirmDialog
            connection={connection}
            title={t("change.confirmTitle")}
            confirmLabel={change.kind === "deletePod" ? t("change.delete") : t("change.apply")}
            danger={change.kind === "deletePod"}
            objectName={preview.confirmPodName && change.kind === "deletePod" ? change.pod : undefined}
            objectLabel={t("change.podName")}
            busy={busy}
            onConfirm={(c) => apply.mutate({ p: preview, c })}
            onCancel={onClose}
          />
        )}
        {apply.error && <ErrorBanner error={apply.error} />}
        {stale && preview?.dryRun.status !== "rejected" && (
          <div className="toolbar">
            <button type="button" className="primary" disabled={busy} onClick={() => edit(change)}>
              {t("change.refreshRetry")}
            </button>
            <button type="button" disabled={busy} onClick={onClose}>
              {t("common.close")}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

/** «Было → станет», предупреждения и итог dry-run. */
function PreviewView({ preview }: { preview: ChangePreview }) {
  const { t } = useTranslation();
  const value = (v: string | null) => v ?? t("change.notSet");
  return (
    <>
      <h4>{t("change.diffTitle")}</h4>
      {preview.diff.length === 0 ? (
        <p className="muted">{t("change.noDiff")}</p>
      ) : (
        <table className="diff-table">
          <thead>
            <tr>
              <th>{t("change.col.container")}</th>
              <th>{t("change.col.field")}</th>
              <th>{t("change.col.before")}</th>
              <th>{t("change.col.after")}</th>
            </tr>
          </thead>
          <tbody>
            {preview.diff.map((d) => (
              <tr key={`${d.container ?? ""}:${d.field}`}>
                <td>{d.container ?? "—"}</td>
                <td>{t(`change.diff.${d.field}`)}</td>
                <td>{value(d.before)}</td>
                <td className="after">{value(d.after)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {preview.warnings.map((w) => (
        <p key={JSON.stringify(w)} className="problem">
          {warningText(w, t)}
        </p>
      ))}
      {preview.dryRun.status === "passed" && <p className="muted">✅ {t("change.dryRun.passed")}</p>}
      {preview.dryRun.status === "skipped" && <p className="muted">{t("change.dryRun.skipped")}</p>}
      {preview.dryRun.status === "rejected" && (
        <>
          <p className="problem">{t("change.dryRun.rejected")}</p>
          <ErrorBanner error={preview.dryRun.error} />
        </>
      )}
    </>
  );
}

interface FieldsProps {
  change: Change;
  hints?: ResourceHints;
  disabled: boolean;
  onChange: (c: Change) => void;
}

/** Поля вида изменения. */
function ChangeFields({ change, hints, disabled, onChange }: FieldsProps) {
  const { t } = useTranslation();
  const int = (s: string) => (s.trim() === "" ? NaN : Number(s));
  switch (change.kind) {
    case "setResources":
      return <ResourceFields change={change} hints={hints} disabled={disabled} onChange={onChange} />;
    case "scale":
      return (
        <label className="form">
          {t("change.replicas")}
          <input type="number" min={0} value={Number.isNaN(change.replicas) ? "" : change.replicas} disabled={disabled} onChange={(e) => onChange({ ...change, replicas: int(e.target.value) })} />
        </label>
      );
    case "setHpaBounds":
      return (
        <div className="form">
          <label htmlFor="hpa-min">{t("change.minReplicas")}</label>
          <input
            id="hpa-min"
            type="number"
            min={1}
            placeholder={t("change.keep")}
            value={change.minReplicas ?? ""}
            disabled={disabled}
            onChange={(e) => onChange({ ...change, minReplicas: e.target.value.trim() === "" ? undefined : int(e.target.value) })}
          />
          <label htmlFor="hpa-max">{t("change.maxReplicas")}</label>
          <input id="hpa-max" type="number" min={1} value={Number.isNaN(change.maxReplicas) ? "" : change.maxReplicas} disabled={disabled} onChange={(e) => onChange({ ...change, maxReplicas: int(e.target.value) })} />
        </div>
      );
    case "rolloutRestart":
      return <p className="muted">{change.workload.kind === "DeploymentConfig" ? t("change.restartDc") : t("change.restartNote")}</p>;
    case "deletePod":
      return <p className="muted">{t("change.deleteNote")}</p>;
  }
}

type Part = "requests" | "limits";
type Res = "cpu" | "memory";
const CELLS: [Part, Res][] = [
  ["requests", "cpu"],
  ["requests", "memory"],
  ["limits", "cpu"],
  ["limits", "memory"],
];

/**
 * Таблица контейнеров: пустое поле — «не менять» (в подсказке — текущее значение), «✕» — убрать значение
 * (`null`), повторный щелчок возвращает «не менять».
 */
function ResourceFields({ change, hints, disabled, onChange }: FieldsProps & { change: Extract<Change, { kind: "setResources" }> }) {
  const { t } = useTranslation();
  const set = (i: number, part: Part, res: Res, v: string | null | undefined) => {
    const containers = change.containers.map((c, j): ContainerResources => {
      if (j !== i) return c;
      const values: ResourceValues = { ...c[part] };
      if (v === undefined) delete values[res];
      else values[res] = v;
      return { ...c, [part]: values };
    });
    onChange({ ...change, containers });
  };
  return (
    <table className="resource-form">
      <thead>
        <tr>
          <th>{t("change.col.container")}</th>
          {CELLS.map(([p, r]) => (
            <th key={`${p}.${r}`}>{t(`change.diff.${p}.${r}`)}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {change.containers.map((c, i) => (
          <tr key={c.name}>
            <td>{c.name}</td>
            {CELLS.map(([p, r]) => {
              const v = c[p][r];
              const now = hints?.[c.name]?.[p]?.[r];
              const label = `${c.name} ${t(`change.diff.${p}.${r}`)}`;
              return (
                <td key={`${p}.${r}`}>
                  <input
                    aria-label={label}
                    className={v === null ? "removed" : undefined}
                    value={v ?? ""}
                    placeholder={v === null ? t("change.removed") : now ? t("change.current", { value: now }) : t("change.keep")}
                    disabled={disabled || v === null}
                    onChange={(e) => set(i, p, r, e.target.value.trim() === "" ? undefined : e.target.value)}
                  />
                  <button
                    type="button"
                    className="icon"
                    aria-label={`${t("change.remove")}: ${label}`}
                    aria-pressed={v === null}
                    title={t("change.remove")}
                    disabled={disabled}
                    onClick={() => set(i, p, r, v === null ? undefined : null)}
                  >
                    ✕
                  </button>
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
