import { useQuery, useQueryClient } from "@tanstack/react-query";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { type KeyboardEvent, useEffect, useId, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../../api/client";
import type { ConnectionProfile, KubeContext } from "../../../api/types";
import { tr } from "../../../i18n/text";
import { trapTab } from "../../../lib/focusTrap";
import { ErrorBanner } from "../../common/ErrorBanner";
import { Hint } from "../../common/Hint";
import { launchLine } from "../exec";
import { folderPaths } from "./folderTree";

interface Props {
  /** Папка новых подключений по умолчанию; null — корень. */
  initialTargetId: string | null;
  onClose: () => void;
  onImported: (saved: ConnectionProfile[]) => void;
}

/**
 * Импорт контекстов kubeconfig: файл по умолчанию (`KUBECONFIG` или `~/.kube/config`) или выбранный,
 * список контекстов с причинами, почему контекст нельзя импортировать, папка и применение.
 * Строку запуска exec-плагина человек разрешает отдельной галочкой (спец. §5.2).
 */
export function KubeconfigImportDialog({ initialTargetId, onClose, onImported }: Props) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const titleId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const [path, setPath] = useState<string | null>(null);
  const [targetId, setTargetId] = useState<string | null>(initialTargetId);
  const [selected, setSelected] = useState<Set<string> | null>(null);
  const [allowed, setAllowed] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);
  const [applyError, setApplyError] = useState<unknown>(null);

  const foldersQ = useQuery({ queryKey: ["folders"], queryFn: api.listFolders });
  const previewQ = useQuery({
    queryKey: ["kubeconfig-preview", path],
    queryFn: () => api.previewKubeconfigImport(path),
    gcTime: 0,
    staleTime: 0,
  });
  const preview = previewQ.data;
  const paths = folderPaths(foldersQ.data ?? []);
  // Пока пользователь не менял выбор, отмечен текущий контекст файла (если его можно импортировать).
  const chosen = selected ?? new Set(preview?.contexts.filter((c) => c.current && !c.unsupported).map((c) => c.name) ?? []);
  // Выбранные exec-плагины, строку запуска которых ещё не разрешили.
  const unconfirmed = (preview?.contexts ?? []).filter((c) => c.exec && chosen.has(c.name) && !allowed.has(c.name));

  useEffect(() => {
    const returnTo = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    rootRef.current?.querySelector<HTMLElement>("button")?.focus();
    return () => {
      if (returnTo?.isConnected) returnTo.focus();
    };
  }, []);

  const toggle = (c: KubeContext) => {
    const next = new Set(chosen);
    if (next.has(c.name)) next.delete(c.name);
    else next.add(c.name);
    setSelected(next);
  };

  const toggleAllowed = (c: KubeContext) => {
    const next = new Set(allowed);
    if (next.has(c.name)) next.delete(c.name);
    else next.add(c.name);
    setAllowed(next);
  };

  const pickFile = async () => {
    const file = await openDialog({ multiple: false, directory: false });
    if (typeof file === "string") {
      setSelected(null);
      setAllowed(new Set());
      setPath(file);
    }
  };

  const apply = async () => {
    if (!preview) return;
    setBusy(true);
    setApplyError(null);
    try {
      const names = preview.contexts.filter((c) => chosen.has(c.name)).map((c) => c.name);
      const approveExec = names.filter((n) => allowed.has(n));
      const saved = await api.applyKubeconfigImport(preview.path, names, targetId, approveExec);
      await queryClient.invalidateQueries({ queryKey: ["connections"] });
      onImported(saved);
    } catch (err) {
      setApplyError(err);
      setBusy(false);
    }
  };

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Tab") trapTab(rootRef.current, e);
    if (e.key === "Escape" && !busy) {
      e.stopPropagation();
      onClose();
    }
  };

  return (
    <div className="modal-backdrop">
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby={titleId} ref={rootRef} onKeyDown={onKeyDown}>
        <h3 id={titleId}>{t("kubeconfig.title")}</h3>
        <div className="toolbar">
          <span className="muted">{preview ? preview.path : t("common.loading")}</span>
          <button type="button" disabled={busy} onClick={() => void pickFile()}>
            {t("kubeconfig.chooseFile")}
          </button>
        </div>
        {previewQ.error != null && <ErrorBanner error={previewQ.error} />}
        {preview && preview.contexts.length === 0 && <p className="empty">{t("kubeconfig.noContexts")}</p>}
        {preview && preview.contexts.length > 0 && (
          <ul className="steps">
            {preview.contexts.map((c) => (
              <li key={c.name}>
                <label className="inline">
                  <input type="checkbox" checked={chosen.has(c.name)} disabled={!!c.unsupported || busy} onChange={() => toggle(c)} />
                  <strong>{c.name}</strong>
                </label>{" "}
                <span className="muted">
                  {c.server} · {c.user || "—"}
                  {c.namespace ? ` · ${c.namespace}` : ""} · {t(`kubeconfig.auth.${c.auth}`)}
                </span>
                {c.current && <span className="muted"> · {t("kubeconfig.current")}</span>}
                {c.insecure && <span className="warn"> · {t("kubeconfig.insecure")}</span>}
                {c.existingId && <span className="muted"> · {t("kubeconfig.updatesExisting")}</span>}
                {c.unsupported && <p className="muted">{tr(c.unsupported)}</p>}
                {c.exec && (
                  <>
                    <pre aria-label={t("connections.form.exec.line")}>{launchLine(c.exec)}</pre>
                    <label className="inline">
                      <input
                        type="checkbox"
                        checked={allowed.has(c.name)}
                        disabled={!chosen.has(c.name) || busy}
                        onChange={() => toggleAllowed(c)}
                      />
                      {t("kubeconfig.allowExec")}
                    </label>
                  </>
                )}
              </li>
            ))}
          </ul>
        )}
        <div className="form">
          <span className="field-label">
            <label htmlFor={`${titleId}-target`}>{t("kubeconfig.target")}</label>
            <Hint k="hints.kubeconfig.target" label={t("kubeconfig.target")} />
          </span>
          <select id={`${titleId}-target`} value={targetId ?? ""} disabled={busy} onChange={(e) => setTargetId(e.target.value || null)}>
            <option value="">{t("connections.form.folderRoot")}</option>
            {paths.map((p) => (
              <option key={p.id} value={p.id}>
                {p.path.join(" / ")}
              </option>
            ))}
          </select>
        </div>
        {applyError != null && <ErrorBanner error={applyError} />}
        {unconfirmed.length > 0 && <p className="muted">{t("kubeconfig.confirmExec")}</p>}
        <div className="toolbar">
          <button
            type="button"
            className="primary"
            disabled={!preview || chosen.size === 0 || unconfirmed.length > 0 || busy}
            onClick={() => void apply()}
          >
            {t("kubeconfig.apply", { count: chosen.size })}
          </button>
          <button type="button" disabled={busy} onClick={onClose}>
            {t("common.cancel")}
          </button>
        </div>
      </div>
    </div>
  );
}
