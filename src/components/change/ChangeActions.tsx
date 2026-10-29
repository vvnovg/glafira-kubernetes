import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { Change, NamespaceAccess, WorkloadDetails } from "../../api/types";
import { useConnection } from "../../lib/useConnection";
import { useNamespaceAccess } from "../project/useNamespaceAccess";
import { allowed, canRestart, canScale, canSetResources, changeRight } from "./changes";
import { ChangeDialog, type ResourceHints } from "./ChangeDialog";

/** Текущие requests и limits контейнеров шаблона — подсказки формы ресурсов. */
export function hintsOf(d: WorkloadDetails): ResourceHints {
  return Object.fromEntries(d.containers.map((c) => [c.name, { requests: c.requests, limits: c.limits }]));
}

/** Пустая форма ресурсов: все контейнеры шаблона, ни одно поле не меняется. */
export function emptyResources(namespace: string, d: WorkloadDetails): Change {
  return {
    kind: "setResources",
    namespace,
    workload: { kind: d.workload.kind, name: d.workload.name },
    containers: d.containers.map((c) => ({ name: c.name, requests: {}, limits: {} })),
  };
}

/** Кнопка изменения: без прав или на подключении только для чтения — неактивна с подсказкой (спец. §5.4). */
export function ChangeButton({ change, access, readOnly, label, onOpen }: { change: Change; access?: NamespaceAccess; readOnly: boolean; label: string; onOpen: (c: Change) => void }) {
  const { t } = useTranslation();
  const right = changeRight(change);
  const can = allowed(access, change);
  const title = readOnly ? t("change.readOnly") : can ? undefined : t("change.noRights", right);
  return (
    <button type="button" disabled={readOnly || !can} title={title} onClick={() => onOpen(change)}>
      {label}
    </button>
  );
}

/** Изменения на обзоре workload'а (спец. §9): ресурсы контейнеров, реплики, перезапуск, границы HPA. */
export function WorkloadActions({ connectionId, namespace, d }: { connectionId: string; namespace: string; d: WorkloadDetails }) {
  const { t } = useTranslation();
  const access = useNamespaceAccess(connectionId, namespace).data;
  const readOnly = useConnection(connectionId)?.readOnly === true;
  const [open, setOpen] = useState<Change | null>(null);
  const w = { kind: d.workload.kind, name: d.workload.name };
  const hpa = d.hpa.data;
  const buttons: { change: Change; label: string }[] = [];
  if (canSetResources(w.kind)) buttons.push({ change: emptyResources(namespace, d), label: t("change.action.setResources") });
  if (canScale(w.kind)) buttons.push({ change: { kind: "scale", namespace, workload: w, replicas: d.workload.desired ?? 1 }, label: t("change.action.scale") });
  if (canRestart(w.kind)) buttons.push({ change: { kind: "rolloutRestart", namespace, workload: w }, label: t("change.action.rolloutRestart") });
  if (hpa) {
    buttons.push({
      change: { kind: "setHpaBounds", namespace, hpa: hpa.name, minReplicas: hpa.minReplicas ?? undefined, maxReplicas: hpa.maxReplicas },
      label: t("change.action.setHpaBounds"),
    });
  }
  if (buttons.length === 0) return null;
  return (
    <div className="change-actions">
      {buttons.map((b) => (
        <ChangeButton key={b.change.kind} change={b.change} access={access} readOnly={readOnly} label={b.label} onOpen={setOpen} />
      ))}
      {open && <ChangeDialog connectionId={connectionId} initial={open} hints={hintsOf(d)} onClose={() => setOpen(null)} />}
    </div>
  );
}

/** «Удалить под» на обзоре пода. */
export function PodActions({ connectionId, namespace, pod }: { connectionId: string; namespace: string; pod: string }) {
  const { t } = useTranslation();
  const access = useNamespaceAccess(connectionId, namespace).data;
  const readOnly = useConnection(connectionId)?.readOnly === true;
  const [open, setOpen] = useState<Change | null>(null);
  return (
    <div className="change-actions">
      <ChangeButton change={{ kind: "deletePod", namespace, pod }} access={access} readOnly={readOnly} label={t("change.action.deletePod")} onOpen={setOpen} />
      {open && <ChangeDialog connectionId={connectionId} initial={open} onClose={() => setOpen(null)} />}
    </div>
  );
}
