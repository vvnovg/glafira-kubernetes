import { useTranslation } from "react-i18next";
import type { Annotation, AnnotationStatus, Finding, HealthTarget, Severity } from "../../api/types";
import { type TabSpec, useWorkspace } from "../../state/workspace";
import { findingTexts } from "./findings";

/**
 * Точка цвета строгости; число — у значков. `noTitle` — без своей подсказки: у значка она
 * общая (заголовки находок) и не должна перекрываться словом «Критично».
 */
export function SeverityMark({ severity, count, noTitle }: { severity: Severity; count?: number; noTitle?: boolean }) {
  const { t } = useTranslation();
  const label = t(`health.severity.${severity}`);
  return (
    <span
      className={`sev sev-${severity}`}
      role="img"
      title={noTitle ? undefined : label}
      aria-label={count === undefined ? label : `${label}: ${count}`}
    >
      ●{count !== undefined && <span className="sev-count">{count}</span>}
    </span>
  );
}

/** Вкладка объекта: у workload'а и пода — своя, у ноды — список нод, у остальных объектов проекта — проект. */
export function targetTab(connectionId: string, target: HealthTarget): TabSpec {
  const ns = target.namespace;
  if (target.kind === "node") return { kind: "nodes", connectionId };
  if (!ns) return { kind: "cluster", connectionId };
  if (target.kind === "workload" && target.workloadKind && target.name) {
    return { kind: "workload", connectionId, namespace: ns, workloadKind: target.workloadKind, name: target.name };
  }
  if (target.kind === "pod" && target.name) return { kind: "pod", connectionId, namespace: ns, name: target.name };
  return { kind: "project", connectionId, namespace: ns };
}

/** Объект находки: вид и ссылка — «Deployment team-a/web», «Нода w1», «Кластер». */
export function TargetLink({ connectionId, target }: { connectionId: string; target: HealthTarget }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const go = () => open(targetTab(connectionId, target));
  const kind = target.kind === "workload" && target.workloadKind ? target.workloadKind : t(`health.kind.${target.kind}`);
  const name = target.kind === "project" ? target.namespace : target.namespace && target.name ? `${target.namespace}/${target.name}` : target.name;
  return (
    <span className="health-target">
      {name === undefined ? (
        <button type="button" className="link" onClick={go}>
          {kind}
        </button>
      ) : (
        <>
          <span className="muted">{kind}</span>{" "}
          <button type="button" className="link" onClick={go}>
            {name}
          </button>
        </>
      )}
    </span>
  );
}

interface FindingItemProps {
  connectionId: string;
  finding: Finding;
  hidden: boolean;
  busy?: boolean;
  showTarget?: boolean;
  onHide: (hide: boolean) => void;
}

export function FindingItem({ connectionId, finding, hidden, busy, showTarget = true, onHide }: FindingItemProps) {
  const { t } = useTranslation();
  const x = findingTexts(finding, t);
  return (
    <li className={hidden ? "finding is-hidden" : "finding"}>
      <div className="finding-head">
        <SeverityMark severity={finding.severity} />
        <strong>{x.title}</strong>
        {showTarget && <TargetLink connectionId={connectionId} target={finding.target} />}
      </div>
      <p>{x.details}</p>
      <p className="muted">
        {t("health.fix")} {x.fix}
      </p>
      <div className="finding-actions">
        <button type="button" className="link" disabled={busy} onClick={() => onHide(!hidden)}>
          {hidden ? t("health.restore") : t("health.hide")}
        </button>
      </div>
    </li>
  );
}

interface NoteItemProps {
  connectionId: string;
  note: Annotation;
  busy?: boolean;
  showTarget?: boolean;
  onStatus: (status: AnnotationStatus) => void;
  onDelete: () => void;
}

/** Замечание LLM: текст как есть (без разметки и ссылок), подпись — клиент. */
export function NoteItem({ connectionId, note, busy, showTarget = true, onStatus, onDelete }: NoteItemProps) {
  const { t } = useTranslation();
  const client = note.client ?? t("mcp.unknownClient");
  return (
    <li className={note.status === "open" ? "finding note" : "finding note is-closed"}>
      <div className="finding-head">
        <SeverityMark severity={note.severity} />
        <strong>{note.title}</strong>
        {showTarget && <TargetLink connectionId={connectionId} target={note.target} />}
      </div>
      <p className="note-body">{note.body}</p>
      <p className="muted">
        {t("health.noteBy", { client })}
        {note.relatedRuleId && <> · {t(`health.ruleNames.${note.relatedRuleId}`)}</>}
        {note.status !== "open" && <> · {t(`health.status.${note.status}`)}</>}
      </p>
      <div className="finding-actions">
        {note.status === "open" ? (
          <>
            <button type="button" className="link" disabled={busy} onClick={() => onStatus("resolved")}>
              {t("health.resolve")}
            </button>
            <button type="button" className="link" disabled={busy} onClick={() => onStatus("dismissed")}>
              {t("health.dismiss")}
            </button>
          </>
        ) : (
          <button type="button" className="link" disabled={busy} onClick={() => onStatus("open")}>
            {t("health.reopen")}
          </button>
        )}
        <button type="button" className="link danger" disabled={busy} onClick={onDelete}>
          {t("common.delete")}
        </button>
      </div>
    </li>
  );
}
