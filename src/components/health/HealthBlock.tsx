import { useTranslation } from "react-i18next";
import { useHealth, useHealthActions } from "../../lib/useHealth";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { badgeFor, badgeTooltip, itemsFor, type Match } from "./findings";
import { FindingItem, NoteItem, SeverityMark } from "./parts";

/**
 * Значок объекта: число находок и замечаний уровня «критично» и «предупреждение»; в подсказке — их заголовки.
 * Без `match` — значок всего подключения (пункт «Замечания» в дереве).
 */
export function HealthBadge({ connectionId, match }: { connectionId: string; match?: Match }) {
  const { t } = useTranslation();
  const health = useHealth(connectionId);
  const b = badgeFor(health.data, match);
  if (!b) return null;
  return (
    <span className="health-badge" title={badgeTooltip(b, t)}>
      <SeverityMark severity={b.severity} count={b.count} noTitle />
    </span>
  );
}

/** Блок «Замечания» на экране объекта: его находки и открытые замечания LLM с действиями. */
export function HealthBlock({ connectionId, match, showTarget = false }: { connectionId: string; match: Match; showTarget?: boolean }) {
  const { t } = useTranslation();
  const health = useHealth(connectionId);
  const actions = useHealthActions(connectionId);
  const open = useWorkspace((s) => s.open);
  const { findings, notes } = itemsFor(health.data, match);
  if (findings.length === 0 && notes.length === 0) return null;
  const busy = actions.ignore.isPending || actions.setStatus.isPending || actions.remove.isPending;
  const error = actions.ignore.error ?? actions.setStatus.error ?? actions.remove.error;
  return (
    <section className="health-block" aria-label={t("health.title")}>
      <h3>
        {t("health.title")}{" "}
        <button type="button" className="link" onClick={() => open({ kind: "findings", connectionId })}>
          {t("health.allFindings")}
        </button>
      </h3>
      <ul className="finding-list">
        {findings.map((f) => (
          <FindingItem
            key={f.key}
            connectionId={connectionId}
            finding={f}
            hidden={false}
            busy={busy}
            showTarget={showTarget}
            onHide={(hide) => actions.ignore.mutate({ key: f.key, ignored: hide })}
          />
        ))}
        {notes.map((a) => (
          <NoteItem
            key={a.id}
            connectionId={connectionId}
            note={a}
            busy={busy}
            showTarget={showTarget}
            onStatus={(status) => actions.setStatus.mutate({ id: a.id, status })}
            onDelete={() => actions.remove.mutate(a.id)}
          />
        ))}
      </ul>
      {error && <ErrorBanner error={error} />}
    </section>
  );
}
