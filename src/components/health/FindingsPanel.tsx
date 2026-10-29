import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { Finding, HealthTarget, Severity, TargetKind } from "../../api/types";
import { useHealth, useHealthActions } from "../../lib/useHealth";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { CheckBar } from "./CheckBar";
import { findingsOf, SEVERITIES, newestClosedFirst } from "./findings";
import { FindingItem, NoteItem, SeverityMark } from "./parts";
import { ProposalItem } from "./ProposalItem";

const KINDS: TargetKind[] = [
  "cluster",
  "node",
  "project",
  "workload",
  "pod",
  "service",
  "route",
  "ingress",
  "httproute",
  "grpcroute",
  "gateway",
  "pvc",
  "hpa",
  "quota",
];

type Source = "all" | "rules" | "llm";

/** Вкладка «Замечания»: находки последней проверки по правилам, замечания и предложения LLM (спец. §8.1, §10.5). */
export function FindingsPanel({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const health = useHealth(connectionId);
  const actions = useHealthActions(connectionId);
  const [severity, setSeverity] = useState<Severity | "all">("all");
  const [source, setSource] = useState<Source>("all");
  const [kind, setKind] = useState<TargetKind | "all">("all");
  const [query, setQuery] = useState("");
  const [showHidden, setShowHidden] = useState(false);
  const [confirmAll, setConfirmAll] = useState(false);
  const [confirmAllProposals, setConfirmAllProposals] = useState(false);

  const view = health.data;
  const hidden = new Set(view?.ignored ?? []);
  const hiddenCount = (view?.lastCheck?.findings ?? []).filter((f) => hidden.has(f.key)).length;
  const q = query.trim().toLowerCase();
  // Поиск — по имени объекта и по проекту.
  const matches = (x: { severity: Severity; target: HealthTarget }) =>
    (severity === "all" || x.severity === severity) &&
    (kind === "all" || x.target.kind === kind) &&
    (q === "" || [x.target.name, x.target.namespace].some((s) => (s ?? "").toLowerCase().includes(q)));
  const findings = source === "llm" ? [] : findingsOf(view, showHidden).filter(matches);
  const groups = new Map<string, Finding[]>();
  for (const f of findings) groups.set(f.ruleId, [...(groups.get(f.ruleId) ?? []), f]);
  const notes = source === "rules" ? [] : (view?.annotations ?? []).filter(matches);
  const open = notes.filter((a) => a.status === "open");
  const closed = newestClosedFirst(notes.filter((a) => a.status !== "open"));
  const proposals = source === "rules" ? [] : (view?.proposals ?? []).filter(matches);
  const openProposals = proposals.filter((p) => p.status === "open");
  const closedProposals = newestClosedFirst(proposals.filter((p) => p.status !== "open"));
  const busy =
    actions.ignore.isPending ||
    actions.setStatus.isPending ||
    actions.remove.isPending ||
    actions.removeAll.isPending ||
    actions.setProposalStatus.isPending ||
    actions.removeProposal.isPending ||
    actions.removeAllProposals.isPending;
  const error =
    actions.ignore.error ??
    actions.setStatus.error ??
    actions.remove.error ??
    actions.removeAll.error ??
    actions.setProposalStatus.error ??
    actions.removeProposal.error ??
    actions.removeAllProposals.error;
  const proposalStatus = (id: string) => (status: "rejected" | "open", reason?: string) =>
    actions.setProposalStatus.mutateAsync({ id, status, reason }).then(
      () => true,
      () => false,
    );
  const filtered = severity !== "all" || kind !== "all" || q !== "";

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{t("health.title")}</h2>
      </div>
      <CheckBar connectionId={connectionId} />
      <div className="toolbar">
        <select aria-label={t("health.filterSeverity")} value={severity} onChange={(e) => setSeverity(e.target.value as Severity | "all")}>
          <option value="all">{t("health.allSeverities")}</option>
          {SEVERITIES.map((s) => (
            <option key={s} value={s}>
              {t(`health.severity.${s}`)}
            </option>
          ))}
        </select>
        <select aria-label={t("health.filterSource")} value={source} onChange={(e) => setSource(e.target.value as Source)}>
          <option value="all">{t("health.allSources")}</option>
          <option value="rules">{t("health.sourceRules")}</option>
          <option value="llm">{t("health.sourceLlm")}</option>
        </select>
        <select aria-label={t("health.filterKind")} value={kind} onChange={(e) => setKind(e.target.value as TargetKind | "all")}>
          <option value="all">{t("health.allKinds")}</option>
          {KINDS.map((k) => (
            <option key={k} value={k}>
              {t(`health.kind.${k}`)}
            </option>
          ))}
        </select>
        <input aria-label={t("health.search")} placeholder={t("health.search")} value={query} onChange={(e) => setQuery(e.target.value)} />
        <span className="field-label">
          <label>
            <input type="checkbox" checked={showHidden} onChange={(e) => setShowHidden(e.target.checked)} />
            {t("health.showHidden", { count: hiddenCount })}
          </label>
          <Hint k="hints.health.hidden" label={t("health.showHidden", { count: hiddenCount })} />
        </span>
      </div>
      {error && <ErrorBanner error={error} />}

      {source !== "llm" && view?.lastCheck && (
        <section aria-label={t("health.sourceRules")}>
          {groups.size === 0 && (
            <p className="empty">
              {filtered ? t("health.nothingMatches") : hiddenCount > 0 ? t("health.allHidden") : t("health.noFindings")}
            </p>
          )}
          {[...groups.entries()].map(([ruleId, items]) => (
            <details key={ruleId} className="finding-group" open={items.length <= 5}>
              <summary>
                <SeverityMark severity={items[0].severity} /> {t(`health.ruleNames.${ruleId}`)}{" "}
                <span className="muted">{t("health.groupSize", { count: items.length })}</span>
              </summary>
              <ul className="finding-list">
                {items.map((f) => (
                  <FindingItem
                    key={f.key}
                    connectionId={connectionId}
                    finding={f}
                    hidden={hidden.has(f.key)}
                    busy={busy}
                    onHide={(hide) => actions.ignore.mutate({ key: f.key, ignored: hide })}
                  />
                ))}
              </ul>
            </details>
          ))}
        </section>
      )}

      {source !== "rules" && (
        <section aria-labelledby="health-proposals">
          <h3 id="health-proposals">
            {t("health.proposals.title")} <Hint k="hints.health.proposals" label={t("health.proposals.title")} />
          </h3>
          <p className="hint">{t("health.proposals.intro")}</p>
          {openProposals.length === 0 && <p className="empty">{filtered ? t("health.nothingMatches") : t("health.proposals.none")}</p>}
          <ul className="finding-list">
            {openProposals.map((p) => (
              <ProposalItem key={p.id} connectionId={connectionId} proposal={p} busy={busy} onStatus={proposalStatus(p.id)} onDelete={() => actions.removeProposal.mutate(p.id)} />
            ))}
          </ul>
          {closedProposals.length > 0 && (
            <details className="finding-group">
              <summary>{t("health.closedNotes", { count: closedProposals.length })}</summary>
              <ul className="finding-list">
                {closedProposals.map((p) => (
                  <ProposalItem key={p.id} connectionId={connectionId} proposal={p} busy={busy} onStatus={proposalStatus(p.id)} onDelete={() => actions.removeProposal.mutate(p.id)} />
                ))}
              </ul>
            </details>
          )}
          {(view?.proposals.length ?? 0) > 0 &&
            (confirmAllProposals ? (
              <p role="alert">
                {t("health.proposals.deleteAllConfirm")}{" "}
                <button
                  type="button"
                  className="danger"
                  disabled={busy}
                  onClick={() => actions.removeAllProposals.mutate(undefined, { onSettled: () => setConfirmAllProposals(false) })}
                >
                  {t("health.proposals.deleteAll")}
                </button>{" "}
                <button type="button" onClick={() => setConfirmAllProposals(false)}>
                  {t("common.cancel")}
                </button>
              </p>
            ) : (
              <button type="button" className="link danger" disabled={busy} onClick={() => setConfirmAllProposals(true)}>
                {t("health.proposals.deleteAll")}
              </button>
            ))}
        </section>
      )}

      {source !== "rules" && (
        <section aria-labelledby="health-notes">
          <h3 id="health-notes">{t("health.notes")}</h3>
          <p className="hint">{t("health.llmNotes")}</p>
          {open.length === 0 && <p className="empty">{filtered ? t("health.nothingMatches") : t("health.noNotes")}</p>}
          <ul className="finding-list">
            {open.map((a) => (
              <NoteItem
                key={a.id}
                connectionId={connectionId}
                note={a}
                busy={busy}
                onStatus={(status) => actions.setStatus.mutate({ id: a.id, status })}
                onDelete={() => actions.remove.mutate(a.id)}
              />
            ))}
          </ul>
          {closed.length > 0 && (
            <details className="finding-group">
              <summary>{t("health.closedNotes", { count: closed.length })}</summary>
              <ul className="finding-list">
                {closed.map((a) => (
                  <NoteItem
                    key={a.id}
                    connectionId={connectionId}
                    note={a}
                    busy={busy}
                    onStatus={(status) => actions.setStatus.mutate({ id: a.id, status })}
                    onDelete={() => actions.remove.mutate(a.id)}
                  />
                ))}
              </ul>
            </details>
          )}
          {(view?.annotations.length ?? 0) > 0 &&
            (confirmAll ? (
              <p role="alert">
                {t("health.deleteAllConfirm")}{" "}
                <button
                  type="button"
                  className="danger"
                  disabled={busy}
                  onClick={() => actions.removeAll.mutate(undefined, { onSettled: () => setConfirmAll(false) })}
                >
                  {t("health.deleteAll")}
                </button>{" "}
                <button type="button" onClick={() => setConfirmAll(false)}>
                  {t("common.cancel")}
                </button>
              </p>
            ) : (
              <button type="button" className="link danger" disabled={busy} onClick={() => setConfirmAll(true)}>
                {t("health.deleteAll")}
              </button>
            ))}
        </section>
      )}
    </div>
  );
}
