import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { Proposal } from "../../api/types";
import { currentLocale } from "../../i18n";
import { useConnection } from "../../lib/useConnection";
import { useWorkspace } from "../../state/workspace";
import { SeverityMark, TargetLink } from "./parts";
import { changeSummary, proposalStatusText, proposalTab } from "./proposals";

/** Длина причины отклонения — как `MAX_REJECT_REASON_CHARS` в Rust. */
const REJECT_REASON_MAX = 300;

interface Props {
  connectionId: string;
  proposal: Proposal;
  busy?: boolean;
  showTarget?: boolean;
  /** `true` — статус сменился; при ошибке (её показывает панель) поле причины остаётся с текстом. */
  onStatus: (status: "rejected" | "open", reason?: string) => Promise<boolean>;
  onDelete: () => void;
}

/**
 * Предложение LLM (спец. §10.5): заголовок, обоснование как есть, описание изменения (у применённого
 * с правками — и фактически применённого), подпись клиента и статус. «Открыть в форме» открывает
 * вкладку объекта с заполненной формой; для read-only подключения кнопка неактивна. «Отклонить»
 * спрашивает необязательную причину.
 */
export function ProposalItem({ connectionId, proposal: p, busy, showTarget = true, onStatus, onDelete }: Props) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const connection = useConnection(connectionId);
  const [rejecting, setRejecting] = useState(false);
  const [reason, setReason] = useState("");
  const lang = currentLocale();
  const client = p.client ?? t("mcp.unknownClient");
  const readOnly = connection?.readOnly === true;
  const status = proposalStatusText(p, t, Date.now(), lang);
  const cancelReject = () => {
    setRejecting(false);
    setReason("");
  };
  return (
    <li className={p.status === "open" ? "finding note proposal" : "finding note proposal is-closed"}>
      <div className="finding-head">
        <SeverityMark severity={p.severity} />
        <strong>{p.title}</strong>
        {showTarget && <TargetLink connectionId={connectionId} target={p.target} />}
      </div>
      <p className="note-body">{p.rationale}</p>
      <p className="proposal-change">{changeSummary(p.change, t)}</p>
      {p.appliedChange && (
        <p className="proposal-change">
          {t("health.proposals.appliedAs", { change: changeSummary(p.appliedChange, t) })}
        </p>
      )}
      <p className="muted">
        {t("health.proposals.by", { client })}
        {p.relatedRuleId && <> · {t(`health.ruleNames.${p.relatedRuleId}`)}</>}
        {status && <> · {status}</>}
      </p>
      <div className="finding-actions">
        {p.status === "open" && (
          <>
            <button
              type="button"
              className="link"
              disabled={busy || readOnly}
              title={readOnly ? t("health.proposals.readOnly") : undefined}
              onClick={() => open(proposalTab(connectionId, p))}
            >
              {t("health.proposals.openForm")}
            </button>
            {rejecting ? (
              <span className="proposal-reject">
                <input
                  aria-label={t("health.proposals.reasonPlaceholder")}
                  placeholder={t("health.proposals.reasonPlaceholder")}
                  value={reason}
                  maxLength={REJECT_REASON_MAX}
                  autoFocus
                  onChange={(e) => setReason(e.target.value)}
                />
                <button
                  type="button"
                  className="link danger"
                  disabled={busy}
                  onClick={async () => {
                    if (await onStatus("rejected", reason.trim() || undefined)) cancelReject();
                  }}
                >
                  {t("health.proposals.rejectConfirm")}
                </button>
                <button type="button" className="link" onClick={cancelReject}>
                  {t("common.cancel")}
                </button>
              </span>
            ) : (
              <button type="button" className="link" disabled={busy} onClick={() => setRejecting(true)}>
                {t("health.proposals.reject")}
              </button>
            )}
          </>
        )}
        {p.status === "rejected" && (
          <button type="button" className="link" disabled={busy} onClick={() => void onStatus("open")}>
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
