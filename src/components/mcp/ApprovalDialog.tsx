import { useMutation, useQueryClient } from "@tanstack/react-query";
import { type KeyboardEvent, useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { trapTab } from "../../lib/focusTrap";
import { MCP_STATUS_KEY, useMcpStatus } from "../../lib/useMcpStatus";
import { ErrorBanner } from "../common/ErrorBanner";

/**
 * Подтверждение нового MCP-клиента (спец. §10.1) поверх любого экрана; несколько ожидающих —
 * по очереди. Системный window.confirm в Tauri 2 не показывается, поэтому — диалог в странице.
 *
 * Диалог появляется асинхронно и без действий пользователя, поэтому сам забирает фокус на контейнер
 * (не на «Разрешить»: случайный Enter не должен выдавать доступ), держит Tab внутри, по Escape
 * отклоняет запрос и после ответа возвращает фокус туда, где он был.
 */
export function ApprovalDialog() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const { data: s } = useMcpStatus();
  const answer = useMutation({
    mutationFn: ({ id, approve }: { id: string; approve: boolean }) => api.answerMcpApproval(id, approve),
    onSuccess: (st) => qc.setQueryData(MCP_STATUS_KEY, st),
  });
  const rootRef = useRef<HTMLDivElement>(null);
  const req = s?.pending[0];
  const sessionId = req?.sessionId;
  const open = sessionId !== undefined;
  const { reset } = answer;

  // Объявлен раньше эффекта фокуса: запоминает элемент в фокусе до того, как диалог его заберёт.
  useEffect(() => {
    if (!open) return;
    const returnTo = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    return () => {
      if (returnTo?.isConnected) returnTo.focus();
    };
  }, [open]);

  // Следующий запрос в очереди — новый диалог для пользователя: фокус на него, ошибка прошлого ответа не нужна.
  useEffect(() => {
    reset();
    if (sessionId !== undefined) rootRef.current?.focus();
  }, [sessionId, reset]);

  if (!s || !req) return null;
  const client = req.clientName ? `${req.clientName} ${req.clientVersion ?? ""}`.trim() : t("mcp.unknownClient");
  const decide = (approve: boolean) => answer.mutate({ id: req.sessionId, approve });
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Tab") trapTab(rootRef.current, e);
    if (e.key === "Escape") {
      e.stopPropagation();
      if (!answer.isPending) decide(false);
    }
  };

  return (
    <div className="modal-backdrop">
      <div
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="mcp-approval-title"
        tabIndex={-1}
        ref={rootRef}
        onKeyDown={onKeyDown}
      >
        <h3 id="mcp-approval-title">{t("mcp.approval.title", { client })}</h3>
        <p>{t("mcp.approval.body", { n: s.allowedConnections })}</p>
        <p className="hint">{t("mcp.approval.readOnly")}</p>
        {!req.clientName && <p className="hint">{t("mcp.approval.unnamed")}</p>}
        {s.pending.length > 1 && <p className="muted">{t("mcp.approval.more", { n: s.pending.length - 1 })}</p>}
        {answer.error && <ErrorBanner error={answer.error} />}
        <div className="toolbar">
          <button type="button" className="primary" disabled={answer.isPending} onClick={() => decide(true)}>
            {t("mcp.approval.allow")}
          </button>
          <button type="button" disabled={answer.isPending} onClick={() => decide(false)}>
            {t("mcp.approval.reject")}
          </button>
        </div>
      </div>
    </div>
  );
}
