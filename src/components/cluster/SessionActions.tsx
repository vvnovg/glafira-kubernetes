import { useIsFetching, useMutation, useQueryClient } from "@tanstack/react-query";
import { openUrl } from "@tauri-apps/plugin-opener";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { ConnectionInfo, ConnectionProfile } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";

/** Ожидание входа в браузере или exec-плагина с кнопкой «Отменить»; кладётся в строку `.toolbar`. */
export function LoginWait({ connectionId, label }: { connectionId: string; label: string }) {
  const { t } = useTranslation();
  return (
    <>
      <span className="muted">{label}</span>
      <button
        type="button"
        onClick={() => {
          // Сбой отмены не показываем: ожидание закончится само (входом, ошибкой или по таймауту), а кнопка остаётся для повторной отмены.
          api.cancelOauthLogin(connectionId).catch(() => undefined);
        }}
      >
        {t("cluster.session.cancelLogin")}
      </button>
    </>
  );
}

/**
 * Что сделать, если сессия истекла или токена нет: войти через браузер, вставить токен
 * (в том числе со страницы токена кластера), запустить exec-плагин снова или изменить
 * подключение (сертификат, строка запуска exec-плагина).
 */
export function SessionActions({ profile }: { profile: ConnectionProfile }) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const open = useWorkspace((s) => s.open);
  const [token, setToken] = useState("");
  const id = profile.id;
  const done = (info: ConnectionInfo) => queryClient.setQueryData(["connection", id], info);
  // Подключение этой вкладки (в том числе «Подключиться заново» на панели) тоже может запускать exec-плагин.
  const connecting = useIsFetching({ queryKey: ["connection", id] }) > 0;

  const login = useMutation({ mutationFn: () => api.startOauthLogin(id), onSuccess: done });
  // Exec-плагин: подключение заново запускает его снова.
  const rerun = useMutation({ mutationFn: () => api.connect(id, true), onSuccess: done });
  const paste = useMutation({
    mutationFn: async () => {
      await api.setConnectionToken(id, token);
      return api.connect(id, true);
    },
    onSuccess: (info) => {
      setToken("");
      done(info);
    },
  });
  const tokenPage = useMutation({
    mutationFn: async () => {
      await openUrl(await api.oauthTokenPage(id));
    },
  });

  if (profile.auth.method === "clientCert")
    return (
      <div className="toolbar">
        <button type="button" onClick={() => open({ kind: "connection-form", connectionId: id })}>
          {t("cluster.session.editConnection")}
        </button>
      </div>
    );

  if (profile.auth.method === "exec")
    return (
      <div className="panel">
        <div className="toolbar">
          {rerun.isPending || connecting ? (
            <LoginWait connectionId={id} label={t("cluster.session.waitingExec")} />
          ) : (
            <button type="button" className="primary" onClick={() => rerun.mutate()}>
              {t("cluster.session.loginExec")}
            </button>
          )}
          <button type="button" onClick={() => open({ kind: "connection-form", connectionId: id })}>
            {t("cluster.session.editConnection")}
          </button>
        </div>
        {rerun.error && <ErrorBanner error={rerun.error} />}
      </div>
    );

  return (
    <div className="panel">
      {profile.auth.method === "browser" && (
        <div className="toolbar">
          {login.isPending ? (
            <LoginWait connectionId={id} label={t("cluster.session.waitingBrowser")} />
          ) : (
            <button type="button" className="primary" onClick={() => login.mutate()}>
              {t("cluster.session.loginBrowser")}
            </button>
          )}
        </div>
      )}
      {/* Отмена входа самим пользователем — не сбой. */}
      {login.error && toAppError(login.error).key !== "errors.oauth.cancelled" && <ErrorBanner error={login.error} />}
      <div className="toolbar">
        <label htmlFor={`token-${id}`}>{t("cluster.session.pasteToken")}</label>
        <input
          id={`token-${id}`}
          type="password"
          autoComplete="off"
          placeholder="sha256~…"
          value={token}
          onChange={(e) => setToken(e.target.value)}
        />
        <button type="button" disabled={!token.trim() || paste.isPending} onClick={() => paste.mutate()}>
          {t("cluster.session.saveToken")}
        </button>
        <button type="button" className="link" disabled={tokenPage.isPending} onClick={() => tokenPage.mutate()}>
          {t("cluster.session.tokenPage")}
        </button>
      </div>
      {paste.error && <ErrorBanner error={paste.error} />}
      {tokenPage.error && <ErrorBanner error={tokenPage.error} />}
    </div>
  );
}
