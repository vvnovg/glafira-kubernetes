import { type QueryClient, useIsFetching, useMutation, useQueryClient } from "@tanstack/react-query";
import { openUrl } from "@tauri-apps/plugin-opener";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { ConnectionInfo, ConnectionProfile } from "../../api/types";
import { afterSignIn, restartSessions } from "../../lib/afterSignIn";
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

/** Ключ мутаций входа: по нему панель блокирует «Подключиться заново», пока идёт вход. */
export const sessionMutationKey = (connectionId: string) => ["session", connectionId];

/**
 * Подключение заново (свежие сведения и права). Идёт через запрос вкладки: тот помечается как загружаемый,
 * и панель не запускает рядом второй exec-плагин (бэкенд ставит подключения одного id в очередь).
 * После него открытые вкладки подключения перечитываются (`afterSignIn`).
 */
export async function reconnect(queryClient: QueryClient, connectionId: string): Promise<ConnectionInfo> {
  const info = await queryClient.fetchQuery({ queryKey: ["connection", connectionId], queryFn: () => api.connect(connectionId, true) });
  afterSignIn(queryClient, connectionId);
  return info;
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
  const done = (info: ConnectionInfo) => {
    queryClient.setQueryData(["connection", id], info);
    afterSignIn(queryClient, id);
  };
  // Подключение этой вкладки (в том числе «Подключиться заново» на панели) тоже может запускать exec-плагин.
  const connecting = useIsFetching({ queryKey: ["connection", id], exact: true }) > 0;
  const mutationKey = sessionMutationKey(id);

  const login = useMutation({ mutationKey, mutationFn: () => api.startOauthLogin(id), onSuccess: done });
  // Exec-плагин: подключение заново запускает его снова; сведения запрос сохраняет сам.
  const rerun = useMutation({ mutationKey, mutationFn: () => reconnect(queryClient, id) });
  const paste = useMutation({
    mutationKey,
    mutationFn: async () => {
      await api.setConnectionToken(id, token);
      try {
        return await reconnect(queryClient, id);
      } catch (e) {
        // Токен сохранён, и Rust остановил сессии подключения (`stop_for`). После удачного входа их перезапустит
        // `afterSignIn`, а при сбое входа открытые списки остались бы «живыми» без сессии.
        restartSessions([id]);
        throw e;
      }
    },
    onSuccess: () => setToken(""),
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
            <button type="button" className="primary" disabled={connecting || paste.isPending} onClick={() => login.mutate()}>
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
        <button type="button" disabled={!token.trim() || paste.isPending || login.isPending || connecting} onClick={() => paste.mutate()}>
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
