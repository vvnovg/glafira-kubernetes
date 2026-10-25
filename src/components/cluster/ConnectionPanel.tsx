import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { ApiGroups, AppError, Capabilities, ConnectionInfo, ConnectionProfile } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { LoginWait, SessionActions } from "./SessionActions";

/** Ошибки, которые лечатся входом заново или новым токеном. */
const SIGN_IN_KEYS = new Set(["errors.connections.tokenMissing", "errors.connections.loginRequired", "errors.connections.clientKeyMissing"]);

const CAPABILITIES: (keyof Capabilities)[] = ["listProjects", "listNodes", "listAllPods", "readClusterOperators", "readClusterVersion"];
/** Права, которые бывают только у OpenShift. */
const OPENSHIFT_ONLY: (keyof Capabilities)[] = ["readClusterOperators", "readClusterVersion"];

/** Группы API, найденные в кластере; названия видов не переводятся. */
const API_GROUPS: [keyof ApiGroups, string][] = [
  ["routes", "Route"],
  ["projects", "Project"],
  ["deploymentConfigs", "DeploymentConfig"],
  ["gatewayApi", "Gateway API"],
];

/** Помогает ли войти заново: истёкшая сессия, нет токена или ключа, ошибка exec-плагина. */
function needsSignIn(e: AppError, profile: ConnectionProfile): boolean {
  if (e.code === "SESSION_EXPIRED" || SIGN_IN_KEYS.has(e.key ?? "")) return true;
  return profile.auth.method === "exec" && (e.key?.startsWith("errors.exec.") || e.key === "errors.oauth.cancelled");
}

/** Вкладка подключения: подключается при открытии и показывает, кто вошёл, что за кластер и какие есть права. */
export function ConnectionPanel({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const open = useWorkspace((s) => s.open);
  const connections = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const q = useQuery({ queryKey: ["connection", connectionId], queryFn: () => api.connect(connectionId, false) });
  const profile = connections.data?.find((c) => c.id === connectionId);
  const error = q.error ? toAppError(q.error) : null;

  // Подключение заново: свежие сведения и права (после смены токена или прав в кластере).
  const reconnect = () => queryClient.fetchQuery({ queryKey: ["connection", connectionId], queryFn: () => api.connect(connectionId, true) });

  if (!profile) return <p className="empty">{connections.isPending ? t("common.loading") : t("connections.form.deleted")}</p>;

  const exec = profile.auth.method === "exec";
  const signIn = error !== null && needsSignIn(error, profile);

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{profile.name}</h2>
        <span className="muted">{profile.apiUrl}</span>
        <button type="button" disabled={q.isFetching} onClick={() => void reconnect().catch(() => undefined)}>
          {t("cluster.connection.reconnect")}
        </button>
        <button type="button" onClick={() => open({ kind: "connection-form", connectionId })}>
          {t("cluster.connection.edit")}
        </button>
      </div>
      {/* Exec-плагин может ждать входа в браузере до 5 минут, и при первом подключении, и при повторе. Рядом с ошибкой входа то же ожидание показывает SessionActions. */}
      {exec && q.isFetching && !signIn && (
        <div className="toolbar">
          <LoginWait connectionId={connectionId} label={t("cluster.session.waitingExec")} />
        </div>
      )}
      {!exec && q.isPending && <p className="muted">{t("cluster.connection.connecting")}</p>}
      {error && <ConnectionError error={error} signIn={signIn} onRetry={() => void q.refetch()} profile={profile} />}
      {q.data && <ConnectionSummary info={q.data} />}
    </div>
  );
}

function ConnectionError({ error, signIn, onRetry, profile }: { error: AppError; signIn: boolean; onRetry: () => void; profile: ConnectionProfile }) {
  const { t } = useTranslation();
  return (
    <>
      <ErrorBanner error={error} onRetry={signIn ? undefined : onRetry} />
      {signIn && (
        <>
          <h3>{t("cluster.session.title")}</h3>
          <SessionActions profile={profile} />
        </>
      )}
    </>
  );
}

function ConnectionSummary({ info }: { info: ConnectionInfo }) {
  const { t } = useTranslation();
  const openShift = info.platform.kind === "openshift";
  const found = API_GROUPS.filter(([k]) => info.apiGroups[k]).map(([, name]) => name);
  return (
    <>
      <dl className="stats">
        <div>
          <dt>{t("cluster.connection.user")}</dt>
          <dd>{info.user}</dd>
        </div>
        <div>
          <dt>
            <span className="field-label">
              {t("cluster.connection.platform")}
              <Hint k="hints.cluster.platform" label={t("cluster.connection.platform")} />
            </span>
          </dt>
          <dd>{info.platform.label}</dd>
        </div>
        <div>
          <dt>{t("cluster.connection.version")}</dt>
          <dd>{info.serverVersion}</dd>
        </div>
        <div>
          <dt>
            <span className="field-label">
              {t("cluster.connection.apiGroups")}
              <Hint k="hints.cluster.apiGroups" label={t("cluster.connection.apiGroups")} />
            </span>
          </dt>
          <dd>{found.length > 0 ? found.join(", ") : "—"}</dd>
        </div>
      </dl>
      <h3>{t("cluster.connection.capabilities")}</h3>
      <ul className="steps">
        {CAPABILITIES.filter((c) => openShift || !OPENSHIFT_ONLY.includes(c)).map((c) => (
          <li key={c}>
            {info.capabilities[c] ? "✅" : "—"} {t(`cluster.capability.${c}`)}
          </li>
        ))}
      </ul>
      {info.groups.length > 0 && (
        <details>
          <summary>{t("cluster.connection.groups", { count: info.groups.length })}</summary>
          <ul>
            {info.groups.map((g) => (
              <li key={g}>
                <code>{g}</code>
              </li>
            ))}
          </ul>
        </details>
      )}
    </>
  );
}
