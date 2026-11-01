import { useIsMutating, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { ApiGroups, AppError, Capabilities, ConnectionInfo, ConnectionProfile } from "../../api/types";
import { formatDateTime } from "../../lib/age";
import { needsSignIn } from "../../lib/signIn";
import { useNow } from "../../lib/useNow";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { ClusterOverviewView } from "./ClusterOverview";
import { LoginWait, reconnect, SessionActions, sessionMutationKey } from "./SessionActions";

/** За сколько до истечения токена показывается предупреждение (спец. §5.2). */
const TOKEN_WARNING_SEC = 3600;
/** Как часто сверяется срок токена: предупреждение появляется не позже чем через минуту после порога. */
const TOKEN_CHECK_MS = 60_000;

const CAPABILITIES: (keyof Capabilities)[] = ["listProjects", "listNodes", "listAllPods", "readClusterOperators", "readClusterVersion"];
/** Права, которые бывают только у OpenShift. */
const OPENSHIFT_ONLY: (keyof Capabilities)[] = ["readClusterOperators", "readClusterVersion"];

/** Группы API, найденные в кластере; названия видов не переводятся. Project у OpenShift и DropApp — один вид. */
const API_GROUPS: [keyof ApiGroups, string][] = [
  ["routes", "Route"],
  ["projects", "Project"],
  ["dropappProjects", "Project"],
  ["deploymentConfigs", "DeploymentConfig"],
  ["gatewayApi", "Gateway API"],
];

/** Имя вида `system:serviceaccount:team-a:glafira-dev` переносится после двоеточий, а не посреди слова. */
function breakAfterColons(name: string) {
  return name.split(":").flatMap((part, i) => (i === 0 ? [part] : [":", <wbr key={i} />, part]));
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
  // Идёт вход, запуск exec-плагина или сохранение токена: второе подключение рядом не запускаем.
  const signingIn = useIsMutating({ mutationKey: sessionMutationKey(connectionId) }) > 0;
  const now = useNow(TOKEN_CHECK_MS);

  if (!profile) return <p className="empty">{connections.isPending ? t("common.loading") : t("connections.form.deleted")}</p>;

  const exec = profile.auth.method === "exec";
  const signIn = error !== null && needsSignIn(error, profile);
  // Подключение заново: свежие сведения и права (после смены токена или прав в кластере).
  const reconnectNow = () => void reconnect(queryClient, connectionId).catch(() => undefined);

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{profile.name}</h2>
        <span className="muted">{profile.apiUrl}</span>
        <button type="button" disabled={q.isFetching || signingIn} onClick={reconnectNow}>
          {t("cluster.connection.reconnect")}
        </button>
        <button type="button" onClick={() => open({ kind: "connection-form", connectionId })}>
          {t("cluster.connection.edit")}
        </button>
        {q.data?.capabilities.listNodes && (
          <button type="button" onClick={() => open({ kind: "nodes", connectionId })}>
            {t("cluster.nodes.open")}
          </button>
        )}
      </div>
      {/* Exec-плагин может ждать входа в браузере до 5 минут, и при первом подключении, и при повторе. Рядом с ошибкой входа то же ожидание показывает SessionActions. */}
      {exec && q.isFetching && !signIn && (
        <div className="toolbar">
          <LoginWait connectionId={connectionId} label={t("cluster.session.waitingExec")} />
        </div>
      )}
      {!exec && q.isPending && <p className="muted">{t("cluster.connection.connecting")}</p>}
      {error && <ConnectionError error={error} signIn={signIn} onRetry={() => void q.refetch()} profile={profile} />}
      {q.data && <ConnectionSummary info={q.data} now={now} busy={q.isFetching || signingIn} onReconnect={reconnectNow} />}
      {q.data && <ClusterOverviewView connectionId={connectionId} />}
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

function ConnectionSummary({ info, now, busy, onReconnect }: { info: ConnectionInfo; now: number; busy: boolean; onReconnect: () => void }) {
  const { t } = useTranslation();
  const openShift = info.platform.kind === "openshift";
  const found = [...new Set(API_GROUPS.filter(([k]) => info.apiGroups[k]).map(([, name]) => name))];
  const expiresIn = info.tokenExpiresAt === null ? null : info.tokenExpiresAt - now / 1000;
  return (
    <>
      {/* Область объявления есть, пока у токена есть срок: предупреждение появляется в ней со временем. */}
      {expiresIn !== null && (
        <div role="status">
          {expiresIn < TOKEN_WARNING_SEC && (
            <p className="warn">
              {expiresIn > 0 ? (
                t("cluster.connection.tokenExpiresSoon")
              ) : (
                <>
                  {t("cluster.connection.tokenExpired")}{" "}
                  <button type="button" disabled={busy} onClick={onReconnect}>
                    {t("cluster.connection.reconnect")}
                  </button>
                </>
              )}
            </p>
          )}
        </div>
      )}
      <dl className="stats">
        <div>
          <dt>{t("cluster.connection.user")}</dt>
          <dd>{breakAfterColons(info.user)}</dd>
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
        {info.tokenExpiresAt !== null && (
          <div>
            <dt>{t("cluster.connection.tokenExpires")}</dt>
            <dd>{formatDateTime(info.tokenExpiresAt)}</dd>
          </div>
        )}
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
