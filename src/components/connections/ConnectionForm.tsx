import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { AppError, AuthConfig, AuthMethod, CaMode, ConnectionProfile, Environment, ExecAuth, ProxyMode, SecretsInput, TestReport } from "../../api/types";
import { tr } from "../../i18n/text";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { formatArgs, formatEnv, hasSecretValues, launchKey, launchLine, maskArgs, maskEnv, needsExecConfirmation, newExec, parseArgs, parseEnv } from "./exec";
import { formatNamespaces, parseNamespaces, withAuthMethod, withCaMode, withProxyMode } from "./profile";
import { folderPaths } from "./tree/folderTree";

const ENVIRONMENTS: Environment[] = ["local", "dev", "test", "stage", "prod"];
const AUTH_METHODS: AuthMethod[] = ["browser", "token", "clientCert", "exec"];
const CA_MODES: CaMode[] = ["system", "pem", "insecure"];
const PROXY_MODES: ProxyMode[] = ["system", "none", "url"];

export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile; tabId: string }) {
  const { t } = useTranslation();
  const [profile, setProfile] = useState(initial);
  // Секреты из формы: пустое поле — оставить сохранённый (у нового подключения его нет).
  const [token, setToken] = useState("");
  const [clientKey, setClientKey] = useState("");
  const [favorites, setFavorites] = useState(formatNamespaces(initial.favoriteNamespaces));
  const [manual, setManual] = useState(formatNamespaces(initial.manualNamespaces));
  // Последние введённые значения, чтобы переключение режима туда и обратно их не теряло.
  const [lastCert, setLastCert] = useState(initial.auth.method === "clientCert" ? initial.auth.certificatePem : "");
  const [lastCa, setLastCa] = useState(initial.ca.mode === "pem" ? initial.ca.pem : "");
  const [lastProxy, setLastProxy] = useState(initial.proxy.mode === "url" ? initial.proxy.url : "");
  // Exec-плагин: аргументы и переменные редактируются текстом, по строке на значение.
  const initialExec: ExecAuth = initial.auth.method === "exec" ? initial.auth : newExec();
  const [lastExec, setLastExec] = useState(initialExec);
  const [execArgs, setExecArgs] = useState(formatArgs(initialExec.args));
  const [execEnv, setExecEnv] = useState(formatEnv(initialExec.env));
  // Значения секретных аргументов и переменных скрыты (поля только для чтения), пока не нажата «Показать».
  const [secretsShown, setSecretsShown] = useState(() => !hasSecretValues(initialExec));
  // Подтверждение привязано к строке: хранится ключ подтверждённой строки. Правка строки не обнуляет ключ, а делает производный флаг ложным (ключ не совпадает); обнуляет его только смена способа входа.
  const [confirmedExec, setConfirmedExec] = useState<string | null>(null);
  const [report, setReport] = useState<TestReport | null>(null);
  const [error, setError] = useState<AppError | null>(null);
  const [busy, setBusy] = useState(false);
  // Подтверждение удаления внутри формы: нативный window.confirm в Tauri 2 не показывается и возвращает false.
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const queryClient = useQueryClient();
  const foldersQ = useQuery({ queryKey: ["folders"], queryFn: api.listFolders });
  const policyQ = useQuery({ queryKey: ["policy"], queryFn: api.getPolicyStatus });
  const insecureForbidden = policyQ.data?.allowInsecureTls === false;
  const execForbidden = policyQ.data?.allowExecAuth === false;
  const paths = useMemo(() => folderPaths(foldersQ.data ?? []), [foldersQ.data]);
  // Ссылка на несуществующую папку означает корень (так её показывает и дерево).
  const knownFolder = (id: string | null | undefined): string | null =>
    !id || (foldersQ.isSuccess && !paths.some((p) => p.id === id)) ? null : id;
  // Папка, с которой форма открылась: перенос — только если пользователь сменил её в списке.
  const [loadedFolderId] = useState(initial.folderId);
  const [folderTouched, setFolderTouched] = useState(false);
  const folderId = knownFolder(folderTouched ? profile.folderId : initial.folderId);
  const open = useWorkspace((s) => s.open);
  const close = useWorkspace((s) => s.close);
  const closeForConnection = useWorkspace((s) => s.closeForConnection);
  const isNew = initial.id === "";
  const keepPlaceholder = isNew ? "" : t("connections.form.secretKeep");

  const update = (patch: Partial<ConnectionProfile>) => setProfile((p) => ({ ...p, ...patch }));
  const currentAuth = (): AuthConfig =>
    profile.auth.method === "exec" ? { ...profile.auth, args: parseArgs(execArgs), env: parseEnv(execEnv) } : profile.auth;
  const current = (): ConnectionProfile => ({
    ...profile,
    auth: currentAuth(),
    folderId,
    favoriteNamespaces: parseNamespaces(favorites),
    manualNamespaces: parseNamespaces(manual),
  });
  const secrets = (): SecretsInput => ({
    token: profile.auth.method === "token" && token.trim() ? token : null,
    clientKey: profile.auth.method === "clientCert" && clientKey.trim() ? clientKey : null,
  });
  // Строку запуска подтверждает человек: новая, изменённая или неподтверждённая требует галочки.
  const execKey = profile.auth.method === "exec" ? launchKey(currentAuth() as ExecAuth) : null;
  const confirmExec = execKey !== null && execKey === confirmedExec;
  const execNeedsConfirm = needsExecConfirmation(initial.auth, currentAuth());
  const execBlocked = profile.auth.method === "exec" && (execForbidden || (execNeedsConfirm && !confirmExec));

  async function run(fn: () => Promise<void>) {
    setBusy(true);
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError(toAppError(e));
    } finally {
      setBusy(false);
    }
  }

  const onTest = () =>
    run(async () => {
      setReport(null);
      setReport(await api.testConnection(current(), secrets(), confirmExec));
    });

  const onSave = () =>
    run(async () => {
      const saved = await api.saveConnection(current(), secrets(), confirmExec);
      // save_connection не меняет место существующего подключения в дереве — перенос отдельной командой.
      try {
        if (!isNew && folderTouched && folderId !== knownFolder(loadedFolderId)) {
          await api.moveItems([{ kind: "connection", id: saved.id }], folderId, null);
        }
      } finally {
        // Настройки уже сохранены, даже если перенос не удался: список нужно обновить в любом случае.
        await queryClient.invalidateQueries({ queryKey: ["connections"] });
      }
      queryClient.removeQueries({ predicate: (q) => q.queryKey[1] === saved.id });
      close(tabId);
      open({ kind: "cluster", connectionId: saved.id });
    });

  const onDelete = () =>
    run(async () => {
      setConfirmingDelete(false);
      await api.deleteConnection(profile.id);
      await queryClient.invalidateQueries({ queryKey: ["connections"] });
      queryClient.removeQueries({ predicate: (q) => q.queryKey[1] === profile.id });
      closeForConnection(profile.id);
      close(tabId);
    });

  // Кнопка «Найти»: абсолютный путь к команде по PATH оболочки входа и стандартным каталогам.
  const onFind = () =>
    run(async () => {
      if (profile.auth.method !== "exec") return;
      const command = await api.resolveExecCommand(profile.auth.command);
      update({ auth: { ...profile.auth, command } });
    });

  const auth = profile.auth;
  const ca = profile.ca;
  const proxy = profile.proxy;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{isNew ? t("connections.form.titleNew") : t("connections.form.titleEdit", { name: initial.name })}</h2>
      </div>
      <div className="form">
        <label htmlFor="cf-name">{t("connections.form.name")}</label>
        <input id="cf-name" value={profile.name} onChange={(e) => update({ name: e.target.value })} />

        <label htmlFor="cf-folder">{t("connections.form.folder")}</label>
        <select
          id="cf-folder"
          value={folderId ?? ""}
          onChange={(e) => {
            setFolderTouched(true);
            update({ folderId: e.target.value || null });
          }}
        >
          <option value="">{t("connections.form.folderRoot")}</option>
          {paths.map((p) => (
            <option key={p.id} value={p.id}>
              {p.path.join(" / ")}
            </option>
          ))}
        </select>

        <span className="field-label">
          <label htmlFor="cf-env">{t("connections.form.environment")}</label>
          <Hint k="hints.connections.environment" label={t("connections.form.environment")} />
        </span>
        <select
          id="cf-env"
          value={profile.environment}
          onChange={(e) => {
            const environment = e.target.value as Environment;
            // Новое prod-подключение по умолчанию только для чтения; снять флажок можно сразу.
            update({ environment, readOnly: environment === "prod" && isNew ? true : profile.readOnly });
          }}
        >
          {ENVIRONMENTS.map((env) => (
            <option key={env} value={env}>
              {env}
            </option>
          ))}
        </select>

        <span className="field-label">
          <label htmlFor="cf-ro">{t("common.readOnly")}</label>
          <Hint k="hints.connections.readOnly" label={t("common.readOnly")} />
        </span>
        <span>
          <input id="cf-ro" type="checkbox" checked={profile.readOnly} onChange={(e) => update({ readOnly: e.target.checked })} />
        </span>

        <label htmlFor="cf-color">{t("connections.form.color")}</label>
        <input id="cf-color" type="color" value={profile.color} onChange={(e) => update({ color: e.target.value })} />

        <span className="field-label">
          <label htmlFor="cf-api">{t("connections.form.apiUrl")}</label>
          <Hint k="hints.connections.apiUrl" label={t("connections.form.apiUrl")} />
        </span>
        <input id="cf-api" placeholder="https://api.cluster.example.com:6443" value={profile.apiUrl} onChange={(e) => update({ apiUrl: e.target.value })} />

        <span className="field-label">
          <label htmlFor="cf-auth">{t("connections.form.authMethod")}</label>
          <Hint k="hints.connections.authMethod" label={t("connections.form.authMethod")} />
        </span>
        <select
          id="cf-auth"
          value={auth.method}
          onChange={(e) => {
            if (auth.method === "clientCert") setLastCert(auth.certificatePem);
            if (auth.method === "exec") setLastExec(currentAuth() as ExecAuth);
            setConfirmedExec(null);
            update({ auth: withAuthMethod(auth, e.target.value as AuthMethod, lastCert, lastExec) });
          }}
        >
          {AUTH_METHODS.map((m) => (
            <option key={m} value={m} disabled={m === "exec" && execForbidden && auth.method !== "exec"}>
              {t(`connections.form.auth.${m}`)}
            </option>
          ))}
        </select>

        {auth.method === "browser" && <p className="muted full">{t("connections.form.browserNote")}</p>}

        {auth.method === "token" && (
          <>
            <span className="field-label">
              <label htmlFor="cf-token">{t("connections.form.token")}</label>
              <Hint k="hints.connections.token" label={t("connections.form.token")} />
            </span>
            <input id="cf-token" type="password" autoComplete="off" placeholder={keepPlaceholder} value={token} onChange={(e) => setToken(e.target.value)} />
          </>
        )}

        {auth.method === "clientCert" && (
          <>
            <span className="field-label">
              <label htmlFor="cf-cert">{t("connections.form.certificate")}</label>
              <Hint k="hints.connections.clientCert" label={t("connections.form.certificate")} />
            </span>
            <textarea
              id="cf-cert"
              rows={4}
              placeholder="-----BEGIN CERTIFICATE-----"
              value={auth.certificatePem}
              onChange={(e) => update({ auth: { method: "clientCert", certificatePem: e.target.value } })}
            />
            <label htmlFor="cf-key">{t("connections.form.clientKey")}</label>
            <textarea id="cf-key" rows={4} placeholder={keepPlaceholder || "-----BEGIN PRIVATE KEY-----"} value={clientKey} onChange={(e) => setClientKey(e.target.value)} />
          </>
        )}

        {auth.method === "exec" && (
          <>
            <span className="field-label">
              <label htmlFor="cf-exec-cmd">{t("connections.form.exec.command")}</label>
              <Hint k="hints.connections.exec" label={t("connections.form.exec.command")} />
            </span>
            <span className="toolbar">
              <input
                id="cf-exec-cmd"
                placeholder="/opt/homebrew/bin/kubelogin"
                value={auth.command}
                onChange={(e) => update({ auth: { ...auth, command: e.target.value } })}
              />
              <button type="button" disabled={busy || !auth.command.trim()} onClick={onFind}>
                {t("connections.form.exec.find")}
              </button>
            </span>
            <label htmlFor="cf-exec-args">{t("connections.form.exec.args")}</label>
            <textarea
              id="cf-exec-args"
              rows={3}
              placeholder="get-token"
              readOnly={!secretsShown}
              value={secretsShown ? execArgs : formatArgs(maskArgs(parseArgs(execArgs)))}
              onChange={(e) => setExecArgs(e.target.value)}
            />
            <label htmlFor="cf-exec-env">{t("connections.form.exec.env")}</label>
            <textarea
              id="cf-exec-env"
              rows={2}
              placeholder="AWS_PROFILE=dev"
              readOnly={!secretsShown}
              value={secretsShown ? execEnv : formatEnv(maskEnv(parseEnv(execEnv)))}
              onChange={(e) => setExecEnv(e.target.value)}
            />
            {!secretsShown && (
              <p className="muted full">
                {t("connections.form.exec.secretsHidden")}{" "}
                <button type="button" className="link" onClick={() => setSecretsShown(true)}>
                  {t("connections.form.exec.showSecrets")}
                </button>
              </p>
            )}
            <span className="field-label">
              <span>{t("connections.form.exec.line")}</span>
              <Hint k="hints.connections.execLine" label={t("connections.form.exec.line")} />
            </span>
            <pre aria-label={t("connections.form.exec.line")}>{launchLine(currentAuth() as ExecAuth)}</pre>
            {execNeedsConfirm ? (
              <label className="inline full">
                <input type="checkbox" checked={confirmExec} disabled={execForbidden} onChange={(e) => setConfirmedExec(e.target.checked ? execKey : null)} />
                {t("connections.form.exec.confirm")}
              </label>
            ) : (
              <p className="muted full">{t("connections.form.exec.approved")}</p>
            )}
            {execForbidden && (
              <p className="warn full" role="alert">
                {t("connections.form.exec.forbidden")}
              </p>
            )}
          </>
        )}

        <span className="field-label">
          <label htmlFor="cf-ca">{t("connections.form.ca")}</label>
          <Hint k="hints.connections.ca" label={t("connections.form.ca")} />
        </span>
        <select
          id="cf-ca"
          value={ca.mode}
          onChange={(e) => {
            if (ca.mode === "pem") setLastCa(ca.pem);
            update({ ca: withCaMode(ca, e.target.value as CaMode, lastCa) });
          }}
        >
          {CA_MODES.map((m) => (
            <option key={m} value={m} disabled={m === "insecure" && insecureForbidden && ca.mode !== "insecure"}>
              {t(`connections.form.caMode.${m}`)}
            </option>
          ))}
        </select>
        {ca.mode === "pem" && (
          <textarea
            className="full"
            aria-label={t("connections.form.caPem")}
            rows={4}
            placeholder="-----BEGIN CERTIFICATE-----"
            value={ca.pem}
            onChange={(e) => update({ ca: { mode: "pem", pem: e.target.value } })}
          />
        )}
        {ca.mode === "insecure" && (
          <p className="warn full" role="alert">
            {insecureForbidden ? t("connections.form.insecureForbidden") : t("connections.form.insecureWarning")}
          </p>
        )}

        <span className="field-label">
          <label htmlFor="cf-sni">{t("connections.form.tlsServerName")}</label>
          <Hint k="hints.connections.tlsServerName" label={t("connections.form.tlsServerName")} />
        </span>
        <input id="cf-sni" value={profile.tlsServerName ?? ""} onChange={(e) => update({ tlsServerName: e.target.value || null })} />

        <span className="field-label">
          <label htmlFor="cf-proxy">{t("connections.form.proxy")}</label>
          <Hint k="hints.connections.proxy" label={t("connections.form.proxy")} />
        </span>
        <select
          id="cf-proxy"
          value={proxy.mode}
          onChange={(e) => {
            if (proxy.mode === "url") setLastProxy(proxy.url);
            update({ proxy: withProxyMode(proxy, e.target.value as ProxyMode, lastProxy) });
          }}
        >
          {PROXY_MODES.map((m) => (
            <option key={m} value={m}>
              {t(`connections.form.proxyMode.${m}`)}
            </option>
          ))}
        </select>
        {proxy.mode === "url" && (
          <input
            className="full"
            aria-label={t("connections.form.proxyUrl")}
            placeholder="http://proxy.example.com:3128"
            value={proxy.url}
            onChange={(e) => update({ proxy: { mode: "url", url: e.target.value } })}
          />
        )}

        <span className="field-label">
          <label htmlFor="cf-fav">{t("connections.form.favorites")}</label>
          <Hint k="hints.connections.favorites" label={t("connections.form.favorites")} />
        </span>
        <input id="cf-fav" placeholder="team-a, team-b" value={favorites} onChange={(e) => setFavorites(e.target.value)} />

        <span className="field-label">
          <label htmlFor="cf-manual">{t("connections.form.manualNamespaces")}</label>
          <Hint k="hints.connections.manualNamespaces" label={t("connections.form.manualNamespaces")} />
        </span>
        <input id="cf-manual" value={manual} onChange={(e) => setManual(e.target.value)} />

        <label htmlFor="cf-timeout">{t("connections.form.requestTimeout")}</label>
        <input
          id="cf-timeout"
          type="number"
          min={1000}
          step={1000}
          value={profile.requestTimeoutMs}
          onChange={(e) => update({ requestTimeoutMs: Number(e.target.value) })}
        />
        <label htmlFor="cf-connect">{t("connections.form.connectTimeout")}</label>
        <input
          id="cf-connect"
          type="number"
          min={1000}
          step={1000}
          value={profile.connectTimeoutMs}
          onChange={(e) => update({ connectTimeoutMs: Number(e.target.value) })}
        />
      </div>

      <div className="toolbar">
        <button type="button" disabled={busy || execBlocked} onClick={onTest}>
          {t("connections.form.test")}
        </button>
        <Hint k="hints.connections.test" label={t("connections.form.test")} />
        {busy && auth.method === "exec" && (
          // Плагин может ждать входа в браузере до 5 минут.
          <button
            type="button"
            onClick={() => {
              // Сбой отмены не показываем: ожидание закончится само, а кнопка остаётся для повторной отмены.
              api.cancelConnectionTest(profile.id).catch(() => undefined);
            }}
          >
            {t("common.cancel")}
          </button>
        )}
        <button type="button" className="primary" disabled={busy} onClick={onSave}>
          {t("common.save")}
        </button>
        {!isNew &&
          (confirmingDelete ? (
            <span role="group" aria-label={t("connections.form.deleteGroup")}>
              <strong>{t("connections.form.deleteConfirm", { name: profile.name })}</strong>{" "}
              <button type="button" className="danger" disabled={busy} onClick={onDelete}>
                {t("connections.form.deleteYes")}
              </button>{" "}
              <button type="button" disabled={busy} onClick={() => setConfirmingDelete(false)}>
                {t("common.cancel")}
              </button>
            </span>
          ) : (
            <button type="button" className="danger" disabled={busy} onClick={() => setConfirmingDelete(true)}>
              {t("common.delete")}
            </button>
          ))}
        {busy && <span className="muted">{t("connections.form.busy")}</span>}
      </div>

      {error && <ErrorBanner error={error} />}

      {report && (
        <ul className="steps">
          {report.steps.map((s) => (
            <li key={s.name}>
              {s.ok ? "✅" : "❌"} <strong>{t(`connections.form.step.${s.name}`)}:</strong> <span>{tr(s.message)}</span>
              {s.error?.details && <pre className="muted">{s.error.details}</pre>}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
