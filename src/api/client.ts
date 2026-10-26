import { Channel, invoke } from "@tauri-apps/api/core";
import type {
  AppError,
  AppInfo,
  ClusterOverview,
  ConfigValue,
  ConfigView,
  ConnectionFolder,
  ConnectionInfo,
  ConnectionProfile,
  EventObject,
  EventRow,
  ExportSummary,
  ImportPreview,
  KubeconfigPreview,
  LanguageSettings,
  Listed,
  LogEvent,
  LogParams,
  McpAuditEntry,
  McpClientConfigs,
  McpStatus,
  NamespaceAccess,
  NetworkView,
  NodeDetails,
  NodeRow,
  OperatorRow,
  PodDetails,
  PodRow,
  PolicyStatus,
  ProjectList,
  PvcRow,
  QuotasView,
  SecretsInput,
  TestReport,
  TreeItemRef,
  WatchEvent,
  WatchParams,
  WorkloadDetails,
  WorkloadKind,
  WorkloadList,
  WorkloadRef,
  YamlKind,
} from "./types";

export function isAppError(e: unknown): e is AppError {
  return typeof e === "object" && e !== null && "code" in e && "message" in e;
}

export function toAppError(e: unknown): AppError {
  // Ошибка из Rust приходит целиком: key/params/details сохраняются как есть.
  if (isAppError(e)) return e;
  if (e instanceof Error) return { code: "INTERNAL", message: e.message, retryable: false };
  return { code: "INTERNAL", message: String(e), retryable: false };
}

async function call<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  try {
    return await invoke<T>(cmd, args);
  } catch (e) {
    throw toAppError(e);
  }
}

export const api = {
  getAppInfo: () => call<AppInfo>("get_app_info"),
  getLanguageSettings: () => call<LanguageSettings>("get_language_settings"),
  setLanguage: (lang: string | null) => call<LanguageSettings>("set_language", { lang }),
  getPolicyStatus: () => call<PolicyStatus>("get_policy_status"),

  listConnections: () => call<ConnectionProfile[]>("list_connections"),
  /** `confirmExec` — человек подтвердил строку запуска exec-плагина в форме. */
  saveConnection: (profile: ConnectionProfile, secrets: SecretsInput = {}, confirmExec = false) =>
    call<ConnectionProfile>("save_connection", { input: { profile, secrets, confirmExec } }),
  deleteConnection: (id: string) => call<void>("delete_connection", { id }),
  testConnection: (profile: ConnectionProfile, secrets: SecretsInput = {}, confirmExec = false) =>
    call<TestReport>("test_connection", { input: { profile, secrets, confirmExec } }),
  connect: (connectionId: string, fresh = false) => call<ConnectionInfo>("connect", { connectionId, fresh }),
  disconnect: (connectionId: string) => call<void>("disconnect", { connectionId }),
  setConnectionToken: (connectionId: string, token: string) =>
    call<void>("set_connection_token", { connectionId, token }),
  startOauthLogin: (connectionId: string) => call<ConnectionInfo>("start_oauth_login", { connectionId }),
  /** Отменяет вход через браузер или ожидание exec-плагина при подключении. */
  cancelOauthLogin: (connectionId: string) => call<boolean>("cancel_oauth_login", { connectionId }),
  /** Отменяет ожидание exec-плагина при проверке подключения в форме; подключение вкладки не трогает. */
  cancelConnectionTest: (connectionId: string) => call<boolean>("cancel_connection_test", { connectionId }),
  oauthTokenPage: (connectionId: string) => call<string>("oauth_token_page", { connectionId }),
  resolveExecCommand: (command: string) => call<string>("resolve_exec_command", { command }),
  previewKubeconfigImport: (path: string | null) => call<KubeconfigPreview>("preview_kubeconfig_import", { path }),
  /**
   * `approveExec` — отпечатки строк запуска exec-плагина, которые подтвердил человек
   * (`launchFingerprint` контекстов из предпросмотра). Имя контекста ничего не подтверждает.
   */
  applyKubeconfigImport: (path: string, contexts: string[], folderId: string | null, approveExec: string[] = []) =>
    call<ConnectionProfile[]>("apply_kubeconfig_import", { path, contexts, folderId, approveExec }),

  listFolders: () => call<ConnectionFolder[]>("list_folders"),
  createFolder: (name: string, parentId: string | null) =>
    call<ConnectionFolder>("create_folder", { name, parentId }),
  renameFolder: (id: string, name: string) => call<ConnectionFolder>("rename_folder", { id, name }),
  deleteFolder: (id: string) => call<void>("delete_folder", { id }),
  moveItems: (items: TreeItemRef[], parentId: string | null, before: TreeItemRef | null) =>
    call<void>("move_items", { items, parentId, before }),
  exportConnections: (path: string, folderId: string | null) =>
    call<ExportSummary>("export_connections", { path, folderId }),
  previewImport: (path: string, targetFolderId: string | null, updateExisting: boolean) =>
    call<ImportPreview>("preview_import", { path, targetFolderId, updateExisting }),
  applyImport: (path: string, targetFolderId: string | null, updateExisting: boolean) =>
    call<ImportPreview>("apply_import", { path, targetFolderId, updateExisting }),

  getClusterOverview: (connectionId: string) => call<ClusterOverview>("get_cluster_overview", { connectionId }),
  listNodes: (connectionId: string) => call<Listed<NodeRow>>("list_nodes", { connectionId }),
  getNode: (connectionId: string, name: string) => call<NodeDetails>("get_node", { connectionId, name }),
  listClusterOperators: (connectionId: string) => call<OperatorRow[]>("list_cluster_operators", { connectionId }),
  listProjects: (connectionId: string) => call<ProjectList>("list_projects", { connectionId }),
  /** `force` — ручное обновление: Rust забывает кэш прав проекта (он живёт 5 минут) и читает заново. */
  getNamespaceAccess: (connectionId: string, namespace: string, force = false) =>
    call<NamespaceAccess>("get_namespace_access", { connectionId, namespace, force }),
  listWorkloads: (connectionId: string, namespace: string) => call<WorkloadList>("list_workloads", { connectionId, namespace }),
  getWorkload: (connectionId: string, namespace: string, kind: WorkloadKind, name: string) =>
    call<WorkloadDetails>("get_workload", { connectionId, namespace, kind, name }),
  /** Поды проекта или, если задан `workload`, его поды. */
  listPods: (connectionId: string, namespace: string, workload: WorkloadRef | null = null) =>
    call<Listed<PodRow>>("list_pods", { connectionId, namespace, workload }),
  getPod: (connectionId: string, namespace: string, name: string) => call<PodDetails>("get_pod", { connectionId, namespace, name }),
  /** События проекта или объекта; у workload'а — вместе с его подами. */
  listEvents: (connectionId: string, namespace: string, object: EventObject | null = null) =>
    call<Listed<EventRow>>("list_events", { connectionId, namespace, object }),
  getNetwork: (connectionId: string, namespace: string) => call<NetworkView>("get_network", { connectionId, namespace }),
  getConfig: (connectionId: string, namespace: string) => call<ConfigView>("get_config", { connectionId, namespace }),
  getConfigMapValues: (connectionId: string, namespace: string, name: string) =>
    call<Record<string, ConfigValue>>("get_config_map_values", { connectionId, namespace, name }),
  listPvcs: (connectionId: string, namespace: string) => call<Listed<PvcRow>>("list_pvcs", { connectionId, namespace }),
  getQuotas: (connectionId: string, namespace: string) => call<QuotasView>("get_quotas", { connectionId, namespace }),
  getYaml: (connectionId: string, namespace: string, kind: YamlKind, name: string) =>
    call<string>("get_yaml", { connectionId, namespace, kind, name }),

  /** Watch списка проекта; события приходят в `onEvent`. Возвращает id сессии для `stopSession`. */
  startWatch: (params: WatchParams, onEvent: (e: WatchEvent) => void) => {
    const channel = new Channel<WatchEvent>();
    channel.onmessage = onEvent;
    return call<string>("start_watch", { params, onEvent: channel });
  },
  /** Логи контейнера; события приходят в `onEvent`. Возвращает id сессии для `stopSession`. */
  startLogs: (params: LogParams, onEvent: (e: LogEvent) => void) => {
    const channel = new Channel<LogEvent>();
    channel.onmessage = onEvent;
    return call<string>("start_logs", { params, onEvent: channel });
  },
  /** Сохраняет лог (без follow) в файл; возвращает число строк. */
  saveLogs: (params: LogParams, path: string, timestamps: boolean) => call<number>("save_logs", { params, path, timestamps }),
  stopSession: (sessionId: string) => call<boolean>("stop_session", { sessionId }),

  getMcpStatus: () => call<McpStatus>("get_mcp_status"),
  setMcpEnabled: (enabled: boolean) => call<McpStatus>("set_mcp_enabled", { enabled }),
  answerMcpApproval: (sessionId: string, approve: boolean) =>
    call<McpStatus>("answer_mcp_approval", { sessionId, approve }),
  revokeMcpClient: (name: string) => call<McpStatus>("revoke_mcp_client", { name }),
  disconnectMcpSessions: (sessionId: string | null) => call<McpStatus>("disconnect_mcp_sessions", { sessionId }),
  getMcpAudit: (limit: number) => call<McpAuditEntry[]>("get_mcp_audit", { limit }),
  getMcpClientConfigs: () => call<McpClientConfigs>("get_mcp_client_configs"),
  revealMcpAudit: () => call<void>("reveal_mcp_audit"),
};
