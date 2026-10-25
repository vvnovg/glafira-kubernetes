import { invoke } from "@tauri-apps/api/core";
import type {
  AppError,
  AppInfo,
  ConnectionFolder,
  ConnectionInfo,
  ConnectionProfile,
  ExportSummary,
  ImportPreview,
  KubeconfigPreview,
  LanguageSettings,
  PolicyStatus,
  SecretsInput,
  TestReport,
  TreeItemRef,
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
};
