// Типы IPC-контракта с Rust (спец. §4.2). Поля — camelCase, как сериализует serde.

export type ErrorCode =
  | "CONNECTION_FAILED"
  | "TLS_ERROR"
  | "AUTH_FAILED"
  | "SESSION_EXPIRED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "CONFLICT"
  | "TIMEOUT"
  | "INVALID_INPUT"
  | "READ_ONLY"
  | "CONFIRMATION_REQUIRED"
  | "INTERNAL";

/** Текст из Rust: ключ словаря, параметры и английский текст (запасной вариант). */
export interface LocalizedText {
  key: string;
  params: Record<string, string>;
  text: string;
}

export interface AppError {
  code: ErrorCode;
  /** Ключ словаря; у технического текста без перевода — `errors.generic` с `params.message`. */
  key?: string;
  params?: Record<string, string>;
  /** Английский текст (запасной вариант перевода). */
  message: string;
  details?: string;
  retryable: boolean;
}

export type Environment = "local" | "dev" | "test" | "stage" | "prod";

export interface ExecEnv {
  name: string;
  value: string;
}

/** Exec-плагин (спец. §5.2): абсолютный путь к команде, аргументы, переменные окружения. */
export interface ExecAuth {
  command: string;
  args: string[];
  env: ExecEnv[];
  apiVersion: string;
  provideClusterInfo: boolean;
  /** Отпечаток подтверждённой строки запуска; ставит только Rust по согласию в форме или диалоге. */
  approved?: string | null;
}

export type AuthConfig =
  | { method: "token" }
  | { method: "browser" }
  | { method: "clientCert"; certificatePem: string }
  | ({ method: "exec" } & ExecAuth);
export type AuthMethod = AuthConfig["method"];

export type CaConfig = { mode: "system" } | { mode: "pem"; pem: string } | { mode: "insecure" };
export type CaMode = CaConfig["mode"];

export type ProxyConfig = { mode: "system" } | { mode: "none" } | { mode: "url"; url: string };
export type ProxyMode = ProxyConfig["mode"];

export interface ConnectionProfile {
  id: string;
  name: string;
  color: string;
  environment: Environment;
  readOnly: boolean;
  apiUrl: string;
  auth: AuthConfig;
  ca: CaConfig;
  tlsServerName?: string | null;
  proxy: ProxyConfig;
  favoriteNamespaces: string[];
  manualNamespaces: string[];
  requestTimeoutMs: number;
  connectTimeoutMs: number;
  /** Папка в дереве; null — корень. Меняется только командой перемещения. */
  folderId?: string | null;
  order?: number;
}

/** Секреты из формы: нет поля или null — оставить сохранённый, "" — удалить, иное — сохранить. */
export interface SecretsInput {
  token?: string | null;
  clientKey?: string | null;
}

export interface ConnectionFolder {
  id: string;
  name: string;
  parentId: string | null;
  order: number;
}

/** Итог экспорта подключений: сколько записано в файл. */
export interface ExportSummary {
  connections: number;
  folders: number;
}

/** Элемент предпросмотра импорта; `path` — имена папок от корня до места элемента. */
export interface PreviewItem {
  kind: "folder" | "connection";
  id: string;
  name: string;
  path: string[];
}

/** Подключение файла, которое не будет импортировано. */
export interface SkippedImport {
  name: string;
  reason: LocalizedText;
}

/** Предпросмотр (и итог) импорта подключений. */
export interface ImportPreview {
  added: PreviewItem[];
  updated: PreviewItem[];
  unchanged: PreviewItem[];
  skipped: SkippedImport[];
  warnings: LocalizedText[];
  /** Сколько новых подключений ждут токен или ключ. */
  needsSecrets: number;
}

/** Ссылка на элемент дерева подключений для перемещения. */
export interface TreeItemRef {
  kind: "folder" | "connection";
  id: string;
}

export type TestStepName = "config" | "tcp" | "exec" | "tls" | "auth" | "discovery" | "projects";

export interface TestStep {
  name: TestStepName;
  ok: boolean;
  message: LocalizedText;
  error?: AppError;
}

export interface TestReport {
  ok: boolean;
  steps: TestStep[];
}

export interface Capabilities {
  listProjects: boolean;
  listNodes: boolean;
  listAllPods: boolean;
  readClusterOperators: boolean;
  readClusterVersion: boolean;
}

export type PlatformKind = "openshift" | "kubernetes";
export type Distribution = "eks" | "gke" | "aks" | "rancher" | "deckhouse" | "k3s" | "rke2";

/** Платформа кластера (спец. §4.3); `label` — «OpenShift 4.18», «Kubernetes 1.31 (EKS)». */
export interface PlatformInfo {
  kind: PlatformKind;
  distribution: Distribution | null;
  version: string | null;
  label: string;
}

/** Группы API, от которых зависит, какие объекты показываются. */
export interface ApiGroups {
  routes: boolean;
  projects: boolean;
  deploymentConfigs: boolean;
  gatewayApi: boolean;
}

export interface ConnectionInfo {
  user: string;
  groups: string[];
  serverVersion: string;
  platform: PlatformInfo;
  apiGroups: ApiGroups;
  capabilities: Capabilities;
}

export type ImportAuth = "token" | "clientCert" | "browser" | "exec";

export interface KubeContext {
  name: string;
  cluster: string;
  server: string;
  user: string;
  namespace: string | null;
  auth: ImportAuth;
  insecure: boolean;
  current: boolean;
  /**
   * Строка запуска exec-плагина (путь к команде найден) — её подтверждает человек.
   * В предпросмотре значения секретных переменных окружения и секретных аргументов заменены
   * на `***`, поэтому объект нельзя сохранять как профиль.
   */
  exec: ExecAuth | null;
  /**
   * Отпечаток показанной строки запуска, посчитанный по настоящим значениям (до замены секретов
   * на `***`). Его же отправляют в `applyKubeconfigImport` как подтверждение: имя контекста
   * ничего не подтверждает. Есть только у контекста с `exec`.
   */
  launchFingerprint?: string;
  /** Почему контекст нельзя импортировать; null — можно. */
  unsupported: LocalizedText | null;
  /** Подключение с тем же адресом и именем: импорт обновит его. */
  existingId: string | null;
}

export interface KubeconfigPreview {
  path: string;
  contexts: KubeContext[];
}

export interface PolicyStatus {
  managed: boolean;
  source: string | null;
  updatesEnabled: boolean;
  updateUrl: string | null;
  language: string | null;
  mcpEnabled: boolean;
  /** false — подключения без проверки TLS запрещены администратором. */
  allowInsecureTls: boolean;
  /** false — подключения с exec-плагином запрещены администратором. */
  allowExecAuth: boolean;
  errors: LocalizedText[];
}

export interface AppInfo {
  name: string;
  version: string;
  commit: string;
  os: string;
  arch: string;
  dataDir: string;
  logDir: string;
}

export type LanguageReason = "policy" | "user" | "system" | "default";

export interface LanguageSettings {
  system: string | null;
  systemMatch: string | null;
  chosen: string | null;
  policy: string | null;
  effective: string;
  reason: LanguageReason;
  languages: string[];
}
