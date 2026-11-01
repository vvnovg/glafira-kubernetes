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
  | "METRICS_UNAVAILABLE"
  | "ADMISSION_DENIED"
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
  /** Доступ LLM (MCP); локальное поле: экспорт его не пишет. Нет поля — `off`. */
  llmAccess?: LlmAccess;
  /** Проекты для LLM; null или нет поля — все доступные. */
  llmNamespaces?: string[] | null;
  /** Имена полей для маскировки на уровне `logs`; null или нет поля — стандартный список. Локальное поле. */
  llmMaskFields?: string[] | null;
  /** Источник метрик; локальное поле: экспорт его не пишет. Нет поля — `auto`. */
  metrics?: MetricsConfig;
}

/** Источник метрик (спец. §7.1): автопоиск или свой Thanos Querier / Prometheus. */
export type MetricsConfig = { mode: "auto" } | ({ mode: "thanos" } & MetricsEndpoint) | ({ mode: "prometheus" } & MetricsEndpoint);
export type MetricsMode = MetricsConfig["mode"];

export interface MetricsEndpoint {
  url: string;
  auth: MetricsAuth;
  ca: MetricsCa;
}

/** Токен подключения, свой bearer-токен (keychain) или без авторизации. */
export type MetricsAuth = "connection" | "bearer" | "none";
export type MetricsCa = { mode: "connection" } | { mode: "pem"; pem: string };

/** Уровень доступа LLM: `logs` — ещё логи подов и значения env и ConfigMap (после маскировки). */
export type LlmAccess = "off" | "metadata" | "logs";

/** Секреты из формы: нет поля или null — оставить сохранённый, "" — удалить, иное — сохранить. */
export interface SecretsInput {
  token?: string | null;
  clientKey?: string | null;
  metricsToken?: string | null;
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

export type TestStepName = "config" | "tcp" | "exec" | "tls" | "auth" | "discovery" | "projects" | "metrics";

export interface TestStep {
  name: TestStepName;
  ok: boolean;
  message: LocalizedText;
  error?: AppError;
  /** Пояснения под шагом: у «Метрик» — отвергнутые источники и причины. */
  notes?: LocalizedText[];
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
  /** Когда истекает токен входа (секунды Unix), если платформа это сообщает (OpenShift). */
  tokenExpiresAt: number | null;
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
  /** false — уровень доступа LLM «Метаданные и логи» запрещён администратором. */
  mcpLogsEnabled: boolean;
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

export type UpdateCheck =
  | { status: "disabled"; reason: LocalizedText }
  | { status: "upToDate" }
  | { status: "available"; version: string; notes: string | null; notesI18n: Record<string, string>; date: string | null };

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

// --- MCP-шлюз (этап O2)

export type McpServerState = "off" | "running" | "otherInstance" | "error" | "disabledByPolicy";

export interface McpSession {
  id: string;
  clientName: string | null;
  clientVersion: string | null;
  startedMs: number;
  calls: number;
  /** Идёт вызов. */
  active: boolean;
  /** Вызовов сессии в журнале, включая отказы: растёт с каждой записью. */
  logged: number;
  approved: boolean;
}

export interface McpApprovalRequest {
  sessionId: string;
  clientName: string | null;
  clientVersion: string | null;
}

export interface McpApprovedClient {
  name: string;
  version: string;
  firstSeenMs: number;
  lastSeenMs: number;
  /** Компактный набор инструментов: без заметок и предложений изменений. */
  compactTools: boolean;
}

export interface McpStatus {
  state: McpServerState;
  enabled: boolean;
  policyAllows: boolean;
  error: LocalizedText | null;
  sessions: McpSession[];
  pending: McpApprovalRequest[];
  approvedClients: McpApprovedClient[];
  /** Предел размера ответа LLM, КБ. */
  responseLimitKb: number;
  /** Подключений с доступом LLM (для диалога подтверждения). */
  allowedConnections: number;
}

export interface McpClientConfigs {
  command: string;
  args: string[];
  claudeDesktop: string;
  claudeCode: string;
  /** Команда для любого stdio-клиента: путь с пробелами — в кавычках. */
  otherClient: string;
}

/** Запись журнала MCP (`mcp-audit.jsonl`), новые первыми. */
export interface McpAuditEntry {
  ts: number;
  event: string;
  session?: string;
  client?: string | null;
  tool?: string;
  connection?: { id: string; name: string } | null;
  outcome?: "ok" | "error";
  code?: string | null;
  ms?: number;
  /** Чтение логов: строк просмотрено, отдано и замаскировано. */
  scanned?: number;
  returned?: number;
  masked?: number;
}

// --- Чтение кластера (этап O1b). Время — секунды Unix.

/** Часть экрана: данные или причина, почему их нет (права, ошибка чтения). */
export interface Part<T> {
  data?: T;
  error?: AppError;
}

/** Список; `truncated` — прочитано не всё (больше 10 000 объектов). */
export interface Listed<T> {
  items: T[];
  truncated: boolean;
}

export interface ConditionView {
  type: string;
  status: string;
  reason?: string;
  message?: string;
  lastTransition?: number;
}

export interface Readyz {
  ok: boolean;
  failed: string[];
  error?: AppError;
}

export interface RoleCount {
  role: string;
  total: number;
  ready: number;
}

export interface NodeSummary {
  total: number;
  ready: number;
  roles: RoleCount[];
}

export interface IngressClassRow {
  name: string;
  controller: string | null;
  default: boolean;
}

export interface GatewayClassRow {
  name: string;
  controller: string | null;
  accepted: boolean | null;
}

export interface ProjectCount {
  count: number;
  more: boolean;
}

export interface ClusterVersionInfo {
  version: string | null;
  channel: string | null;
  availableUpdates: number;
  failing: ConditionView | null;
  progressing: ConditionView | null;
}

export interface OperatorRow {
  name: string;
  version: string | null;
  available: boolean | null;
  progressing: boolean | null;
  degraded: boolean | null;
  message: string | null;
}

export interface MachineConfigPoolRow {
  name: string;
  degraded: boolean;
  updating: boolean;
  machines: number;
  ready: number;
  updated: number;
  degradedMachines: number;
}

/** Часть обзора OpenShift. */
export interface OpenShiftOverview {
  kind: "openshift";
  clusterVersion: Part<ClusterVersionInfo>;
  /** `data` null — объект есть, значения нет; `error` — не прочитан (например, нет прав на консоль). */
  infrastructure: Part<string | null>;
  consoleUrl: Part<string | null>;
  operators: Part<{ total: number; problems: OperatorRow[] }>;
  machineConfigPools: Part<MachineConfigPoolRow[]>;
}

export interface ClusterOverview {
  platform: PlatformInfo;
  serverVersion: string;
  readyz: Readyz;
  nodes: Part<NodeSummary>;
  ingressClasses: Part<IngressClassRow[]>;
  /** null — в кластере нет Gateway API. */
  gatewayClasses: Part<GatewayClassRow[]> | null;
  projects: Part<ProjectCount>;
  platformDetails: OpenShiftOverview | null;
}

export interface NodeRow {
  name: string;
  roles: string[];
  ready: boolean;
  problems: string[];
  unschedulable: boolean;
  kubeletVersion: string | null;
  internalIp: string | null;
  cpu: string | null;
  memory: string | null;
  pods: string | null;
  created: number | null;
  taints: number;
}

export interface NodeDetails {
  node: NodeRow;
  conditions: ConditionView[];
  taints: string[];
  labels: Record<string, string>;
  capacity: Record<string, string>;
  allocatable: Record<string, string>;
  addresses: string[];
  osImage: string | null;
  kernelVersion: string | null;
  containerRuntime: string | null;
  architecture: string | null;
}

export interface ProjectRow {
  name: string;
  displayName: string | null;
  phase: string | null;
  favorite: boolean;
  /** Проект из профиля (ручной список или избранное), а не из списка кластера. */
  fromProfile: boolean;
}

export interface ProjectList {
  items: ProjectRow[];
  truncated: boolean;
  /** Почему кластер не дал список (нет прав): тогда в списке только проекты из профиля. */
  error?: AppError;
}

export interface Verbs {
  get: boolean;
  list: boolean;
  watch: boolean;
  /** Изменения (§9): ресурсы, перезапуск, масштаб, границы HPA. */
  patch: boolean;
  /** Удаление пода. */
  delete: boolean;
  /** `instantiate` DeploymentConfig. */
  create: boolean;
}

/** Права в проекте: `ресурс.группа` (`pods`, `deployments.apps`, `pods/log`) → права. */
export interface NamespaceAccess {
  resources: Record<string, Verbs>;
  /** Сервер перечислил не все права — интерфейс ничего не скрывает. */
  incomplete: boolean;
}

export type WorkloadKind = "Deployment" | "StatefulSet" | "DaemonSet" | "DeploymentConfig" | "Job" | "CronJob";
export type WorkloadProblem = "unavailable" | "rolloutStuck" | "failed";

export interface WorkloadRow {
  kind: WorkloadKind;
  name: string;
  desired: number | null;
  ready: number | null;
  available: number | null;
  updated: number | null;
  images: string[];
  created: number | null;
  problem: WorkloadProblem | null;
  schedule?: string | null;
  suspended?: boolean | null;
  lastSchedule?: number | null;
}

export interface KindError {
  kind: WorkloadKind;
  error: AppError;
}

export interface WorkloadList {
  items: WorkloadRow[];
  errors: KindError[];
  truncated: boolean;
}

export type EnvSource = "literal" | "configMapKeyRef" | "secretKeyRef" | "fieldRef" | "resourceFieldRef" | "configMapRef" | "secretRef";

/** Переменная окружения: имя и источник; значения нет. */
export interface EnvView {
  name: string;
  source: EnvSource;
  reference: string | null;
}

export interface ContainerSpecView {
  name: string;
  image: string;
  init: boolean;
  requests: Record<string, string>;
  limits: Record<string, string>;
  ports: string[];
  liveness: string | null;
  readiness: string | null;
  startup: string | null;
  env: EnvView[];
}

export interface HpaView {
  name: string;
  minReplicas: number | null;
  maxReplicas: number;
  currentReplicas: number | null;
  desiredReplicas: number;
  metrics: string[];
  conditions: ConditionView[];
}

export interface PdbView {
  name: string;
  minAvailable: string | null;
  maxUnavailable: string | null;
  allowedDisruptions: number;
  currentHealthy: number;
  desiredHealthy: number;
  expectedPods: number;
}

export interface Revision {
  revision: number;
  name: string;
  images: string[];
  created: number | null;
  replicas: number | null;
  current: boolean;
}

export interface WorkloadDetails {
  workload: WorkloadRow;
  conditions: ConditionView[];
  selector: string | null;
  strategy: string | null;
  containers: ContainerSpecView[];
  labels: Record<string, string>;
  annotations: Record<string, string>;
  activeJobs: string[];
  hpa: Part<HpaView | null>;
  pdb: Part<PdbView | null>;
  /** null — у Job и CronJob истории нет. */
  history: Part<Revision[]> | null;
}

export type PodProblem = "imagePull" | "oomKilled" | "crashLoop" | "pending" | "failed";

export interface OwnerRef {
  kind: string;
  name: string;
}

export interface PodRow {
  name: string;
  phase: string;
  /** Статус, как в `kubectl get pods`. */
  status: string;
  problem: PodProblem | null;
  ready: number;
  containers: number;
  restarts: number;
  lastRestart: number | null;
  node: string | null;
  created: number | null;
  qos: string | null;
  owner: OwnerRef | null;
}

export interface StateView {
  state: "running" | "waiting" | "terminated";
  reason: string | null;
  message: string | null;
  exitCode: number | null;
  started: number | null;
  finished: number | null;
}

export interface ContainerStatusView extends ContainerSpecView {
  imageId: string | null;
  state: StateView | null;
  lastState: StateView | null;
  ready: boolean;
  restarts: number;
}

export interface VolumeView {
  name: string;
  kind: string;
  source: string | null;
}

export interface PodDetails {
  pod: PodRow;
  conditions: ConditionView[];
  podIp: string | null;
  hostIp: string | null;
  serviceAccount: string | null;
  started: number | null;
  containers: ContainerStatusView[];
  labels: Record<string, string>;
  annotations: Record<string, string>;
  volumes: VolumeView[];
}

export interface WorkloadRef {
  kind: WorkloadKind;
  name: string;
}

export interface EventRow {
  key: string;
  /** `Normal` или `Warning`. */
  type: string;
  reason: string;
  object: OwnerRef;
  message: string;
  count: number;
  firstSeen: number | null;
  lastSeen: number | null;
  source: string | null;
}

/** Чьи события: объекта или workload'а вместе с его подами. */
export interface EventObject {
  kind: string;
  name: string;
}

export interface EndpointCount {
  ready: number;
  total: number;
}

export interface ServiceRow {
  name: string;
  type: string;
  clusterIp: string | null;
  external: string[];
  ports: string[];
  selector: string | null;
  endpoints: EndpointCount | null;
  created: number | null;
}

export interface RouteRow {
  name: string;
  host: string | null;
  path: string | null;
  service: string | null;
  port: string | null;
  tls: string | null;
  insecurePolicy: string | null;
  admitted: boolean | null;
  admittedMessage: string | null;
  certExpires: number | null;
  ingress: string | null;
  created: number | null;
}

export interface IngressPath {
  host: string | null;
  path: string | null;
  service: string | null;
  port: string | null;
}

export interface IngressTls {
  hosts: string[];
  secret: string | null;
}

export interface IngressRow {
  name: string;
  class: string | null;
  rules: IngressPath[];
  defaultBackend: string | null;
  tls: IngressTls[];
  addresses: string[];
  routes: string[];
  created: number | null;
}

export interface ListenerView {
  name: string;
  port: number | null;
  protocol: string | null;
  hostname: string | null;
}

export interface GatewayRow {
  name: string;
  class: string | null;
  listeners: ListenerView[];
  addresses: string[];
  accepted: boolean | null;
  programmed: boolean | null;
  created: number | null;
}

export interface ParentView {
  name: string;
  namespace: string | null;
  section: string | null;
  accepted: ConditionView | null;
  resolvedRefs: ConditionView | null;
}

export interface GatewayRouteRow {
  kind: "HTTPRoute" | "GRPCRoute";
  name: string;
  hostnames: string[];
  parents: ParentView[];
  backends: string[];
  created: number | null;
}

/** Сеть проекта; null — вида нет в кластере (нет группы API). */
export interface NetworkView {
  services: Part<ServiceRow[]>;
  endpointsError: AppError | null;
  routes: Part<RouteRow[]> | null;
  ingresses: Part<IngressRow[]>;
  gateways: Part<GatewayRow[]> | null;
  httpRoutes: Part<GatewayRouteRow[]> | null;
  grpcRoutes: Part<GatewayRouteRow[]> | null;
}

export interface ConfigMapRow {
  name: string;
  keys: string[];
  binaryKeys: string[];
  created: number | null;
}

/** Secret: имя, тип и ключи — значения в интерфейс не приходят. */
export interface SecretRow {
  name: string;
  type: string;
  keys: string[];
  created: number | null;
}

export interface ConfigView {
  configMaps: Part<ConfigMapRow[]>;
  secrets: Part<SecretRow[]>;
}

export interface ConfigValue {
  text: string | null;
  binarySize: number | null;
}

export interface PvcRow {
  name: string;
  phase: string | null;
  class: string | null;
  capacity: string | null;
  requested: string | null;
  accessModes: string[];
  volumeMode: string | null;
  volume: string | null;
  /** null — поды не читаются, неизвестно. */
  mountedBy: string[] | null;
  created: number | null;
}

export interface QuotaEntry {
  resource: string;
  used: string | null;
  hard: string;
  ratio: number | null;
}

export interface QuotaView {
  name: string;
  scopes: string[];
  entries: QuotaEntry[];
}

export interface LimitView {
  type: string;
  resource: string;
  min: string | null;
  max: string | null;
  default: string | null;
  defaultRequest: string | null;
  maxLimitRequestRatio: string | null;
}

export interface LimitRangeView {
  name: string;
  limits: LimitView[];
}

export interface QuotasView {
  quotas: Part<QuotaView[]>;
  limitRanges: Part<LimitRangeView[]>;
}

/** Виды объектов с вкладкой YAML. */
export type YamlKind =
  | WorkloadKind
  | "Pod"
  | "Service"
  | "Route"
  | "Ingress"
  | "ConfigMap"
  | "PersistentVolumeClaim"
  | "Gateway"
  | "HTTPRoute"
  | "GRPCRoute";

export type WatchKind = "pods" | "workloads" | "events";

export interface WatchParams {
  connectionId: string;
  namespace: string;
  kind: WatchKind;
  workload?: WorkloadRef | null;
}

export type WatchRow = PodRow | WorkloadRow | EventRow;
export type RowChange = { op: "upsert"; row: WatchRow } | { op: "delete"; key: string };

export type WatchEvent =
  /** Список целиком; `truncated` — сессия держит предельные 10 000 строк, остальные не показываются. Rust присылает его всегда, нет значения — `false`. */
  | { type: "reset"; items: WatchRow[]; errors: KindError[]; truncated?: boolean }
  | { type: "changes"; changes: RowChange[] }
  | { type: "polling" }
  | { type: "error"; error: AppError; fatal: boolean };

export interface LogFilter {
  text: string;
  regex: boolean;
  caseSensitive: boolean;
}

export interface LogParams {
  connectionId: string;
  namespace: string;
  pod: string;
  container?: string | null;
  previous?: boolean;
  tailLines?: number | null;
  sinceSeconds?: number | null;
  sinceTime?: string | null;
  follow?: boolean;
  filter?: LogFilter | null;
}

export interface LogLine {
  ts: string | null;
  text: string;
  truncated: boolean;
}

export type LogEvent =
  | { type: "lines"; lines: LogLine[]; scanned: number; shown: number }
  | { type: "reconnecting"; since: string | null }
  | { type: "end"; scanned: number; shown: number }
  | { type: "error"; error: AppError };

// --- Проверка кластера и замечания (этап O2). Время — миллисекунды Unix.

export type Severity = "info" | "warning" | "critical";
export type TargetKind =
  | "cluster"
  | "node"
  | "project"
  | "workload"
  | "pod"
  | "service"
  | "route"
  | "ingress"
  | "httproute"
  | "grpcroute"
  | "gateway"
  | "pvc"
  | "hpa"
  | "quota";

/** Объект находки или замечания: у кластера нет ни проекта, ни имени, у ноды — только имя. */
export interface HealthTarget {
  kind: TargetKind;
  namespace?: string;
  name?: string;
  /** Только у `workload`. */
  workloadKind?: WorkloadKind;
}

/** Находка правила; тексты — `health.rules.<ruleId>.*` с `params`. */
export interface Finding {
  key: string;
  ruleId: string;
  severity: Severity;
  target: HealthTarget;
  params: Record<string, unknown>;
}

export type CheckTrigger = { kind: "ui" } | { kind: "mcp"; client?: string };

/** Что проверено: проекты (не больше 50), поды (не больше 5000), ноды (`null` — не прочитаны). */
export interface CheckScope {
  projects: string[];
  projectsTotal: number;
  pods: number;
  nodes: number | null;
  /** Проекты, которые попросил LLM; нет поля — все доступные. */
  requested?: string[];
}

export interface CheckRun {
  runId: string;
  startedAtMs: number;
  durationMs: number;
  trigger: CheckTrigger;
  environment: Environment;
  incomplete: boolean;
  scope: CheckScope;
  findings: Finding[];
}

export type AnnotationStatus = "open" | "resolved" | "dismissed";

/** Замечание LLM: текст — как его написал ассистент. */
export interface Annotation {
  id: string;
  target: HealthTarget;
  severity: Severity;
  title: string;
  body: string;
  relatedRuleId?: string;
  client: string | null;
  sessionId?: string;
  createdAtMs: number;
  updatedAtMs: number;
  status: AnnotationStatus;
  closedAtMs?: number;
}

/** Последняя проверка, ключи скрытых находок, замечания и предложения LLM подключения. */
export interface HealthView {
  lastCheck: CheckRun | null;
  ignored: string[];
  annotations: Annotation[];
  proposals: Proposal[];
}

/** Источник метрик (спец. §7.1). */
export type MetricsSourceKind = "thanos" | "prometheus" | "metricsServer";

export interface MetricsSourceInfo {
  kind: MetricsSourceKind;
  /** `auto` — найден автопоиском, `profile` — свой из настроек подключения. */
  origin: "auto" | "profile";
  /** Маршрут или сервис кластера, у своего источника — адрес. */
  location: string;
  history: boolean;
}

export type MetricsRejectReason = "forbidden" | "notFound" | "unavailable" | "timeout" | "badResponse" | "tokenMissing";

export interface MetricsRejected {
  source: MetricsSourceInfo;
  reason: MetricsRejectReason;
  message: LocalizedText;
  error: AppError;
}

/** Какой источник выбран и почему отвергнуты остальные. */
export interface MetricsSourceStatus {
  mode: MetricsMode;
  selected: MetricsSourceInfo | null;
  rejected: MetricsRejected[];
  /** Группа metrics.k8s.io есть: без истории — текущие значения (только в режиме auto). */
  podMetrics: boolean;
  checkedAtMs: number;
}

// --- Ресурсы, изменения и предложения (этап O3). CPU — в ядрах, память — в байтах, время — секунды Unix.

/** CPU и память; `null` — не задано или нет данных. */
export interface Amounts {
  cpu: number | null;
  memory: number | null;
}

/** Действующие requests и limits контейнера из шаблона и значения `LimitRange` по умолчанию у работающих подов. */
export interface ContainerCurrent {
  name: string;
  init: boolean;
  sidecar: boolean;
  requests: Amounts;
  limits: Amounts;
  limitRangeRequests: Amounts;
  limitRangeLimits: Amounts;
  envNames: string[];
}

export interface ContainerStats {
  cpu: { p50: number | null; p95: number | null; p99: number | null; max: number | null };
  memory: { p95: number | null; p99: number | null; max: number | null };
  /** Доля периодов CFS с троттлингом, 0–1. */
  throttling: { mean: number | null; p95: number | null };
  /** `null` — нет kube-state-metrics. */
  oomCount: number | null;
  oomLast: number | null;
  restarts: number | null;
}

export type ContainerUsage = ContainerCurrent & { stats: ContainerStats };

export interface ChartSeries {
  container: string;
  /** [секунды Unix, значение]. */
  points: [number, number][];
}

export interface ChartMarker {
  container: string;
  time: number;
  count: number;
}

export interface Charts {
  start: number;
  end: number;
  stepSeconds: number;
  cpu: ChartSeries[];
  memory: ChartSeries[];
  restarts: ChartMarker[];
  ooms: ChartMarker[];
}

export interface ContainerNow {
  name: string;
  cpu: number | null;
  memory: number | null;
}

export interface PodNow {
  pod: string;
  containers: ContainerNow[];
}

/** Статистика workload'а за окно (спец. §7.2). */
export interface WorkloadUsage {
  namespace: string;
  kind: WorkloadKind;
  name: string;
  windowDays: number;
  source: MetricsSourceInfo;
  sourceStatus: MetricsSourceStatus;
  /** Доля шагов окна с данными, 0–1; у metrics.k8s.io — `null`. */
  coverage: number | null;
  dataSeconds: number | null;
  ownerMetrics: boolean;
  replicas: number | null;
  surge: number;
  containers: ContainerUsage[];
  charts: Charts | null;
  /** Только metrics.k8s.io: текущее потребление подов. */
  podsNow: PodNow[];
}

export interface PodUsage {
  source: MetricsSourceInfo;
  sourceStatus: MetricsSourceStatus;
  containers: ContainerNow[];
}

export type Confidence = "low" | "medium" | "high";
export type ResourceField = "cpuRequest" | "cpuLimit" | "memoryRequest" | "memoryLimit";
export type LimitBound = "min" | "max" | "ratio";

export type RecommendationNote =
  | { code: "cpuLimitNotSet" }
  | { code: "cpuThrottled"; p95: number }
  | { code: "oomKilled"; count: number }
  | { code: "noData" }
  | { code: "limitRange"; field: ResourceField; bound: LimitBound }
  | { code: "limitRangeDefault"; field: ResourceField }
  | { code: "java" }
  | { code: "go" };

export type Violation =
  | { code: "quota"; quota: string; resource: string; needed: number; free: number }
  | { code: "nodeAllocatable"; container: string; resource: string; request: number; allocatable: number };

export interface HpaShift {
  hpa: string;
  resource: string;
  target: number;
  equivalent: number;
}

export interface ResourceSet {
  requests: Amounts;
  limits: Amounts;
}

export interface ContainerRecommendation {
  name: string;
  current: ResourceSet;
  /** `null` — данных нет. */
  recommended: ResourceSet | null;
  notes: RecommendationNote[];
}

/** Рекомендация requests и limits (спец. §7.3). */
export interface Recommendation {
  namespace: string;
  kind: WorkloadKind;
  name: string;
  windowDays: number;
  source: MetricsSourceInfo;
  confidence: Confidence;
  coverage: number | null;
  dataSeconds: number | null;
  replicas: number | null;
  containers: ContainerRecommendation[];
  violations: Violation[];
  hpa: HpaShift[];
  /** Рекомендовано минус сейчас, × реплик. */
  totals: ResourceSet;
}

export interface Analysis {
  usage: WorkloadUsage;
  recommendation: Recommendation;
}

export type Risk =
  | { code: "throttled"; container: string; mean: number }
  | { code: "oom"; container: string; count: number }
  | { code: "memoryNearLimit"; container: string; max: number; limit: number };

export interface Excess {
  container: string;
  resource: "cpu" | "memory";
  request: number;
  usage: number;
}

/** Workload во вкладке «Ресурсы» проекта: суммы по работающим подам. */
export interface WorkloadResources {
  kind: WorkloadKind;
  name: string;
  pods: number;
  requests: Amounts;
  limits: Amounts;
  usage: Amounts;
  reserve: Amounts;
  risks: Risk[];
  excess: Excess[];
}

export interface QuotaRoom {
  quota: string;
  resource: string;
  hard: number;
  used: number;
}

export interface ProjectUsage {
  namespace: string;
  windowDays: number;
  source: MetricsSourceInfo;
  sourceStatus: MetricsSourceStatus;
  workloads: WorkloadResources[];
  errors: KindError[];
  truncated: boolean;
  quotas: QuotaRoom[];
  collectedAtMs: number;
}

export type ProjectUsageEvent = { kind: "done"; usage: ProjectUsage } | { kind: "failed"; error: AppError };

/** Значение quantity строкой; `null` — убрать; нет поля — не менять. */
export interface ResourceValues {
  cpu?: string | null;
  memory?: string | null;
}

export interface ContainerResources {
  name: string;
  requests: ResourceValues;
  limits: ResourceValues;
}

/** Изменение кластера (спец. §9): одна форма у интерфейса, предложений и LLM. */
export type Change =
  | { kind: "setResources"; namespace: string; workload: WorkloadRef; containers: ContainerResources[] }
  | { kind: "scale"; namespace: string; workload: WorkloadRef; replicas: number }
  | { kind: "rolloutRestart"; namespace: string; workload: WorkloadRef }
  | { kind: "setHpaBounds"; namespace: string; hpa: string; minReplicas?: number; maxReplicas: number }
  | { kind: "deletePod"; namespace: string; pod: string };

export type ChangeKind = Change["kind"];

/** Подтверждение изменения (`ConfirmDialog`): имя prod-подключения и введённое имя объекта (пода). */
export interface Confirmation {
  confirm: string | null;
  typedName: string | null;
}

/** Поле «было → станет»: `requests.cpu`, `replicas`, `restartedAt`, `pod`… */
export interface FieldChange {
  container: string | null;
  field: string;
  before: string | null;
  after: string | null;
}

export type ChangeWarning =
  | { code: "hpaManaged"; hpa: string }
  | { code: "scaleToZero" }
  | { code: "quota"; quota: string; resource: string; needed: number; free: number }
  | { code: "orphan" }
  | { code: "unverified"; what: "hpa" | "limitRange" | "quota" };

export type DryRun = { status: "passed" } | { status: "rejected"; error: AppError } | { status: "skipped" };

export interface ChangePreview {
  change: Change;
  /** Версия объекта: g<generation>, без generation — s<хэш spec> (HPA до Kubernetes 1.37), у пода u<uid>; без неё применение — CONFLICT. */
  version: string | null;
  diff: FieldChange[];
  warnings: ChangeWarning[];
  dryRun: DryRun;
  /** Под без контроллера: при применении нужно ввести его имя. */
  confirmPodName: boolean;
}

export type ProposalStatus = "open" | "applied" | "rejected" | "withdrawn";

/** Предложение LLM (спец. §10.5): изменение, которое человек применяет формой. */
export interface Proposal {
  id: string;
  severity: Severity;
  title: string;
  rationale: string;
  target: HealthTarget;
  change: Change;
  relatedRuleId?: string;
  relatedFindingKey?: string;
  client: string | null;
  sessionId?: string;
  createdAtMs: number;
  updatedAtMs: number;
  status: ProposalStatus;
  closedAtMs?: number;
  appliedAsProposed?: boolean;
  appliedChange?: Change;
  rejectReason?: string;
}
