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
