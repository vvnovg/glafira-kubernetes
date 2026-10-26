// Данные для тестов экранов объектов.
import type { ContainerStatusView, PodRow, WorkloadRow } from "../api/types";

export const podRow = (name: string, patch: Partial<PodRow> = {}): PodRow => ({
  name,
  phase: "Running",
  status: "Running",
  problem: null,
  ready: 1,
  containers: 1,
  restarts: 0,
  lastRestart: null,
  node: "w1",
  created: 1,
  qos: "Burstable",
  owner: { kind: "Deployment", name: "web" },
  ...patch,
});

export const workloadRow = (name: string, patch: Partial<WorkloadRow> = {}): WorkloadRow => ({
  kind: "Deployment",
  name,
  desired: 3,
  ready: 3,
  available: 3,
  updated: 3,
  images: ["registry/web:3"],
  created: 1,
  problem: null,
  ...patch,
});

export const container = (patch: Partial<ContainerStatusView> = {}): ContainerStatusView => ({
  name: "web",
  image: "registry/web:3",
  init: false,
  requests: { cpu: "100m", memory: "128Mi" },
  limits: { memory: "256Mi" },
  ports: ["8080/TCP (http)"],
  liveness: null,
  readiness: "http-get http://:8080/ready delay=0s timeout=1s period=5s #success=1 #failure=3",
  startup: null,
  env: [
    { name: "LOG_LEVEL", source: "configMapKeyRef", reference: "web/level" },
    { name: "DB_PASSWORD", source: "secretKeyRef", reference: "db/password" },
  ],
  imageId: "registry/web@sha256:abc",
  state: { state: "waiting", reason: "CrashLoopBackOff", message: "back-off 5m0s", exitCode: null, started: null, finished: null },
  lastState: { state: "terminated", reason: "Error", message: null, exitCode: 1, started: 1, finished: 2 },
  ready: false,
  restarts: 7,
  ...patch,
});
