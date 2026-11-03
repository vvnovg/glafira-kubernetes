# Changelog

Versions in the other interface languages are in [docs/changelog](docs/changelog/). The section of a version is shown in About as the update notes, in the interface language.

## 0.2.0
Resources and changes, logs for LLM assistants, local models, Platform V DropApp and OIDC sign-in.
- Platform V DropApp:
  - detected by its API groups, with the DropApp version in the overview and for the assistant;
  - projects from DropApp projects, VictoriaMetrics found automatically;
  - the cluster overview shows DropApp components, tools, nodes and ingress certificates; three new cluster rules;
  - the guide chapter «Platform V DropApp».
- Sign-in via OIDC (Dex and other providers) in the browser: the values come from a kubeconfig with `kubectl oidc-login` (for DropApp — the one Loginapp issues), the token is refreshed on its own; the issuer CA can be set separately.
- OpenShift 4.14 and older: connecting no longer fails with 403 when the server has no SelfSubjectReview API. A connection with an expired token offers sign-in via browser, and the tree offers to sign in again.
- Resources and changes:
  - CPU and memory usage from the cluster metrics (Thanos, Prometheus, metrics.k8s.io) with charts;
  - requests and limits recommended from the actual usage;
  - changes with a server-side dry run: requests and limits, scale, restart, HPA bounds;
  - the assistant can propose a change; you see it next to the object and apply or reject it yourself.
- Logs for LLM assistants: a separate access level «Metadata and logs», pod logs and a search over the logs of a workload, with masking of secret-looking values.
- Local models: argument hints, a response size limit (100, 48 or 24 KB) and a compact tool set for a client; the guide chapter «Local model».
- Fixes from the smoke tests: a queue for long operations, a live MCP log, clearer texts.

## 0.1.0
First version of Glafira for Kubernetes: a desktop app for reading and diagnosing Kubernetes clusters, with a focus on OpenShift.
- Connections:
  - to Kubernetes and OpenShift API servers, including an address with a path behind a proxy (Rancher);
  - sign-in via browser (OpenShift OAuth), a token, a client certificate or an allowed exec plugin;
  - import from kubeconfig, folders, export and import;
  - secrets are kept only in the system keychain;
  - on connect, Glafira detects the platform and checks what you may read.
- Reading the cluster:
  - the cluster overview, nodes, projects and workloads;
  - pods with their logs, events, network objects (Services, Routes, Ingresses, Gateway API);
  - configuration without the values of Secrets and ConfigMaps, storage and quotas;
  - YAML and automatic refresh.
- Cluster check:
  - 34 rules: crash loops, image pulls, pending pods, unavailable workloads and stuck rollouts, requests, limits and probes, Services without endpoints, Ingress, Route and Gateway API problems, quotas, PVCs, HPAs, OpenShift operators and nodes;
  - severities depend on the connection's environment;
  - the Findings tab with filters and hiding, badges in the tree and tables, blocks on object screens.
- MCP gateway for LLM assistants (Claude Desktop, Claude Code and other stdio clients):
  - each client is approved in Glafira when it connects;
  - LLM access is set per connection (off, or metadata of all or listed projects) and is not exported;
  - values that look like secrets are masked;
  - every call is written to the MCP log;
  - the assistant reads metadata and runs the check; the clusters cannot be changed, and its notes are kept in Glafira.
- Updates from About → Updates, signed installers for macOS (signed and notarized by Apple), Windows and Linux.
- Guide in English and Russian.
