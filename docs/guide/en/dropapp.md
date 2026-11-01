# Platform V DropApp

Platform V DropApp (SberTech) is a Kubernetes distribution with its own platform components, sign-in through Dex and
VictoriaMetrics for metrics. Glafira recognizes it and adds what plain Kubernetes lacks: sign-in via OIDC, projects
from `projects.dropapp.ru`, the state of the platform in the cluster overview, three check rules and the metrics
history from VictoriaMetrics. Everything else works as on any Kubernetes cluster.

## How Glafira recognizes DropApp {#detection}

A cluster with the `config.dropapp.ru` API group is DropApp: the group appears in clusters of version 3.x and later
installed with `dactl`. Any signed-in user may read the list of API groups, so a user of one namespace gets DropApp too.
DropApp 1.x–2.x has no such group and works as Kubernetes.

[[ui:cluster.connection.platform]] shows the DropApp version from `ClusterMetadata` (`spec.dappVersion`), for example
“DropApp 5.1.2”. If `ClusterMetadata` cannot be read (a namespace user usually has no right to it), the label carries
the Kubernetes version instead: “DropApp (Kubernetes 1.29)”. An LLM assistant sees the same platform and label in
`list_connections` and `get_cluster_overview`.

## Sign-in {#sign-in}

DropApp signs people in through Dex: the Dex page asks for the name and password of the directory (LDAP). Glafira does
not ask for the password itself: you enter it on the Dex page in the browser.

- **Kubeconfig from Loginapp.** Loginapp (`https://loginapp.<domain>`) issues a kubeconfig with the exec plugin
  `kubectl oidc-login get-token` and the issuer, the client and its secret in the arguments. Import it with
  [[ui:sidebar.importKubeconfig]] and keep [[ui:kubeconfig.useOidc]] for the context: Glafira takes the issuer, the
  client, the secret, the scopes and the issuer CA from the arguments.
- **By hand.** Choose [[ui:connections.form.auth.oidc]] in the connection form: the issuer is
  `https://auth.<cluster>.<domain>`, the client is `dapp`, and the client secret comes from the administrator or from
  the Loginapp kubeconfig (`--oidc-client-secret`).

Then click [[ui:cluster.session.loginOidc]] on the connection tab. The fields, the token lifetime and the refresh are
described in [Sign-in via OIDC](connections.md#oidc).

**Ports 8000 and 18000.** Dex returns you only to the addresses registered for the `dapp` client. Glafira uses the
addresses of kubelogin: `http://localhost:8000`, and if port 8000 is busy, `http://localhost:18000`. If both ports are
busy, the sign-in fails with a hint. Usually they are held by kubelogin waiting for a sign-in in another terminal:
finish or close it. To find the program, run `lsof -i :8000` on macOS and Linux or `netstat -ano | findstr :8000` on
Windows. If the `dapp` client of your cluster has no such addresses, ask the DropApp administrator to add them to the
Dex client (they are in the default configuration).

**Issuer CA.** Dex is behind the ingress, and its certificate is often issued by another CA than the API server. The
Loginapp kubeconfig names that CA in the plugin argument `--certificate-authority` (a file) or
`--certificate-authority-data`; the import copies it into [[ui:connections.form.oidc.issuerCaPem]]. If the file cannot
be read, is not a PEM certificate, also contains a private key, is larger than 1 MiB or is not a regular file, the
import warns and creates the connection without the CA: paste the certificate into the form. Without the right CA the
sign-in stops with “The certificate of the OIDC issuer … is not trusted”. After changing the issuer, the client ID or
the issuer CA, sign in again: the saved tokens are deleted.

**The exec plugin as a fallback.** If sign-in via OIDC does not suit your cluster, choose [[ui:kubeconfig.useExec]] on
import: Glafira runs `kubectl oidc-login` as the Loginapp kubeconfig says, and the plugin opens the browser itself (see
[Exec plugins](connections.md#exec)). The plugin must be installed, and your organization may forbid exec plugins. The
DropApp documentation spells the plugin version as `client.authentication.io/v1beta1`; with it the exec variant is
unavailable, and the dialog says why. Fix it to `client.authentication.k8s.io/v1beta1` in the file. Sign-in via OIDC
is not affected.

## Projects {#projects}

In DropApp 5.x the list of projects comes from `projects.dropapp.ru`: this API returns only the projects you have access
to, and the project name is the namespace name. Glafira takes the list from it when the cluster has no OpenShift
projects, so a namespace user sees their projects without entering them by hand. Clusters of 3.x and 4.x have no such
API: Glafira lists namespaces, and if you may not list them, enter your projects in
[[ui:connections.form.manualNamespaces]].

## Cluster overview and check {#overview}

On DropApp the [[ui:cluster.overview.title]] gets a DropApp section:

- the cluster name and the DropApp version (`ClusterMetadata`);
- [[ui:cluster.dropapp.components]] (`Component` and `Tool`): how many there are and which are not fine — `failed`,
  `error` or `upgrading` — with the status, the number of retries and the last `Warning` event of the object (for the
  first 20 of them);
- [[ui:cluster.dropapp.nodes]] (`DropAppNode`) that are not `ready`;
- [[ui:cluster.dropapp.ingressTls]]: `IngressTLS` objects whose `Ready` condition is not `True`. An object without the
  condition has not been processed yet and is not counted.

A status Glafira does not know is shown as is and is not counted as a problem. A part the cluster does not have is
empty; a part you may not read shows a dash (—) with the reason: a namespace user usually sees dashes here. With sign-in
via OIDC the connection tab also shows [[ui:cluster.connection.tokenExpires]].

The cluster check (see [Cluster check and notes](health-check.md#rules)) adds three rules:

| Rule | When | prod, stage | other |
|---|---|---|---|
| [[ui:health.ruleNames.cluster.componentFailed]] | a `Component` or `Tool` is `failed` or `error` | [[ui:health.severity.critical]] | [[ui:health.severity.warning]] |
| [[ui:health.ruleNames.cluster.dropappNodeNotReady]] | a `DropAppNode` is not `ready` | [[ui:health.severity.warning]] | [[ui:health.severity.info]] |
| [[ui:health.ruleNames.cluster.ingressTlsNotReady]] | the `Ready` condition of an `IngressTLS` is not `True` | [[ui:health.severity.warning]] | [[ui:health.severity.info]] |

`upgrading` is not a failure: an update in progress cannot be told from a stuck one. The cause of a failure is in the
events of the object: `kubectl describe tool NAME` (or `component NAME`). If you may not read these objects, the check
is marked [[ui:health.partial]].

## Metrics {#metrics}

DropApp keeps the metrics history in VictoriaMetrics in the `dapp-monitoring` namespace. In the
[[ui:connections.form.metricsMode.auto]] mode Glafira first tries
`dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481` through the API server proxy (the Prometheus API is at
`/select/0/prometheus`), then the usual Kubernetes candidates, then the current values from `metrics.k8s.io`.

- **Administrator.** The proxy needs the `get services/proxy` right in `dapp-monitoring`. A DropApp administrator has
  it, and the history works without settings.
- **Namespace user.** VictoriaMetrics does not separate the metrics by namespace, so such users have no right to the
  proxy. The [[ui:connections.form.step.metrics]] step of the connection test shows that VictoriaMetrics was rejected
  for lack of the right, and Glafira takes the current values from `metrics.k8s.io`: without a history, the resource
  rules of the check do not run. For the history, set your own source:
  - the ingress `metrics.<cluster>.<domain>`: [[ui:connections.form.metricsMode.prometheus]] with the address
    `https://metrics.<cluster>.<domain>/select/0/prometheus` and the authorization the administrator set up for the
    ingress;
  - `vmauth`: the `vmauth` address with the path its `url_map` sends to vmselect, and your own token (the JWT or
    `bearer_token` from the administrator). Authorization by name and password is not supported.

  Both are off by default: the DropApp administrator turns them on in the values of the `victoria-metrics-k8s-stack`
  tool. Without filters such a source shows the metrics of the whole cluster; `vmauth` can narrow them with the
  `vm_access` claim of the token.

The fields of your own source are described in [Metrics](connections.md#metrics).

## What Glafira does not show on DropApp {#not-supported}

- `TenantCluster` (vcluster): its kubeconfig is kept in a Secret, which Glafira does not read. Import the kubeconfig of
  the tenant cluster as a separate connection.
- A link to the DropApp console: its address is not written in any object. The console is usually at
  `https://console.<cluster>.<domain>`.
- Loki: Glafira reads pod logs through the API server, as on any cluster. The logs of deleted pods are only in Loki
  (Grafana).
- Route and DeploymentConfig: DropApp has none, use Ingress and Deployment. An LLM assistant is told this too.
- Sign-in with a name and password in the Glafira window: the password goes only to the Dex page.

## DropApp documentation {#docs}

The public documentation of DropApp is at https://platformv.sbertech.ru/docs/public/K8S/. Glafira follows version
5.1.2: the API groups, the Dex client, the VictoriaMetrics address and the platform namespaces are taken from it.
