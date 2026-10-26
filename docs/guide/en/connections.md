# Connections

A connection is the API server address, the sign-in method and the TLS settings of one cluster. Passwords, tokens
and keys are kept in the system secret store (Keychain, Credential Manager, Secret Service), never in files. The one
exception is the secret values in the arguments and environment of an exec plugin: they stay in the connections file
(`connections.json`) in the data folder, whose path is shown in [[ui:sidebar.about]]. Only you can read that file, like
kubeconfig, and the values are not exported.

## API server address {#api-url}

Use the address of the API server, as in `oc login` or the `server` of a kubeconfig cluster:
`https://api.<cluster>:6443`. A path is allowed, for example the Rancher proxy
`https://rancher.example.com/k8s/clusters/c-xxxxx`. The web console address
(`https://console-openshift-console.apps…`) does not work here. Only `https://` addresses are accepted.

## Sign-in methods {#sign-in}

- **Browser** — like `oc login --web`: Glafira opens the OpenShift sign-in page in your browser and receives the token
  after you sign in. Use [[ui:cluster.session.loginBrowser]] on the connection tab. This works only on OpenShift: other
  clusters have no OpenShift OAuth server, and the sign-in fails with a hint.
- **Token** — paste the output of `oc whoami -t` or a service account token.
- **Client certificate** — a certificate and a key in PEM format, for example from the installer kubeconfig.
- **Exec plugin** — a command that issues credentials, as in kubeconfig: `aws eks get-token`, `aws-iam-authenticator`,
  `gke-gcloud-auth-plugin`, `kubelogin get-token`, `kubectl oidc-login get-token`, `oc get-token`,
  `yc k8s create-token`, `rancher token`. Other commands are not run.

OpenShift tokens usually expire after 24 hours. When the session expires, the connection tab offers to sign in again
or to paste a new token. If sign-in via browser is not available on the cluster, use
[[ui:cluster.session.tokenPage]]: it opens the token page of the cluster; copy the token from it and paste it.

## Exec plugins {#exec}

Glafira runs the plugin by its full path, without a shell. An app started from the Dock or an application menu does
not see the `PATH` of your terminal, so enter the command name and click [[ui:connections.form.exec.find]]: Glafira
looks in the `PATH` of your login shell and in the standard folders (`/opt/homebrew/bin`, `/usr/local/bin`,
`~/.local/bin`). On Linux the search is the same; Windows has no login shell, so Glafira looks in the `PATH` of the
app, in `%LOCALAPPDATA%\Microsoft\WinGet\Links` and in `%LOCALAPPDATA%`.

Before the arguments of the command, such as `eks get-token`, the line may have only the documented global options of
the tool that Glafira knows: an option with a value, such as `aws --region eu-west-1 eks get-token` or
`--region=eu-west-1`, or a flag, such as `aws --no-verify-ssl eks get-token`. Glafira refuses any other option there
and names it: put it after the arguments or remove it. `kubectl oidc-login` takes no options before `oidc-login`:
kubectl starts a plugin only when nothing precedes its name. The environment variables of the plugin cannot change
which code the command runs: `PATH` (Glafira sets it itself), `LD_*`, `DYLD_*`, `PYTHON*`, `NODE_OPTIONS` and similar
variables are not accepted. Variables that configure the tool itself, such as `AWS_PROFILE`, are fine, and so are
variables that point the tool to another config file, such as `AWS_CONFIG_FILE` or `KUBECONFIG`: such a file can
contain a command too, so check it before you confirm the line.

The form shows the exact launch line: the path, the arguments and the environment variables. A value is hidden
(`••••`) when the name of the variable or option contains TOKEN, SECRET, PASS, KEY, CREDENTIAL, BEARER or COOKIE, for
example `--client-secret x`, `--oidc-client-secret=x` or `AWS_SECRET_ACCESS_KEY`. The argument and variable fields
hide such values too and stay read-only until you click [[ui:connections.form.exec.showSecrets]]. Glafira runs the
line only after you
tick [[ui:connections.form.exec.confirm]]. The confirmation is bound to the exact line: any change to the command, the
arguments or the variables needs a new confirmation. In the form, a switch of the sign-in method also drops a tick you
have set; a line that was saved confirmed and is unchanged stays confirmed. A connection that arrives from an export
file as a new one is unconfirmed.

Secret values are kept in the connections file, not in the secret store. They are not written to the export file that
you create with [[ui:sidebar.exportAll]] or [[ui:sidebar.folder.export]]: after importing such a connection on another
computer, fill them in again. Importing a file over a connection you already have keeps the secret values you already
have. If the plugin supports it, keep the secret out of the command line instead, for example in the plugin's own
config or credential helper.

Plugins that open the browser (`kubectl oidc-login`, `kubelogin`) work: Glafira waits up to 5 minutes, and the
connection tab can cancel the wait. Plugins that need a terminal (`interactiveMode: Always`, device code sign-in) are
not supported. The credentials are kept in memory until they expire; then the plugin runs again
([[ui:cluster.session.loginExec]]). Your organization may forbid exec plugins.

## Import from kubeconfig {#kubeconfig}

[[ui:sidebar.importKubeconfig]] reads `~/.kube/config` (or the first file from `KUBECONFIG`) and lists its contexts.
Choose the contexts to import: tokens and keys go to the secret store, CA certificates and the project of the
context are copied into the connection. Glafira does not keep a link to the file. For an exec plugin the dialog shows
the launch line with the full path to the command. The dialog hides the secret values of the line (`••••`), but the
import copies the real ones into the connection. Tick [[ui:kubeconfig.allowExec]] under the line to import it
confirmed: the tick confirms the line exactly as shown. If the launch line in the kubeconfig file changes after the
preview, the changed line is imported unconfirmed, and the connection form asks you to confirm it. Commands outside the
allowed list, forbidden environment variables and `auth-provider` contexts are not supported: the preview explains
why.

## Platform {#platform}

[[ui:cluster.connection.platform]] is detected on connection: a cluster with the `config.openshift.io` API group is
OpenShift (OCP or OKD), any other is Kubernetes, with the distribution if Glafira recognizes it (EKS, GKE, AKS,
Rancher, Deckhouse, k3s, RKE2). Platform V DropApp works as Kubernetes for now. What Glafira shows depends on the API
groups, not on the platform: Route, projects and Gateway API appear wherever the cluster has them.

## Certificates and proxy {#tls}

[[ui:connections.form.ca]]: the system certificate store, or only the listed certificates (as `certificate-authority`
in kubeconfig). The mode without certificate verification is insecure; your organization may forbid it.
[[ui:connections.form.proxy]]: the system proxy (`HTTPS_PROXY` and `NO_PROXY`), no proxy, or an explicit address.

## Rights {#rights}

Glafira works with your rights only. On connection it checks what you may read at the cluster level (projects, nodes,
operators). If the cluster does not let you list projects, enter them in
[[ui:connections.form.manualNamespaces]].

## Cluster overview {#overview}

Under the connection details, the connection tab shows the [[ui:cluster.overview.title]]: whether the API server is
ready (the `/readyz` checks, which any user may read, and the checks that failed; if `/readyz` does not answer, a dash
with the reason), ready nodes by role, the number of projects, the Ingress classes and, with Gateway API, the Gateway
classes. On OpenShift it adds the version (during an update — the last completed one, and the update in progress
separately), the update channel and the available updates, the infrastructure, the web console (a link only for an
http or https address), the cluster operators that are not fine ([[ui:cluster.overview.operatorsAll]] lists every
one) and the machine config pools. A part you may not read shows a dash (—) with the reason; the rest is shown. On
OpenShift the tab also shows until when the sign-in token is valid and warns an hour before it expires; the warning
appears on time even if the tab stays open, and once the token has expired, [[ui:cluster.connection.reconnect]] is
next to it.

## Nodes {#nodes}

With the right to list nodes, the connection tab and the tree have [[ui:cluster.nodes.open]]: the list of nodes with
their readiness, problems (`MemoryPressure`, `DiskPressure`…), roles and kubelet version. Click a node to see its
conditions, taints, capacity and allocatable resources, addresses and labels.

## Environments and read-only {#environments}

[[ui:connections.form.environment]] sets the color of the connection. A new `prod` connection is
[[ui:common.readOnly]]: Glafira will not change anything in the cluster through it.

## Folders, export and import {#folders}

Connections can be arranged in nested folders: [[ui:sidebar.newFolder]], drag and drop. [[ui:sidebar.exportAll]]
saves the tree to an export file without tokens, keys and the secret values of exec plugins; [[ui:sidebar.import]]
adds connections from such a file with a preview. A connection that is new on this computer arrives without secrets,
and an exec plugin in it arrives unconfirmed. When the file updates a connection you already have, your tokens, keys
and secret values stay.
