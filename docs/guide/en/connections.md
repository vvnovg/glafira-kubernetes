# Connections

A connection is the API server address, the sign-in method and the TLS settings of one cluster. Passwords, tokens
and keys are kept in the system secret store (Keychain, Credential Manager, Secret Service), never in files.

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

Glafira runs the plugin by its full path, without a shell. An app started from the Dock does not see the `PATH` of
your terminal, so enter the command name and click [[ui:connections.form.exec.find]]: Glafira looks in the `PATH` of
your login shell and in the standard folders (`/opt/homebrew/bin`, `/usr/local/bin`, `~/.local/bin`).

The form shows the exact launch line: the path, the arguments and the environment variables; values of variables that
look like secrets are hidden. Glafira runs the line only after you tick [[ui:connections.form.exec.confirm]]. Any
change to the line, and a connection imported from a file, needs a new confirmation.

Plugins that open the browser (`kubectl oidc-login`, `kubelogin`) work: Glafira waits up to 5 minutes, and the
connection tab can cancel the wait. Plugins that need a terminal (`interactiveMode: Always`, device code sign-in) are
not supported. The credentials are kept in memory until they expire; then the plugin runs again
([[ui:cluster.session.loginExec]]). Your organization may forbid exec plugins.

## Import from kubeconfig {#kubeconfig}

[[ui:sidebar.importKubeconfig]] reads `~/.kube/config` (or the first file from `KUBECONFIG`) and lists its contexts.
Choose the contexts to import: tokens and keys go to the secret store, CA certificates and the project of the
context are copied into the connection. Glafira does not keep a link to the file. For an exec plugin the dialog shows
the launch line with the full path to the command; tick the permission under the line to import it confirmed.
Commands outside the allowed list and `auth-provider` contexts are not supported: the preview explains why.

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

## Environments and read-only {#environments}

[[ui:connections.form.environment]] sets the color of the connection. A new `prod` connection is
[[ui:common.readOnly]]: Glafira will not change anything in the cluster through it.

## Folders, export and import {#folders}

Connections can be arranged in nested folders: [[ui:sidebar.newFolder]], drag and drop. [[ui:sidebar.exportAll]]
saves the tree to a file without tokens and keys; [[ui:sidebar.import]] adds connections from such a file with a
preview.
