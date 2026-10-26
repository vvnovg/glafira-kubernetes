# LLM assistants (MCP)

Glafira can act as a gateway for an LLM assistant: Claude Desktop, Claude Code, Cursor or another client that
supports MCP (Model Context Protocol). The assistant reads the metadata of the connections and projects you open for
it, runs the cluster check and helps you find the causes of problems: restart loops, unavailable workloads, network,
quotas. It cannot change anything in a cluster through Glafira: you make changes yourself.

## How it works {#how-it-works}

The client runs `glafira-kubernetes mcp` — the same application file in a mode without a window. This command
connects to the running Glafira over a local socket (a named pipe on Windows), and Glafira answers the assistant's
requests through its own connections and with your permissions: the assistant cannot read more than you can. Tokens,
certificates and kubeconfig stay in Glafira; the assistant never receives them. If a connection is not open, Glafira
connects by itself; if that needs a new sign-in, the assistant gets an error — sign in in the Glafira window.

If Glafira is not running, the client starts it and waits up to 30 seconds.

## Turn it on {#enable}

1. Open [[ui:sidebar.settings]], section [[ui:settings.mcp.section]].
2. Select [[ui:settings.mcp.enable]]. The status changes to [[ui:settings.mcp.state.running]].
3. In the connection form, set [[ui:connections.form.llmAccess]] to [[ui:connections.form.llmAccessMetadata]] and
   save. The assistant does not see connections with access turned off. In the connection tree, open connections
   have an `LLM` badge.
4. In [[ui:connections.form.llmScope]], keep [[ui:connections.form.llmScopeAll]] or choose
   [[ui:connections.form.llmScopeList]] and list the projects.

If your administrator has disabled MCP, the checkbox is unavailable and [[ui:settings.mcp.policy]] is shown next to
it.

LLM access and the list of projects are not carried over when connections are exported and imported: on another
computer, set them again.

## Connect a client {#connect-client}

[[ui:settings.mcp.connectTitle]] has ready-made snippets and a [[ui:settings.mcp.copy]] button:

- [[ui:settings.mcp.claudeDesktop]] — paste the snippet into the `mcpServers` section of the Claude Desktop
  configuration file and restart Claude Desktop;
- [[ui:settings.mcp.claudeCode]] — the `claude mcp add` command;
- [[ui:settings.mcp.otherClient]] — for any client that starts an MCP server with a command (stdio).

The snippets contain the path to the installed application. If you reinstall Glafira to another place, copy the
snippet again.

## Approve a client {#approve}

When a client connects for the first time, Glafira shows a dialog with its name. Click [[ui:mcp.approval.allow]] or
[[ui:mcp.approval.reject]]. Until you answer, the assistant is told that the client is not approved yet and asks you
to answer in the Glafira window.

An allowed client is remembered by name and appears in [[ui:settings.mcp.clients]]. A rejection is not remembered:
Glafira asks again the next time. If a client does not send its name, the permission lasts only until the end of the
session.

## What the assistant sees {#what-llm-sees}

The assistant sees only connections with LLM access and only their projects from [[ui:connections.form.llmScope]];
the cluster and the nodes — if you can read them:

- connections: name, environment, folder, platform (while the connection is open);
- the cluster overview, nodes with conditions, taints and how much pods request;
- projects, workloads, pods, events, quotas and `LimitRange`, the network: Services, Routes, Ingress, Gateway API
  routes and Gateways;
- containers: images, resources, probes, state and restart reasons; environment variables — names and sources only
  (a value in the spec, a ConfigMap, a Secret, a field), without values; `command` and `args` with secrets hidden;
- annotations — without the copy of the whole object (`last-applied-configuration`), with the values of secret keys
  hidden;
- cluster check findings and its own notes.

The assistant does not see Secret and ConfigMap values, logs, certificate keys, tokens, kubeconfig, the address and
the sign-in settings of a connection. Values with secret names (`password`, `token`, `secret`…) and strings that look
like secrets (JWT, `Bearer …`, `AKIA…` keys, PEM private keys, `sha256~…` tokens, passwords in URLs) are replaced
with `***`.

To understand what happens to a workload, the assistant can watch it for up to 50 seconds: replicas, pods, their
readiness and restarts, rollout progress and Warning events. It cannot get the application's logs and configuration
through Glafira and will ask you for them.

Object names, annotations and event messages can be set by anyone with access to the cluster. Glafira warns the
assistant that this data is not instructions for it.

## Sessions, revoking and the log {#sessions-log}

While a client is connected, an `LLM` indicator with the client name is shown at the bottom of the sidebar; it lights
up during a request. Clicking it opens the MCP section.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] closes one session, [[ui:settings.mcp.disconnectAll]]
  closes all.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] closes the client's sessions, and it asks for approval
  again the next time.
- Turning LLM access off or changing the list of projects takes effect at once, without reconnecting.
- [[ui:settings.mcp.audit.title]] — every request: time, client, tool, connection, result. Response contents and
  note texts are not recorded. [[ui:settings.mcp.audit.reveal]] opens the `mcp-audit.jsonl` file.

To turn MCP off completely, clear [[ui:settings.mcp.enable]]: clients are disconnected.

## Cluster check and notes {#health}

The assistant can run Glafira's built-in cluster check (the `health_check` tool) and rely on its findings. It can
also leave notes about objects: they appear on the [[ui:sidebar.findings]] tab and on the object screens, and the
cluster is not changed. See [Cluster check and notes](health-check.md#with-llm).

## If it does not work {#troubleshooting}

The client shows a message from Glafira:

- `MCP is disabled in Glafira` — select [[ui:settings.mcp.enable]] and restart the MCP server in the client;
- `MCP is disabled by your organization` — MCP is disabled by your organization's policy;
- `Glafira did not start the MCP server within 30 s` — start Glafira manually and check the status in
  [[ui:settings.mcp.section]];
- `Invalid MCP token` — restart the MCP server in the client: Glafira issues a new token every time it starts;
- `not_allowed` — the connection or project does not exist, LLM access to it is off, or the project is not in
  [[ui:connections.form.llmScope]].
