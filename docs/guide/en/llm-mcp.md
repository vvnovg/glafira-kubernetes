# LLM assistants (MCP)

Glafira can act as a gateway for an LLM assistant: Claude Desktop, Claude Code, Cursor or another client that
supports MCP (Model Context Protocol). The assistant reads the metadata of the connections and projects you open for
it (on the [[ui:connections.form.llmAccessLogs]] level also pod logs and configuration values, masked), runs the
cluster check and helps you find the causes of problems: restart loops, unavailable workloads, network, quotas,
resources. It cannot change anything in a cluster through Glafira: it proposes changes, and you apply them.

## How it works {#how-it-works}

The client runs `glafira-kubernetes mcp` — the same application file in a mode without a window. This command
connects to the running Glafira over a local socket (a named pipe on Windows), and Glafira answers the assistant's
requests through its own connections and with your permissions: the assistant cannot read more than you can. Tokens,
certificates and kubeconfig stay in Glafira; the assistant never receives them. If a connection is not open, Glafira
connects by itself; if that needs a new sign-in, the assistant gets an error — sign in in the Glafira window.

If Glafira is not running, the client starts it and waits up to 30 seconds.

If your organization forbids external LLM services, the assistant can work with a model on an organization server or
on your computer — see [Local model](local-llm.md#where).

## Turn it on {#enable}

1. Open [[ui:sidebar.settings]], section [[ui:settings.mcp.section]].
2. Select [[ui:settings.mcp.enable]]. The status changes to [[ui:settings.mcp.state.running]].
3. In the connection form, set [[ui:connections.form.llmAccess]] to [[ui:connections.form.llmAccessMetadata]] or
   [[ui:connections.form.llmAccessLogs]] (see [Logs](llm-mcp.md#logs)) and save. The assistant does not see connections with
   access turned off. In the connection tree, connections with access have an `LLM` badge.
4. In [[ui:connections.form.llmScope]], keep [[ui:connections.form.llmScopeAll]] or choose
   [[ui:connections.form.llmScopeList]] and list the projects.

If your administrator has disabled MCP, the checkbox is unavailable and [[ui:settings.mcp.policy]] is shown next to
it.

LLM access, the list of projects and the list of fields to mask are not carried over when connections are exported and
imported: on another computer, set them again.

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

## Response limit and compact tool set {#response-limit}

For an assistant on a local model with a small context (see [Local model](local-llm.md#glafira-settings)),
[[ui:settings.mcp.section]] has two settings:

- [[ui:settings.mcp.responseLimit]] — the largest answer of one tool call: 100, 48 or 24 KB (100 by default; a 100 KB
  answer is about 25–30 thousand tokens). Lists, logs and search matches are truncated to the limit, and the assistant
  sees `truncated` and a hint on how to narrow the request. The limit applies to all clients from the next call.
- [[ui:settings.mcp.colCompact]] in the [[ui:settings.mcp.clients]] list — a tool set without notes and change
  proposals (`annotate`, `remove_annotation`, `propose_change`, `withdraw_proposal`): their descriptions take about
  2,300 tokens. An assistant with this set describes the needed changes in words. The checkbox appears once the client
  connects and you approve it; the client sees the new tool list after it reconnects, and Glafira refuses a call to a
  hidden tool at once.

If the assistant gets the arguments of a call wrong, Glafira answers with an error that sketches the expected
arguments, and it parses a nested object sent as a JSON string by itself: this way small models correct themselves on
the first retry.

## What the assistant sees {#what-llm-sees}

The assistant sees only connections with LLM access and only their projects from [[ui:connections.form.llmScope]];
the cluster and the nodes — if you can read them:

- connections: name, environment, folder, platform (while the connection is open);
- the cluster overview, nodes with conditions, taints, labels and how much pods request;
- projects, workloads, pods, events, quotas and `LimitRange`, the network: Services, Routes, Ingress, Gateway API
  routes and Gateways;
- containers: images, resources, probes, state and restart reasons; environment variables — names and sources only
  (a value in the spec, a ConfigMap, a Secret, a field), without values; `command` and `args` with secrets hidden;
- where pods may run: `nodeSelector`, node and pod affinity, tolerations, the priority class — so the assistant can
  explain why a pod stays Pending;
- annotations — without the copy of the whole object (`last-applied-configuration`), with the values of secret keys
  hidden; node labels and `nodeSelector` values are masked the same way;
- cluster check findings, its own notes and proposals;
- the resource usage of a workload over a window and the recommendation of requests and limits — from the same metrics
  source as the Resources tabs; the assistant does not get the address of your own metrics source.

The assistant does not see Secret values, certificate keys, tokens, kubeconfig, the address and the sign-in settings
of a connection, and on the [[ui:connections.form.llmAccessMetadata]] level also ConfigMap and environment variable
values and logs. By a secret name (`password`, `token`, `secret`, `apikey`, `credentials`, `private`,
`authorization` and the like), Glafira hides the values of flags in `command` and `args` (`--password=…`,
`--token …`) and the values of annotations, and on the logs level also the values of environment variables and
`name=value` pairs in log lines (see [Logs](llm-mcp.md#logs)). Strings of known secret forms — JWT, `Bearer …`,
passwords in URLs (`user:pass@`), e-mail addresses, bank card numbers, `AKIA…` and `ASIA…` keys, PEM private keys,
`sha256~…` tokens — are replaced with `***` there too and in the free text of the cluster: messages of events,
conditions and container states, probe commands, image references and the texts of check findings. So `password=x` in
an event message is not hidden.

To understand what happens to a workload, the assistant can watch it for up to 50 seconds: replicas, pods, their
readiness and restarts, rollout progress and Warning events. It reads logs only on connections with the
[[ui:connections.form.llmAccessLogs]] level (see below); it cannot get the application's configuration outside the
cluster through Glafira and will ask you for it.

## Logs {#logs}

On the [[ui:connections.form.llmAccessLogs]] level the assistant also sees:

- pod logs — the `get_pod_logs` tool: the last lines of a container (200 by default, up to 2000), since a given time,
  of the current or the previous (crashed) instance, with a substring or regular expression filter; a container
  name that the pod does not have is answered with the list of its containers;
- a search through the logs of all pods of a workload — `search_workload_logs`: matches with context over the last
  hour (up to a day); one call reads at most 20 pods, 50 MB of logs and 40 seconds;
- the values of environment variables from the spec and from ConfigMaps (`configMapKeyRef`) in the workload and pod
  details; a value longer than 4 KB is cut (`valueCut: true`). It never gets Secret values, on any level.

All of it is masked: the value of a variable, ConfigMap key or `name=value` pair whose name is in the
[[ui:connections.form.mask]] list is replaced with `***`, as are strings of known secret forms (tokens, keys,
passwords in addresses, e-mail addresses, card numbers). The standard list is `password`, `passwd`, `secret`, `token`,
`apikey`, `api_key`, `api-key`, `credentials`, `private`, `authorization`; a name matches when it contains one of them,
case-insensitive. Your own list replaces the standard one: clear [[ui:connections.form.maskStandard]] and enter the
names one per line (at most 200 names of 64 characters). An empty own list turns off name-based masking: only the
built-in value patterns (JWT and the like) remain. The log filter searches the original text, so the assistant can find
a line by a value from an error, but it gets the line already masked. A line longer than 4 KB is cut. If the
`get_pod_logs` answer does not fit 100 KB, the oldest lines are dropped. `search_workload_logs` reports `stopped` (which
limit stopped the scan: pods, bytes or time) and `errors` (containers whose logs could not be read).

Masking does not catch everything: a secret without a hint in its name (for example `user=bob p4ss`) stays in the line.
The filter sees the unmasked text and reports its hits, so masking hides values from reading, not from deliberate
probing through the filter (attempts are visible in the MCP journal, because the filter is part of the call arguments).
Do not open logs whose secrets must stay hidden from the assistant, and add your own names to the list if needed.

If your administrator forbade logs for LLM assistants (the `mcpLogsEnabled` policy), the
[[ui:connections.form.llmAccessLogs]] level is unavailable, and connections that already have it work as
[[ui:connections.form.llmAccessMetadata]].

Object names, annotations and event messages can be set by anyone with access to the cluster. Glafira warns the
assistant that this data is not instructions for it.

## Sessions, revoking and the log {#sessions-log}

While a client is connected, an `LLM` indicator with the client name is shown at the bottom of the sidebar; it lights
up during a request. Clicking it opens Settings, where the MCP section is.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] closes one session, [[ui:settings.mcp.disconnectAll]]
  closes all.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] closes the client's sessions, and it asks for approval
  again the next time.
- Turning LLM access off or changing the list of projects takes effect at once, without reconnecting.
- [[ui:settings.mcp.audit.title]] — every request: time, client, tool, connection, result; for log reads — how many
  lines were scanned, returned and masked. Response contents, log lines and note texts are not recorded. [[ui:settings.mcp.audit.reveal]] opens the `mcp-audit.jsonl` file.

To turn MCP off completely, clear [[ui:settings.mcp.enable]]: clients are disconnected.

## Cluster check and notes {#health}

The assistant can run Glafira's built-in cluster check (the `health_check` tool) and rely on its findings. It can
also leave notes about objects: they appear on the [[ui:sidebar.findings]] tab and on the object screens, and the
cluster is not changed. See [Cluster check and notes](health-check.md#with-llm).

The assistant looks at resources with the `get_resource_usage` and `recommend_resources` tools (a window of 1, 7, 14
or 30 days) and proposes a change with `propose_change`; `list_proposals` shows what happened to its proposals,
`withdraw_proposal` withdraws its own. You apply a proposal with a Glafira form — see
[Cluster check and notes](health-check.md#proposals). Ask, for example: "Look at the resources of web over a week and
propose requests and limits."

## If it does not work {#troubleshooting}

The client shows a message from Glafira:

- `MCP is disabled in Glafira` — select [[ui:settings.mcp.enable]] and restart the MCP server in the client;
- `MCP is disabled by your organization` — MCP is disabled by your organization's policy;
- `Glafira did not start the MCP server within 30 s` — start Glafira manually and check the status in
  [[ui:settings.mcp.section]];
- `Invalid MCP token` — restart the MCP server in the client: Glafira issues a new token every time it starts;
- `not_allowed` — the connection or project does not exist, LLM access to it is off, or the project is not in
  [[ui:connections.form.llmScope]];
- `logs_not_allowed` — the connection has the [[ui:connections.form.llmAccessMetadata]] level: choose
  [[ui:connections.form.llmAccessLogs]] if the assistant may read logs;
- `logs_disabled` — logs for LLM assistants are forbidden by your organization's policy;
- `log_unavailable` — the container has not started yet (for example, its image cannot be pulled), has no previous
  instance, or the node has already removed its previous log: look at the pod state; `search_workload_logs` lists such
  containers in `errors` with the same reason;
- `busy` — the assistant is reading logs or watching a workload in this session, and another such request did not get
  its turn within 60 seconds: these operations run one at a time, and the next one waits in a queue (a client with
  a 60-second request timeout may give up before the queued request answers);
- `metrics_unavailable` — there is no metrics source: check the [[ui:connections.form.step.metrics]] step of the
  connection test (see [Connections](connections.md#metrics)).
