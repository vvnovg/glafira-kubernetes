# Cluster check and notes

Glafira checks a cluster with a set of built-in rules and shows the problems it finds next to the objects: in the
connection tree, in the workload and pod tables, on the cluster and project overviews, on the workload and pod
screens. An LLM assistant can add its own notes to the findings. The check only reads the cluster and changes nothing
in it.

## Run a check {#run}

Open [[ui:sidebar.cluster]] or [[ui:sidebar.findings]] of a connection and click [[ui:health.check]]. Glafira reads
the API server, the nodes and up to 50 projects you can access (up to 5000 pods), with at most 4 requests at a time
and for no longer than 45 seconds. The line under the summary shows how many projects, pods and nodes were checked.
Whatever could not be read (missing permissions, more than 50 projects, time ran out) marks the result as
[[ui:health.partial]], and the [[ui:health.ruleNames.check.partial]] finding tells what was missing.

Glafira does not poll the cluster by itself: a check runs only when you click the button or an LLM assistant asks for
it. The result is kept until the next check.

## What is checked {#rules}

- Cluster and nodes: the API server is not ready (`/readyz`), on OpenShift — ClusterOperators and MachineConfigPools
  are not healthy, the update is failing; a node is not ready, under pressure (`MemoryPressure`, `DiskPressure`,
  `PIDPressure`), almost fully requested by pods (over 90%) or cordoned.
- Pods: restart loops (`CrashLoopBackOff` or exiting with an error between restarts), an image cannot be pulled,
  `Pending` for more than 5 minutes, `OOMKilled` in the last day, frequent restarts. The pods of one workload make one
  finding with a list of pods.
- Workloads and containers: a workload unavailable for more than 5 minutes, a stuck rollout, a single replica;
  containers without requests, a memory limit or a readiness probe, images with the `latest` tag.
- Network: a Service has no ready endpoints; a Route is not admitted or its certificate expires; an Ingress has no
  class, backend or TLS Secret; a Gateway API route is not accepted or its references are unresolved; a Gateway is
  not programmed.
- Quotas, storage and scaling: a quota used at 90% or more, a PVC lost or waiting for a volume for more than 5
  minutes, an HPA at its maximum or unable to scale.
- Resources over the last day from the metrics history (see [Resources and changes](resources-changes.md#source)): CPU
  throttling above 25 %, memory near the limit (90 % or more), requests far above the usage. The pods of the Jobs of a
  CronJob count for the CronJob. Without a history source, or when the source cannot be chosen or answers "not found",
  these rules do not run and the check is marked partial.

## Severity and environment {#severity}

Every finding has a severity: [[ui:health.severity.critical]], [[ui:health.severity.warning]] or
[[ui:health.severity.info]]. It depends on the connection environment. For example, a restart loop is critical on
prod and a warning on the other environments; a single replica is a warning on prod, info on stage and not shown on
dev, test and local. The environment is set in the connection form; if it changed after the check, the summary line
tells which environment the severity was computed for.

## The Findings tab {#tab}

On the [[ui:sidebar.findings]] tab, findings are grouped by rule; each has an explanation and what to do. Filters:
[[ui:health.filterSeverity]], [[ui:health.filterSource]], [[ui:health.filterKind]] and a search by object name or
project. The object link opens its tab: the workload, the pod, the nodes or the project.

A badge with a number — on the [[ui:sidebar.findings]] item and on projects in the connection tree, in the workload
and pod tables — shows how many findings, open LLM notes and proposals with severity [[ui:health.severity.critical]] or
[[ui:health.severity.warning]] concern the object. Point at the badge to see their titles. A pod screen also shows
the findings of its workload that mention this pod.

## Hide findings {#hide}

If a finding is expected for a particular object — for example, a single replica of a utility Deployment — click
[[ui:health.hide]]. A hidden finding is not counted in badges and does not come back after later checks. To see
hidden findings, select “Show hidden”; the [[ui:health.restore]] button is there too.

## LLM notes {#notes}

An LLM assistant connected over MCP (see [LLM assistants (MCP)](llm-mcp.md#how-it-works)) can leave a note about the
cluster, a node, a project or an object in a project; Glafira checks that the object exists. Notes appear in
[[ui:health.notes]] and on the object screens, signed with the client name. The text is shown as is, without
formatting or links.

- [[ui:health.resolve]] and [[ui:health.dismiss]] close a note; closed notes are kept in a collapsed section.
- [[ui:common.delete]] deletes a note, [[ui:health.deleteAll]] deletes all notes of the connection.
- A note is deleted automatically 7 days after it was created. A connection can have at most 200 open notes.

## LLM proposals {#proposals}

The assistant can propose a change: container resources, replicas, restart or HPA bounds (not pod deletion). Glafira
checks a proposal by reading the cluster — the object and the containers exist, the values parse, `limits` are not below
`requests`, the values are within the `LimitRange` bounds; otherwise the proposal is refused. Scaling a workload that an
HPA scales is refused too, with advice to propose HPA bounds. The quota room, scaling to 0 and an HPA, `LimitRange` or
quota list that could not be read come back to the assistant as warnings, and the proposal is created: the form checks
everything again when you apply it. Proposals are shown in [[ui:health.proposals.title]] and on the object screens.

- [[ui:health.proposals.openForm]] opens the change form with the fields filled in (see
  [Resources and changes](resources-changes.md#proposals)); after it is applied the proposal is marked "applied" or
  "applied with edits", and the assistant sees it. On a read-only connection the button is disabled.
- [[ui:health.proposals.reject]] — with an optional reason the assistant sees; [[ui:health.reopen]] brings the
  proposal back. The assistant can withdraw its own proposal.
- A proposal of any status is deleted 7 days after it was created; a connection has at most 50 open ones.

## With an LLM assistant {#with-llm}

The assistant runs the same check with the `health_check` tool — over the projects opened to it in the connection
form ([[ui:connections.form.llmScope]]) or over some of them — and sees the same findings, hidden ones marked as
such. It does not see findings outside its projects. If the last check is less than 5 minutes old and covers the
projects it needs, the assistant gets its result without a new request to the cluster. The assistant's check becomes
the last check in the interface too: the summary line shows the client and the projects it covered. Ask, for example:
“Check the cluster and leave notes on the most important problems.”
