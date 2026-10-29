# Resources and changes

Glafira shows how much CPU and memory containers actually use, compares it with their `requests` and `limits` and
recommends new values. Changes — container resources, replicas, restart, HPA bounds and pod deletion — are made with
forms that the API server checks before anything is applied.

## Metrics source {#source}

The usage history comes from Prometheus. The [[ui:connections.form.metricsMode.auto]] mode finds Thanos Querier in
`openshift-monitoring` on OpenShift and Prometheus through the API server proxy on other clusters (kube-prometheus,
Prometheus Operator, Rancher Monitoring). If none fits, Glafira shows the current values from `metrics.k8s.io` — no
history and low confidence. Your own Thanos or Prometheus is set in the connection form (see
[Connections](connections.md#metrics)). Which source is chosen and why the others did not fit is shown in the connection
test and above the resource tables. The choice is kept in the connection for 5 minutes; [[ui:common.refresh]] and
[[ui:common.retry]] on the Resources tabs choose again. With `metrics.k8s.io` only, the tabs say
[[ui:resources.currentOnly]], and the window cannot be chosen.

The rights are yours: Thanos on OpenShift needs the `cluster-monitoring-view` role or similar, the Prometheus proxy
needs `get services/proxy` in its project. Usually only cluster administrators have it.

## Workload resources {#workload}

The [[ui:objects.tabs.resources]] sub-tab of a workload counts statistics over the [[ui:resources.window]]: 1, 7, 14 or
30 days. Percentiles are taken per pod, then the worst pod: replicas can have different load. The table per container
shows "now → recommendation" for CPU and memory, CPU p95 and maximum, memory maximum, throttling, OOM and restarts.
Values a pod got from a `LimitRange` default count as current too. A container with only limits gets a request equal to
its limit from the API server: that is its current request, not a `LimitRange` default. Below the table are the total
for the whole workload with replicas, constraint violations (quota, the largest node) and CPU and memory charts with
lines of the current request and limit and marks of restarts and OOM.

[[ui:resources.applyRecommendation]] opens the container resources form with the recommended values: you can edit them
before the check.

## How the recommendation is made {#recommendation}

- CPU request — p95 × 1.15, at least 10m; a CPU limit is added only if there already is one or a `LimitRange` requires
  it. Throttling p95 above 10 % means the usage hit the limit: the limit is at least the current one × 1.5.
- Memory request — p99 × 1.15, at least 32Mi; memory limit — at least the maximum × 1.3. OOM in the window — the limit is
  at least the current one × 1.5.
- Values are pulled to the `LimitRange` bounds. The quota room counts all replicas and the rollout pods: the increase
  per pod times the replicas plus the new value for each rollout pod, so even a decrease needs room while the old pods
  are still running. If an HPA scales the workload by CPU or memory, Glafira says which target keeps the old behaviour.
- Confidence is [[ui:resources.confidenceLevel.high]] with a window and data of 7 days or more and coverage of 80 % or
  more, [[ui:resources.confidenceLevel.medium]] with data of a day or more and coverage of 50 % or more, otherwise
  [[ui:resources.confidenceLevel.low]]. With low confidence the form warns, but you can apply.
- For containers with `JAVA_OPTS`, `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS` or `GOMEMLIMIT` Glafira reminds you to check
  `-Xmx`, `MaxRAMPercentage` or `GOMEMLIMIT` together with the memory limit.

## Project resources {#project}

The [[ui:objects.tabs.resources]] sub-tab of a project collects all workloads of the project: totals of requests and
limits of running pods, usage (CPU p95 and memory maximum), reserve, risks — throttling, OOM, memory near the limit —
and excess requests, and above the table the quota usage. A workload without running pods has unknown usage (—), not 0.
With `metrics.k8s.io` only, the usage is one current sample: risks are shown, excess is not — a quiet minute is no
reason to cut requests. The collection runs in the background for up to 45 seconds and is kept for 5 minutes; it is
forgotten when you disconnect, sign in with another token, edit or delete the connection, and a collection with a
network error (timeout, broken connection, server error) is not kept. [[ui:common.refresh]] collects again and chooses
the metrics source again. Sorting is [[ui:resources.sortRisk]] or [[ui:resources.sortReserve]];
[[ui:resources.onlyActionable]] keeps workloads with a risk or excess. The table shows up to 100 rows — narrow the list
by name for more.

## Pod resources {#pod}

The [[ui:objects.tabs.resources]] sub-tab of a pod shows the current CPU and memory usage per container.

## Changes {#changes}

The workload overview has [[ui:change.action.setResources]], [[ui:change.action.scale]] (Deployment, StatefulSet,
DeploymentConfig), [[ui:change.action.rolloutRestart]] and, if the workload has an HPA,
[[ui:change.action.setHpaBounds]]; the pod overview has [[ui:change.action.deletePod]]. Without rights in the project
a button is disabled and its hint names the right; on a read-only connection all are disabled.

1. The form opens over the screen. In the resources form an empty field means "keep" (its hint shows the current value),
   "✕" removes the value. [[ui:change.preview]] is unavailable while a number is empty, fractional or out of bounds:
   replicas 0–1000, HPA bounds 1–1000 with the minimum not above the maximum.
2. [[ui:change.preview]] shows "before → after", warnings and the result of a check on the API server that does not
   change the cluster (server-side dry-run): a webhook or policy refusal is visible before you confirm. Warnings: an HPA
   will overwrite the replicas; the quota has no room, so the pods of the new revision would not be created and the
   rollout would stall; a pod without a controller; the HPA, `LimitRange` or quotas of the project could not be read, so
   the change was not checked against them. A value outside a `LimitRange` bound is an error: the API server checks only
   pods against `LimitRange`, not the workload template, and would reject the pods of the new revision later. The form
   then shows the error without "before → after", dry-run and [[ui:change.apply]].
3. [[ui:change.apply]]. On a prod connection type its name, for a pod without a controller — the pod name.
4. If the object changed while the form was open (its generation or, if it has none, its spec), Glafira changes nothing:
   instead of [[ui:change.apply]] the form offers [[ui:change.refreshRetry]].

Glafira writes changes with the `glafira-kubernetes` field manager (visible in `managedFields`), and the app log gets
the connection, the kind of change, the object and the result, without values.

## LLM proposals {#proposals}

An LLM assistant does not change the cluster, but it can propose a change: resources, replicas, restart or HPA bounds.
The proposal appears in the [[ui:sidebar.findings]] tab and on the object screen. [[ui:health.proposals.openForm]]
opens the form with the fields filled in and a line about the proposal; then it is an ordinary change, and the model
learns whether it was applied and whether with edits. More in [Cluster check and notes](health-check.md#proposals).
