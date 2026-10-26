# Pods and logs

## Pod list {#pods}

The pods of a project or a workload show the status as `kubectl get pods` does (`Running`, `CrashLoopBackOff`,
`Init:1/2`, `Completed`…), the ready containers, the restarts and when the last one was, the node, the age, the QoS
class and the owner. A problem pod — the image cannot be pulled, a container was killed for memory (`OOMKilled`),
restarts in a loop, the pod waits to be scheduled (`Pending`) or has failed — is highlighted; the problems-only
filter leaves just them.

## Pod tab {#pod}

Click a pod to open its tab. [[ui:objects.tabs.overview]] shows the phase, the conditions, the node, the IP, the QoS,
the owner (a click opens the workload) and every container, init containers too: the image and the digest that is
running, the state and the last state with the reason, the exit code and the time, the restarts, the probes, the
requests and limits and the environment variables — names and sources only (a value in the spec, a ConfigMap or
Secret key, a field), never the values. [[ui:objects.tabs.events]] and YAML are there too.

## Logs {#logs}

[[ui:logs.title]] shows the log of a container: choose the [[ui:logs.container]], the [[ui:logs.previous]], the
[[ui:logs.tail]] (500 by default) or where to start from ([[ui:logs.since]]), and [[ui:logs.follow]].
[[ui:logs.timestamps]] adds the time of each line. The [[ui:logs.filter]] — a substring or a regular expression,
[[ui:logs.caseSensitive]] if needed — runs in Glafira before the lines reach the window, and the counter shows how many
lines were shown and how many were read. The window keeps up to 100,000 lines; a line longer than 64 KB is cut. When
the connection is lost while following, Glafira reconnects from the last line it has, without repeats, and reports an
error after a minute without success. [[ui:logs.save]] reads the log again with the same settings (without
following) and saves the whole lines to a file.

## When the session expires {#session}

If the token expires while you read the cluster, the tabs show the error with [[ui:objects.signInAgain]]: it opens the
connection tab, where you sign in again. After that the open tabs of the connection read their data again by
themselves, and the logs and the lists that update themselves start over.
