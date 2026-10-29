# Pods and logs

## Pod list {#pods}

The pods of a project or a workload show the status by the rules of `kubectl get pods` (`Running`,
`CrashLoopBackOff`, `Init:1/2`, `Completed`…), the ready containers, the restarts and when the last one was, the node,
the age, the QoS class and the owner. A native sidecar (an init container with `restartPolicy: Always`) counts with the
containers once it has started and does not hold the pod in `Init`. Known differences from `kubectl`: a pod waiting
for its scheduling gates shows `Pending`, not `SchedulingGated`; ephemeral containers are not shown; while a pod
initializes, its restarts do not include those of regular init containers. A problem pod — the image cannot be pulled, a container was
killed for memory (`OOMKilled`), restarts in a loop (`CrashLoopBackOff` or, between restarts, exited with an error), the
pod waits to be scheduled (`Pending`) or has failed — is highlighted; the problems-only filter leaves just them.

## Pod tab {#pod}

Click a pod to open its tab. [[ui:objects.tabs.overview]] shows the phase, the conditions, the node, the IP, the QoS,
the owner (a click opens the workload) and every container, init containers too: the image and the digest that is
running, the state and the last state with the reason, the exit code and the time, the restarts, the probes, the
requests and limits and the environment variables — names and sources only (a value in the spec, a ConfigMap or
Secret key, a field), never the values. [[ui:objects.tabs.events]] and YAML are there too. YAML shows the object
without `managedFields` and without the annotations that copy the whole object; the values of environment variables
with secret-looking names (PASSWORD, TOKEN, KEY…) are replaced by `***`, other values are shown.
[[ui:objects.tabs.resources]] shows the current CPU and memory usage per container; [[ui:change.action.deletePod]]
on the overview deletes the pod (see [Resources and changes](resources-changes.md#changes)).

## Logs {#logs}

[[ui:logs.title]] shows the log of a container: choose the [[ui:logs.container]], the [[ui:logs.previous]], the
[[ui:logs.tail]] (500 by default) or where to start from ([[ui:logs.since]]), and [[ui:logs.follow]]. The previous
run has already finished, so with [[ui:logs.previous]] following is off, and the option says why.
[[ui:logs.timestamps]] adds the time of each line. The [[ui:logs.filter]] — a substring or a regular expression,
[[ui:logs.caseSensitive]] if needed — runs in Glafira before the lines reach the window, and the counter shows how many
lines were shown and how many were read; it changes even when no new line matches.

The window keeps the last 100,000 lines; when more arrive, a note above the lines says how many older lines were
dropped. A line longer than 64 KB is cut and marked [[ui:logs.truncated]]; long lines scroll sideways. Bytes that are
not UTF-8 are shown as replacement characters (�).

When the connection is lost while following, the status says so, and Glafira reconnects from the last line it has,
without repeats; it reports an error after a minute without success. A connection that brings no line for almost 5
minutes counts as lost (the computer slept, the network or VPN changed); when the container is just quiet, the stream
opens again without notice. When the credentials of an exec plugin have run out, Glafira runs the plugin again to
reconnect. An expired session, a TLS error or missing rights end the log at once. When the container stops or the pod
is deleted, the log ends.

[[ui:logs.save]] reads the log again with the same settings (without following) and saves the whole lines to a file.
The file appears only when the whole log is read (on macOS and Linux only you may read it); disconnecting stops the
saving, and a log that stops sending lines for longer than the request timeout ends it with an error.

## When the session expires {#session}

If the token expires while you read the cluster, the tabs show the error with [[ui:objects.signInAgain]]: it connects
again at once (an exec plugin runs again) and opens the connection tab; when that cannot sign you in, the tab shows how
to sign in again. After that the open tabs of the connection read their data again by themselves, and the logs and the
lists that update themselves start over.
