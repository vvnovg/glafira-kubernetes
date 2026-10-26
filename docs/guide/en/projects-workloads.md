# Projects and workloads

Glafira reads the cluster with your rights. A part of a screen you may not read shows a dash (—) with the reason, and
the rest of the screen is shown.

## Projects {#projects}

In the tree, [[ui:sidebar.projects]] under a connection opens the list of its projects: OpenShift projects or, on
other clusters, namespaces. Favorite projects of the connection are on top (★); with many projects a filter
appears. If the cluster does not list projects for you, the tree shows the projects of the profile — its favorites
and [[ui:connections.form.manualNamespaces]].

## Project tab {#project}

A project opens in its own tab: [[ui:objects.tabs.overview]], [[ui:project.tabs.workloads]],
[[ui:objects.tabs.pods]], [[ui:project.tabs.network]], [[ui:project.tabs.config]], [[ui:project.tabs.storage]],
[[ui:objects.tabs.events]] and [[ui:project.tabs.quotas]]. Glafira asks the cluster what you may read in the project
and hides the subtabs you have no rights for. The answer is kept for 5 minutes; the tab asks again every 5 minutes and
when you press [[ui:common.refresh]] on the overview, so a subtab hidden by rights that changed comes back. The
overview shows the pods by phase and the problem pods, the workloads that are not fine (and the kinds of workloads you
may not read, with the reason), the quotas used by 90 % or more and the `Warning` events of the last hour.

## Lists that update themselves {#watch}

Workloads, pods and events of a project update by themselves while their subtab is open: Glafira watches the
changes in the cluster. Without the right to watch, the list refreshes every 10 seconds. A kind of workload you may
not read is shown as a line with the reason, and the other kinds are listed. After a network error the list tries
again by itself; when the session expires, it stops with the error. Other screens have [[ui:common.refresh]] and
[[ui:common.autoRefresh]] (every 10 or 30 seconds).

While a tab is hidden behind another one, it does not ask the cluster: its lists stop watching and auto-refresh
pauses. When you come back, the lists are read anew, and auto-refresh starts counting its interval again
([[ui:common.refresh]] gives fresh data at once). A log that follows new lines keeps running in a hidden tab.

A list reads at most 10,000 objects. Pods, workloads, events, storage, the overview of a project, the nodes and the
projects in the tree say so when the list is cut: [[ui:common.truncated]]. Network, configuration, quotas and the
cluster overview read at most 10,000 objects of a kind too, but do not show this note yet.

## Workloads {#workloads}

Deployment, StatefulSet, DaemonSet, DeploymentConfig (OpenShift), Job and CronJob, with a filter by kind and name. A
workload tab shows the replicas (desired, ready, available, updated), the problem if there is one, the conditions, the
pod selector, the strategy, the HPA and the PodDisruptionBudget, the rollout history (ReplicaSets or
ReplicationControllers with the revision and the images) and the containers. [[ui:objects.tabs.pods]] lists its pods.
[[ui:objects.tabs.events]] shows its events together with those of the objects it creates (ReplicaSets, Jobs,
ReplicationControllers of a DeploymentConfig, pods): they are recognized by the names Kubernetes generates for them, so
the pods of an indexed Job, Jobs created from a CronJob by hand and the children of a workload whose name is longer than
about 46 characters are not included. YAML shows the object as `kubectl get -o yaml` does, but without `managedFields`,
without the annotations that copy the whole object (`kubectl.kubernetes.io/last-applied-configuration` and the like) and
without the private key of a Route; the values of environment variables with secret-looking names (PASSWORD, TOKEN,
KEY…) are replaced by `***`.

## Network {#network}

Services with the ready endpoints, Routes (OpenShift) with the router admission and the expiry of the built-in
certificate (less than 30 days left is highlighted; an expired one is red and marked «expired»), Ingress with the
rules, the TLS hosts and the Routes OpenShift created from it, and with Gateway API — Gateway, HTTPRoute and GRPCRoute
with the conditions of each parent. Only the kinds your cluster has are shown.

## Configuration, storage and quotas {#config}

ConfigMaps show their keys; [[ui:project.config.showValues]] reads the values (a long value wraps and does not widen
the table). Secrets show only the name, the type and the keys: Glafira never shows their values. Storage lists the
PVCs with the status (`Pending` is highlighted as a warning, `Lost` as an error), the class, the capacity, the access
modes and the pods that mount them. Quotas show each ResourceQuota (used and limit; 90 % or more is highlighted) and
the LimitRange limits.
