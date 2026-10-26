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
and hides the subtabs you have no rights for; the answer is kept for 5 minutes. The overview shows the pods by phase
and the problem pods, the workloads that are not fine, the quotas that are almost used up and the `Warning` events of
the last hour.

## Lists that update themselves {#watch}

Workloads, pods and events of a project update by themselves while their subtab is open: Glafira watches the
changes in the cluster. Without the right to watch, the list refreshes every 10 seconds. Other screens have
[[ui:common.refresh]] and [[ui:common.autoRefresh]] (every 10 or 30 seconds).

## Workloads {#workloads}

Deployment, StatefulSet, DaemonSet, DeploymentConfig (OpenShift), Job and CronJob, with a filter by kind and name.
A workload tab shows the replicas (desired, ready, available, updated), the problem if there is one, the conditions,
the pod selector, the strategy, the HPA and the PodDisruptionBudget, the rollout history (ReplicaSets or
ReplicationControllers with the revision and the images) and the containers. [[ui:objects.tabs.pods]] lists its
pods, [[ui:objects.tabs.events]] shows its events together with those of its pods, and YAML shows the object without
`managedFields`.

## Network {#network}

Services with the ready endpoints, Routes (OpenShift) with the router admission and the expiry of the built-in
certificate, Ingress with the rules, the TLS hosts and the Routes OpenShift created from it, and with Gateway API —
Gateway, HTTPRoute and GRPCRoute with the conditions of each parent. Only the kinds your cluster has are shown.

## Configuration, storage and quotas {#config}

ConfigMaps show their keys; [[ui:project.config.showValues]] reads the values. Secrets show only the name, the type
and the keys: Glafira never shows their values. Storage lists the PVCs with the status, the class, the capacity, the
access modes and the pods that mount them. Quotas show each ResourceQuota (used and limit) and the LimitRange limits.
