# Проекты и workloads

Glafira читает кластер с вашими правами. Часть экрана, которую вам нельзя читать, показывается как «—» с причиной, а
остальной экран виден.

## Проекты {#projects}

В дереве пункт [[ui:sidebar.projects]] под подключением открывает список его проектов: проекты OpenShift или, в других
кластерах, namespace. Избранные проекты подключения — сверху (★); если проектов много, появляется фильтр. Если кластер
не даёт вам список проектов, дерево показывает проекты из профиля — избранные и
[[ui:connections.form.manualNamespaces]].

## Вкладка проекта {#project}

Проект открывается в своей вкладке: [[ui:objects.tabs.overview]], [[ui:project.tabs.workloads]],
[[ui:objects.tabs.pods]], [[ui:project.tabs.network]], [[ui:project.tabs.config]], [[ui:project.tabs.storage]],
[[ui:objects.tabs.events]] и [[ui:project.tabs.quotas]]. Glafira спрашивает у кластера, что вам можно читать в проекте,
и прячет подвкладки, на которые прав нет; ответ хранится 5 минут. Обзор показывает поды по фазам и проблемные поды,
workloads не в норме, почти исчерпанные квоты и события `Warning` за последний час.

## Списки, которые обновляются сами {#watch}

Workloads, поды и события проекта обновляются сами, пока открыта их подвкладка: Glafira следит за изменениями в
кластере. Без права на watch список перечитывается раз в 10 секунд. На остальных экранах есть [[ui:common.refresh]] и
[[ui:common.autoRefresh]] (каждые 10 или 30 секунд).

## Workloads {#workloads}

Deployment, StatefulSet, DaemonSet, DeploymentConfig (OpenShift), Job и CronJob, с фильтром по виду и имени. Вкладка
workload'а показывает реплики (желаемо, готово, доступно, обновлено), проблему, если она есть, условия, селектор подов,
стратегию, HPA и PodDisruptionBudget, историю выкладки (ReplicaSets или ReplicationControllers с ревизией и образами) и
контейнеры. [[ui:objects.tabs.pods]] — его поды, [[ui:objects.tabs.events]] — его события вместе с событиями его подов,
YAML — объект без `managedFields`.

## Сеть {#network}

Services с готовыми endpoints, Routes (OpenShift) с решением роутера и сроком встроенного сертификата, Ingress с
правилами, TLS-хостами и Routes, которые OpenShift создал из него, а при Gateway API — Gateway, HTTPRoute и GRPCRoute с
условиями от каждого родителя. Показываются только те виды, которые есть в вашем кластере.

## Конфигурация, хранилище и квоты {#config}

У ConfigMaps видны ключи; [[ui:project.config.showValues]] читает значения. У Secrets видны только имя, тип и ключи:
их значения Glafira не показывает никогда. Хранилище — PVC со статусом, классом, ёмкостью, режимами доступа и подами,
которые их монтируют. Квоты — каждая ResourceQuota (использовано и лимит) и границы LimitRange.
