# Глоссарий переводов Glafira for Kubernetes

Единые решения по терминам. Эталон ключей и параметров — `en.json`, эталон смысла — `ru.json`. Меняя решение,
исправьте его во всех словарях одним коммитом. Общие правила стиля — как в Glafira for Apache Kafka: коротко и
нейтрально, параметры `{{…}}` и теги `<code>` не переводятся, многоточие — `…`, тире — `—`.

## Не переводится
OpenShift, OKD, Kubernetes, Platform V DropApp, Glafira, OAuth, OIDC, TLS, PEM, DNS, TCP, CA, API, URL, JSON, MCP,
LLM, kubeconfig, `oc`, `kubectl`; названия дистрибутивов (EKS, GKE, AKS, Rancher, Deckhouse, k3s, RKE2); названия
видов ресурсов (Pod, Deployment, StatefulSet, DaemonSet, DeploymentConfig, Job, CronJob, Route, Ingress, Gateway,
HTTPRoute, GRPCRoute, Service, ConfigMap, Secret, PVC, HPA, ResourceQuota, LimitRange, ClusterOperator) и групп API
(Gateway API); `request` / `limit` в значении полей ресурсов; команды, адреса и имена переменных (`HTTPS_PROXY`,
`NO_PROXY`, `oc login --web`, `interactiveMode`).

## Термины

| en | ru | Примечание |
|---|---|---|
| connection | подключение | |
| cluster | кластер | |
| project | проект | namespace OpenShift; «namespace» — только там, где речь об обычном Kubernetes |
| node | нода | |
| pod | под | в тексте; вид ресурса в таблицах — Pod |
| workload | workload | |
| sign in / sign-in | войти / вход | не «логин» |
| token | токен | |
| client certificate | клиентский сертификат | |
| trusted CA | доверенный CA | |
| certificate verification | проверка сертификата | |
| proxy | прокси | |
| read-only | только чтение | |
| environment | окружение | local, dev, test, stage, prod не переводятся |
| folder | папка | |
| sign in again | войдите снова | заголовок блока на вкладке подключения, когда сессия истекла или токена нет; про токен и сертификат — «истёк» («токен истёк или неверен») |
| not allowed | нет прав | «Not allowed: …» → «Нет прав: …»; «права» — права пользователя в тексте («какие бы права у вас ни были») |
| connection check / test connection | проверка подключения / проверить подключение | |
| platform | платформа | OpenShift или Kubernetes (§4.3 спеки) |
| distribution | дистрибутив | EKS, GKE, Rancher… |
| exec plugin | exec-плагин | команда kubeconfig, которая выдаёт учётные данные |
| launch line | строка запуска | путь, аргументы и переменные exec-плагина |
| allow (to run a command) | разрешить (запуск) | подтверждение строки запуска |
| cluster overview | обзор кластера | |
| condition | условие | `status.conditions` объекта; названия условий (`Ready`, `Available`) не переводятся |
| ready / desired / available / updated | готово / желаемо / доступно / обновлено | реплики workload'а |
| rollout history | история выкладки | ReplicaSets или ReplicationControllers |
| event | событие | `Normal` и `Warning` не переводятся |
| endpoint | endpoint | готовые endpoints сервиса |
| taint | taint | не переводится |
| probe | проба | `Liveness-проба`, `Readiness-проба`, `Startup-проба` |
| capacity / allocatable | ёмкость / доступно подам | ресурсы ноды |
| logs, follow | логи, следить за новыми строками | |
| previous run | предыдущий запуск | логи прошлого запуска контейнера (`previous`) |
| accepted / programmed | принят / настроен | заголовки условий `Accepted` и `Programmed` (Route, Gateway); значения условий не переводятся |
| expired | истёк | срок сертификата Route, токена |
| the list is incomplete | список неполный | показаны первые 10 000 объектов (`truncated`) |
| dropped (log lines) | отброшены (строки лога) | ранние строки, которые не помещаются в окно логов |
| cluster check / check | проверка кластера / проверка | запуск встроенных правил; «Проверить кластер» |
| finding | находка | результат правила проверки; вкладка с находками и замечаниями — «Замечания» (Findings) |
| LLM note / note | замечание LLM / замечание | пишет LLM-ассистент через MCP; статусы — «решено», «не актуально» |
| severity: critical / warning / info | строгость: критично / предупреждение / информация | |
| hide / restore (a finding) | скрыть / вернуть | скрытая находка не считается в значках |
| restart loop | перезапуски по кругу | `CrashLoopBackOff` или завершение с ошибкой между перезапусками |
| cordoned (node) | закрыта для новых подов | `spec.unschedulable` |
| LLM assistant | LLM-ассистент | Claude Desktop, Claude Code, Cursor… |
| LLM access | доступ LLM | настройка подключения: «Выключен» / «Только метаданные» |
| LLM projects | проекты для LLM | `llmNamespaces`: «Все доступные» / «Только перечисленные» |
| MCP client / approve / revoke | MCP-клиент / разрешить / отозвать | подтверждение нового клиента диалогом |
