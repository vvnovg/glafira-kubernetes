# Имитация DropApp на kind

Kubernetes в Docker, который Glafira узнаёт как Platform V DropApp 5.1.2: на нём интеграционный тест
`tests/it_dropapp.rs` проверяет платформу `dropapp` с подписью «DropApp 5.1.2», часть DropApp обзора кластера
(версия, проблемные `Component` и `Tool` с событием, узлы DropApp, `IngressTLS`), правила `cluster.componentFailed`,
`cluster.dropappNodeNotReady` и `cluster.ingressTlsNotReady`, список проектов из `projects.dropapp.ru` и источник
метрик — vmselect VictoriaMetrics через прокси API-сервера. Вход через OIDC (Dex) тест не проверяет — нужен браузер;
его проверяет смоук на этом стенде.

Что имитируется:

- **Dex** в `dapp-auth` (`auth.yaml`): HTTPS с самоподписанным CA (CA, сертификат и ключ пишет `up.sh` в `out/`),
  клиент `dapp` с секретом и `redirectURIs` DropApp (`http://localhost:8000`, `http://localhost:18000` — порты
  `kubectl oidc-login`, `http://127.0.0.1:9000/auth/callback`, `http://localhost:9000/auth/callback` — Loginapp),
  экран согласия пропускается. API-сервер принимает его токены, как у DropApp: `--oidc-client-id=dapp`,
  `--oidc-username-claim=email`, `--oidc-groups-claim=groups`, `--oidc-ca-file` (CA смонтирован в ноду через
  `extraMounts`, `kind.yaml`).
- **CRD** (`crds.yaml`): `config.dropapp.ru` (`ClusterMetadata`, `Component`, `Tool`), `nodes.dropapp.ru`
  (`DropAppNode`), `tls.dropapp.ru` (`IngressTLS`, в namespace), `projects.dropapp.ru` (`Project`), версии `v1alpha1`,
  имена ресурсов — константы `src-tauri/src/k8s/dropapp.rs`.
- **Объекты** (`objects.yaml`, статусы и событие пишет `up.sh`): `ClusterMetadata` (`glafira-dropapp`, `5.1.2`);
  `Component` `auth`, `cni`, `monitoring` (`success`) и `ingress-controller` (`upgrading`); `Tool` `dex`, `grafana`
  (`success`) и `console` (`failed`, `retry: 3`) с событием `Warning` `InstallFailed`; `DropAppNode`
  `glafira-dropapp-control-plane` (`ready`) и `worker-2` (`provisioning`); `IngressTLS` `default-cert` и `apps-cert`
  (`Ready=False`, `IssuerNotReady`) в `dapp-ingress`; `Project` и namespace `team-a` с Deployment `web`.
- **VictoriaMetrics** в `dapp-monitoring` (`metrics.yaml`): `vmstorage`, `vminsert`, `vmselect` за Service
  `vmselect-victoria-metrics-k8s-stack:8481` (`/select/0/prometheus`) и `vmagent`, который собирает метрики kubelet
  и cAdvisor через прокси API-сервера. `up.sh` ждёт, пока запрос `up` через прокси не вернёт данные.

Чего нет:

- `projects.dropapp.ru` у DropApp — агрегированный API `dapp-apiserver`, который отдаёт пользователю только его
  проекты; здесь это CRD без фильтрации по RBAC: `dev@dropapp.test` получает 403 на список, и Glafira показывает
  проекты из профиля.
- LDAP: у DropApp `oauth2.passwordConnector: ldap`, здесь — пароли Dex (`local`); групп у таких пользователей нет,
  RBAC привязан к email.
- Настоящие поля статуса объектов DropApp документация не показывает: стенд пишет то, что разбирает приложение, —
  `status.phase` и `status.retry` у `Component` и `Tool`, `status.status` у `DropAppNode`, условие `Ready` у
  `IngressTLS`. Область CRD (`Component`, `Tool` и `ClusterMetadata` — уровня кластера) тоже выбрана стендом.
- kube-state-metrics: владельцы подов в метриках не видны. Операторов DropApp нет — статусы не меняются сами.
- Событие `Warning` пишет `up.sh` с текущим временем; чтобы оно не пропало через час, у API-сервера
  `--event-ttl=720h`.

## Имя издателя Dex

Издатель — `https://dex.glafira-dropapp.localhost:5556`, одинаковый для браузера, Glafira, `kubectl oidc-login` и
API-сервера. Под Dex работает с `hostNetwork` на ноде и слушает её порт 5556; `kind.yaml` пробрасывает его на
`127.0.0.1:5556` рабочей машины. На macOS имена `*.localhost` разрешаются в `127.0.0.1` сами (на Linux — так же
с nss-myhostname или systemd-resolved, иначе нужна строка в `/etc/hosts`), поэтому ни `/etc/hosts`, ни `sudo` на
рабочей машине не нужны.
В ноде это имя не разрешается: API-серверу (статический под с `hostNetwork`) его даёт `hostAliases` из
`patches/kube-apiserver+strategic.yaml` — патч kubeadm, kubelet дописывает запись в `/etc/hosts` пода; `/etc/hosts`
ноды не меняется. Порт 5556 рабочей машины должен быть свободен.

## Запуск

```bash
brew install kind               # или https://kind.sigs.k8s.io/docs/user/quick-start/
dev/kind/dropapp/up.sh          # около минуты с нуля; повторный запуск досоздаёт недостающее
GO_IT_DROPAPP_KUBECONFIG="$PWD/dev/kind/dropapp/out/kubeconfig" \
  cargo test --manifest-path src-tauri/Cargo.toml --test it_dropapp
dev/kind/dropapp/down.sh        # удаляет кластер и out/
```

Образы (Dex, VictoriaMetrics) скрипт не скачивает — они должны быть у Docker заранее:
`ghcr.io/dexidp/dex:v2.45.1`, `victoriametrics/vmstorage:v1.148.0-cluster`, `victoriametrics/vminsert:v1.148.0-cluster`,
`victoriametrics/vmselect:v1.148.0-cluster`, `victoriametrics/vmagent:v1.148.0`. В ноду их кладёт `kind load
docker-image`, а если он не справляется с многоплатформенным образом (хранилище containerd в Docker Desktop) —
`docker save --platform` и `ctr images import` в ноде.

`it_dropapp` запускается вручную: в CI второй кластер с OIDC не поднимается, и без `GO_IT_DROPAPP_KUBECONFIG` тест
пропускается. `~/.kube/config` не меняется: `kind` пишет в `out/kind-admin.kubeconfig`, а `up.sh` — свой
`out/kubeconfig` с контекстами:

- `admin` — сертификат администратора kind (тест);
- `oidc-admin` и `oidc-dev` — `kubectl oidc-login get-token` с издателем, `--oidc-client-id=dapp`, секретом
  клиента, `--oidc-extra-scope=email`, `--oidc-extra-scope=groups` и `--certificate-authority=out/dex-ca.crt`, как
  kubeconfig от Loginapp (у каждого свой `--token-cache-dir`). Для `kubectl` нужен плагин
  `brew install int128/kubelogin/kubelogin`.

Пользователи Dex:

| Пользователь | Пароль | Права |
|---|---|---|
| `admin@dropapp.test` | `dropapp-admin` | `cluster-admin` |
| `dev@dropapp.test` | `dropapp-dev` | `admin` в namespace `team-a` |

Клиент `dapp`, секрет `glafira-dropapp-secret`.

## Glafira против имитации

Импортируйте `dev/kind/dropapp/out/kubeconfig`: контекст `admin` — вход
сертификатом, `oidc-admin` и `oidc-dev` — вход через OIDC (Dex) с издателем и клиентом из аргументов
`oidc-login`. Сертификат Dex подписан своим CA: импорт читает его из `--certificate-authority` плагина (`up.sh` пишет
абсолютный путь к `out/dex-ca.crt`) в поле «CA издателя (PEM)» формы. В браузере — форма Dex: email и пароль из
таблицы.

`dev/kind/dropapp/out/` в git не попадает: в нём ключ администратора кластера, ключи CA и сервера Dex, кэш токенов.
CA смонтирован в ноду при создании кластера: удаляйте `out/` только через `down.sh`.
