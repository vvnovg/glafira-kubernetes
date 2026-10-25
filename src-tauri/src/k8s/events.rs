//! События проекта (спец. §6.3–6.5): все события namespace, события объекта или workload'а
//! вместе с его подами, ReplicaSet, ReplicationController и Job. Новые — сверху.

use k8s_openapi::api::core::v1::Event;
use kube::api::ListParams;
use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::k8s::pods::OwnerRef;
use crate::k8s::read::{Listed, ReadClient};
use crate::k8s::view::seconds;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventRow {
    /// Ключ строки в списке и в watch (uid события).
    pub key: String,
    /// `Normal` или `Warning`.
    #[serde(rename = "type")]
    pub kind: String,
    pub reason: String,
    pub object: OwnerRef,
    pub message: String,
    pub count: i32,
    pub first_seen: Option<i64>,
    pub last_seen: Option<i64>,
    /// Кто сообщил: компонент и узел.
    pub source: Option<String>,
}

/// Строка события. Время — из серии (`series.lastObservedTime`), `lastTimestamp`, `eventTime`
/// или времени создания: новые события `events.k8s.io` заполняют не все поля.
pub fn event_row(e: &Event) -> EventRow {
    let created = e.metadata.creation_timestamp.as_ref().map(seconds);
    let event_time = e.event_time.as_ref().map(|t| t.0.as_second());
    let last_seen = e
        .series
        .as_ref()
        .and_then(|s| s.last_observed_time.as_ref())
        .map(|t| t.0.as_second())
        .or_else(|| e.last_timestamp.as_ref().map(seconds))
        .or(event_time)
        .or(created);
    let source = e
        .source
        .as_ref()
        .and_then(|s| s.component.clone())
        .or_else(|| e.reporting_component.clone())
        .filter(|s| !s.is_empty())
        .map(|c| {
            match e
                .source
                .as_ref()
                .and_then(|s| s.host.clone())
                .filter(|h| !h.is_empty())
            {
                Some(h) => format!("{c}, {h}"),
                None => c,
            }
        });
    EventRow {
        key: e
            .metadata
            .uid
            .clone()
            .or_else(|| e.metadata.name.clone())
            .unwrap_or_default(),
        kind: e.type_.clone().unwrap_or_else(|| "Normal".into()),
        reason: e.reason.clone().unwrap_or_default(),
        object: OwnerRef {
            kind: e.involved_object.kind.clone().unwrap_or_default(),
            name: e.involved_object.name.clone().unwrap_or_default(),
        },
        message: e.message.clone().unwrap_or_default(),
        count: e
            .series
            .as_ref()
            .and_then(|s| s.count)
            .or(e.count)
            .unwrap_or(1),
        first_seen: e
            .first_timestamp
            .as_ref()
            .map(seconds)
            .or(event_time)
            .or(created),
        last_seen,
        source,
    }
}

/// Чьи события нужны: объекта (`Pod web-1`) или workload'а — вместе с теми его детьми, которых
/// создаёт именно этот вид workload'а (по префиксу имени `web-` и виду остатка, см. `is_child`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventObject {
    pub kind: String,
    pub name: String,
}

/// Создаёт ли объект этого вида детей (workload или ReplicaSet): события такого объекта
/// собираются вместе с событиями детей, а не выбираются на сервере по одному объекту.
fn has_children(kind: &str) -> bool {
    matches!(
        kind,
        "Deployment"
            | "StatefulSet"
            | "DaemonSet"
            | "DeploymentConfig"
            | "Job"
            | "CronJob"
            | "ReplicaSet"
    )
}

/// Строчные латинские буквы и цифры: алфавит имён, которые Kubernetes генерирует сам.
fn lower_alnum(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

fn digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Случайный суффикс `generateName`: пять символов. Настоящий алфавит уже (без гласных и
/// похожих символов), но для отсечения «соседей» хватает длины и строчных букв с цифрами.
fn random5(s: &str) -> bool {
    s.len() == 5 && lower_alnum(s)
}

/// `<голова>-<хвост>` ровно с одним дефисом: у лишних сегментов (`web-api-…` у `web`) нет шанса
/// попасть в шаблон.
fn pair(s: &str, head: impl Fn(&str) -> bool, tail: impl Fn(&str) -> bool) -> bool {
    s.split_once('-').is_some_and(|(h, t)| head(h) && tail(t))
}

/// Подходит ли `rest` — имя ребёнка вида `child` без префикса `{workload}-` — под имя, которое
/// порождает workload вида `parent`. Остальные виды детей workload не создаёт и не получает.
fn is_child(parent: &str, child: &str, rest: &str) -> bool {
    match (parent, child) {
        // ReplicaSet `web-<hash>`, под `web-<hash>-<5>`.
        ("Deployment", "ReplicaSet") => lower_alnum(rest),
        ("Deployment", "Pod") => pair(rest, lower_alnum, random5),
        // Под `db-<порядковый номер>`.
        ("StatefulSet", "Pod") => digits(rest),
        ("DaemonSet" | "Job" | "ReplicaSet", "Pod") => random5(rest),
        // Job `report-<время расписания>`, его под `report-<время>-<5>`.
        ("CronJob", "Job") => digits(rest),
        ("CronJob", "Pod") => pair(rest, digits, random5),
        // ReplicationController `legacy-<n>`, его под `legacy-<n>-<5>`, под выкладки
        // `legacy-<n>-deploy`.
        ("DeploymentConfig", "ReplicationController") => digits(rest),
        ("DeploymentConfig", "Pod") => pair(rest, digits, |t| random5(t) || t == "deploy"),
        _ => false,
    }
}

/// Относится ли событие к объекту `o`: это он сам или его ребёнок.
pub fn concerns(row: &EventRow, o: &EventObject) -> bool {
    if row.object.kind == o.kind && row.object.name == o.name {
        return true;
    }
    row.object
        .name
        .strip_prefix(&format!("{}-", o.name))
        .is_some_and(|rest| is_child(&o.kind, &row.object.kind, rest))
}

/// Новые сверху.
pub fn sort_events(rows: &mut [EventRow]) {
    rows.sort_by(|a, b| b.last_seen.cmp(&a.last_seen).then(a.key.cmp(&b.key)));
}

/// События проекта или объекта. События пода выбираются на сервере (`fieldSelector`),
/// события workload'а — по объекту и именам его детей (`concerns`).
pub async fn list_events(
    r: &ReadClient,
    namespace: &str,
    object: Option<&EventObject>,
) -> AppResult<Listed<EventRow>> {
    let lp = match object {
        Some(o) if !has_children(&o.kind) => ListParams::default().fields(&format!(
            "involvedObject.kind={},involvedObject.name={}",
            o.kind, o.name
        )),
        _ => ListParams::default(),
    };
    let events = r.list::<Event>(Some(namespace), &lp).await?;
    let mut rows: Vec<EventRow> = events
        .items
        .iter()
        .map(event_row)
        .filter(|e| object.is_none_or(|o| concerns(e, o)))
        .collect();
    sort_events(&mut rows);
    Ok(Listed {
        items: rows,
        truncated: events.truncated,
    })
}

#[cfg(test)]
pub(crate) mod fixtures {
    pub fn event(uid: &str, kind: &str, object: (&str, &str), last: &str) -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "Event",
            "metadata": {"name": format!("{}.{uid}", object.1), "uid": uid, "namespace": "team-a",
                "creationTimestamp": "2026-10-25T09:00:00Z"},
            "involvedObject": {"kind": object.0, "name": object.1, "namespace": "team-a"},
            "type": kind, "reason": "BackOff", "message": "Back-off restarting failed container",
            "count": 5, "firstTimestamp": "2026-10-25T09:30:00Z", "lastTimestamp": last,
            "source": {"component": "kubelet", "host": "w1"}})
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::k8s::testing::mock_client;
    use std::time::Duration;

    #[test]
    fn rows_take_time_from_whatever_the_event_has() {
        let e: Event = serde_json::from_value(event(
            "u1",
            "Warning",
            ("Pod", "web-1"),
            "2026-10-25T10:00:00Z",
        ))
        .unwrap();
        let row = event_row(&e);
        assert_eq!(row.kind, "Warning");
        assert_eq!((row.count, row.last_seen), (5, Some(1_792_922_400)));
        assert_eq!(row.first_seen, Some(1_792_922_400 - 1800));
        assert_eq!(row.source.as_deref(), Some("kubelet, w1"));
        let series: Event = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "x", "creationTimestamp": "2026-10-25T09:00:00Z"},
            "involvedObject": {"kind": "Pod", "name": "a"},
            "eventTime": "2026-10-25T09:10:00.000000Z", "reportingComponent": "scheduler",
            "series": {"count": 12, "lastObservedTime": "2026-10-25T09:20:00.000000Z"}}))
        .unwrap();
        let row = event_row(&series);
        assert_eq!(row.count, 12);
        assert_eq!(row.last_seen, Some(1_792_922_400 - 2400));
        assert_eq!(row.first_seen, Some(1_792_922_400 - 3000));
        assert_eq!(row.source.as_deref(), Some("scheduler"));
        assert_eq!(row.kind, "Normal");
        assert_eq!(row.key, "x", "без uid — по имени");
    }

    #[test]
    fn events_of_a_workload_include_its_children() {
        let row = |kind: &str, name: &str| {
            event_row(
                &serde_json::from_value(event("u", "Normal", (kind, name), "2026-10-25T10:00:00Z"))
                    .unwrap(),
            )
        };
        let web = EventObject {
            kind: "Deployment".into(),
            name: "web".into(),
        };
        assert!(concerns(&row("Deployment", "web"), &web));
        assert!(concerns(&row("ReplicaSet", "web-7d9f"), &web));
        assert!(concerns(&row("Pod", "web-7d9f-x5k2m"), &web));
        assert!(
            !concerns(&row("Pod", "webhook-1"), &web),
            "только по префиксу «web-»"
        );
        assert!(!concerns(&row("Service", "web-svc"), &web));
        let pod = EventObject {
            kind: "Pod".into(),
            name: "web-1".into(),
        };
        assert!(
            !concerns(&row("Pod", "web-1-x"), &pod),
            "у пода — только его события"
        );
    }

    /// Событие объекта `kind name`, как его видит `concerns`.
    fn row(kind: &str, name: &str) -> EventRow {
        event_row(
            &serde_json::from_value(event("u", "Normal", (kind, name), "2026-10-25T10:00:00Z"))
                .unwrap(),
        )
    }

    fn object(kind: &str, name: &str) -> EventObject {
        EventObject {
            kind: kind.into(),
            name: name.into(),
        }
    }

    #[test]
    fn events_of_a_workload_skip_siblings_with_the_same_prefix() {
        let web = object("Deployment", "web");
        for (kind, name) in [
            // Соседний Deployment `web-api`: его ReplicaSet и поды.
            ("ReplicaSet", "web-api-6b8f9c7d5"),
            ("Pod", "web-api-6b8f9c7d5-x5k2m"),
            // CronJob `web-cleanup`: его Job и поды Job.
            ("Job", "web-cleanup-28123456"),
            ("Pod", "web-cleanup-28123456-ab12c"),
            // Deployment не создаёт Job и ReplicationController.
            ("Job", "web-7d9f5c8b6"),
            ("ReplicationController", "web-7d9f5c8b6"),
            // Другие объекты.
            ("Pod", "webhook-1"),
            ("Service", "web-7d9f5c8b6"),
            ("Pod", "web-"),
            ("Pod", "web"),
        ] {
            assert!(!concerns(&row(kind, name), &web), "{kind} {name}");
        }
        // Частые пары: имя одного — префикс другого.
        let redis = object("StatefulSet", "redis");
        assert!(!concerns(
            &row("Pod", "redis-exporter-7d9f5c8b6-x5k2m"),
            &redis
        ));
        assert!(!concerns(&row("Pod", "redis-exporter-0"), &redis));
        assert!(concerns(&row("Pod", "redis-0"), &redis));
        assert!(concerns(&row("StatefulSet", "redis"), &redis));
    }

    #[test]
    fn events_of_a_workload_include_the_children_it_creates() {
        let cases = [
            ("Deployment", "web", "ReplicaSet", "web-6b8f9c7d5"),
            ("Deployment", "web", "Pod", "web-6b8f9c7d5-x5k2m"),
            ("StatefulSet", "db", "Pod", "db-0"),
            ("StatefulSet", "db", "Pod", "db-12"),
            ("DaemonSet", "agent", "Pod", "agent-x5k2m"),
            ("Job", "migrate", "Pod", "migrate-ab12c"),
            ("CronJob", "report", "Job", "report-28123456"),
            ("CronJob", "report", "Pod", "report-28123456-ab12c"),
            (
                "DeploymentConfig",
                "legacy",
                "ReplicationController",
                "legacy-7",
            ),
            ("DeploymentConfig", "legacy", "Pod", "legacy-7-ab12c"),
            ("DeploymentConfig", "legacy", "Pod", "legacy-7-deploy"),
            ("ReplicaSet", "web-6b8f9c7d5", "Pod", "web-6b8f9c7d5-x5k2m"),
        ];
        for (parent, name, kind, child) in cases {
            let o = object(parent, name);
            assert!(concerns(&row(parent, name), &o), "сам {parent} {name}");
            assert!(
                concerns(&row(kind, child), &o),
                "{parent} {name}: {kind} {child}"
            );
        }
    }

    #[test]
    fn a_workload_gets_only_the_kinds_and_names_its_own_children_have() {
        // Что вид workload'а не создаёт и как выглядят не его дети.
        let cases = [
            // StatefulSet создаёт только поды `db-<номер>`.
            ("StatefulSet", "db", "Job", "db-28123456"),
            ("StatefulSet", "db", "ReplicaSet", "db-6b8f9c7d5"),
            ("StatefulSet", "db", "ReplicationController", "db-1"),
            ("StatefulSet", "db", "Pod", "db-replica-0"),
            ("StatefulSet", "db", "Pod", "db-6b8f9c7d5-x5k2m"),
            ("StatefulSet", "db", "Pod", "db-x5k2m"),
            // DaemonSet и Job: под с суффиксом из пяти символов, и только он.
            ("DaemonSet", "agent", "ReplicaSet", "agent-6b8f9c7d5"),
            ("DaemonSet", "agent", "Pod", "agent-extra-x5k2m"),
            ("DaemonSet", "agent", "Pod", "agent-0"),
            ("Job", "migrate", "Job", "migrate-28123456"),
            ("Job", "migrate", "Pod", "migrate-1-ab12c-zz"),
            ("Job", "migrate", "Pod", "migrate-ab12"),
            // CronJob: Job `<цифры>`, под `<цифры>-<5>`.
            ("CronJob", "report", "Job", "report-manual"),
            ("CronJob", "report", "Job", "report-daily-28123456"),
            ("CronJob", "report", "Pod", "report-daily-28123456-ab12c"),
            ("CronJob", "report", "Pod", "report-ab12c"),
            ("CronJob", "report", "ReplicaSet", "report-28123456"),
            // DeploymentConfig: RC `<n>`, под `<n>-<5>` или `<n>-deploy`.
            ("DeploymentConfig", "legacy", "ReplicaSet", "legacy-7"),
            (
                "DeploymentConfig",
                "legacy",
                "ReplicationController",
                "legacy-api-7",
            ),
            ("DeploymentConfig", "legacy", "Pod", "legacy-api-7-ab12c"),
            ("DeploymentConfig", "legacy", "Pod", "legacy-ab12c"),
            ("DeploymentConfig", "legacy", "Pod", "legacy-7-redeploy"),
            // Deployment: ReplicaSet `<hash>` и под `<hash>-<5>`, и больше ничего.
            ("Deployment", "web", "Pod", "web-6b8f9c7d5"),
            ("Deployment", "web", "Pod", "web-x5k2m"),
            ("Deployment", "web", "ReplicaSet", "web-6b8f9c7d5-x5k2m"),
            (
                "Deployment",
                "redis",
                "Pod",
                "redis-exporter-6b8f9c7d5-x5k2m",
            ),
            (
                "Deployment",
                "redis",
                "ReplicaSet",
                "redis-exporter-6b8f9c7d5",
            ),
            // ReplicaSet создаёт только поды `<5>`.
            (
                "ReplicaSet",
                "web-6b8f9c7d5",
                "ReplicaSet",
                "web-6b8f9c7d5-x5k2m",
            ),
            (
                "ReplicaSet",
                "web-6b8f9c7d5",
                "Pod",
                "web-6b8f9c7d5-x5k2m-0",
            ),
        ];
        for (parent, name, kind, child) in cases {
            assert!(
                !concerns(&row(kind, child), &object(parent, name)),
                "{parent} {name}: {kind} {child}"
            );
        }
        // Каждый вид workload'а знает своих детей: список видов не расходится с WorkloadKind.
        for kind in crate::k8s::workloads::WorkloadKind::ALL {
            assert!(has_children(&format!("{kind:?}")), "{kind:?}");
        }
    }

    #[tokio::test]
    async fn events_of_a_pod_are_selected_by_the_server_and_sorted() {
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "EventList", "metadata": {}, "items": [
                    event("old", "Normal", ("Pod", "web-1"), "2026-10-25T09:00:00Z"),
                    event("new", "Warning", ("Pod", "web-1"), "2026-10-25T10:00:00Z")
                ]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let pod = EventObject {
            kind: "Pod".into(),
            name: "web-1".into(),
        };
        let l = list_events(&r, "team-a", Some(&pod)).await.unwrap();
        let keys: Vec<_> = l.items.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["new", "old"]);
        list_events(&r, "team-a", None).await.unwrap();
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            seen[0]
                .uri
                .contains("fieldSelector=involvedObject.kind%3DPod%2CinvolvedObject.name%3Dweb-1"),
            "{}",
            seen[0].uri
        );
        assert!(!seen[1].uri.contains("fieldSelector"));
    }
}
