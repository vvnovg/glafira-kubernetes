//! События проекта (спец. §6.3–6.5): все события namespace, события объекта или workload'а
//! вместе с его подами, ReplicaSet и Job. Новые — сверху.

use k8s_openapi::api::core::v1::Event;
use kube::api::ListParams;
use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::k8s::pods::OwnerRef;
use crate::k8s::read::{Listed, ReadClient};
use crate::k8s::view::seconds;
use crate::k8s::workloads::WorkloadKind;

/// Виды объектов, которые создаёт workload: их события показываются вместе с его собственными.
const CHILD_KINDS: &[&str] = &["Pod", "ReplicaSet", "ReplicationController", "Job"];

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

/// Чьи события нужны: объекта (`Pod web-1`) или workload'а — вместе с его подами, ReplicaSet и
/// Job (по префиксу имени `web-`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventObject {
    pub kind: String,
    pub name: String,
}

fn is_workload(kind: &str) -> bool {
    serde_json::from_value::<WorkloadKind>(kind.into()).is_ok()
}

/// Относится ли событие к объекту `o`.
pub fn concerns(row: &EventRow, o: &EventObject) -> bool {
    if row.object.kind == o.kind && row.object.name == o.name {
        return true;
    }
    is_workload(&o.kind)
        && CHILD_KINDS.contains(&row.object.kind.as_str())
        && row.object.name.starts_with(&format!("{}-", o.name))
}

/// Новые сверху.
pub fn sort_events(rows: &mut [EventRow]) {
    rows.sort_by(|a, b| b.last_seen.cmp(&a.last_seen).then(a.key.cmp(&b.key)));
}

/// События проекта или объекта. События пода выбираются на сервере (`fieldSelector`),
/// события workload'а — по объекту и префиксу имени.
pub async fn list_events(
    r: &ReadClient,
    namespace: &str,
    object: Option<&EventObject>,
) -> AppResult<Listed<EventRow>> {
    let lp = match object {
        Some(o) if !is_workload(&o.kind) => ListParams::default().fields(&format!(
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
        assert!(concerns(&row("Pod", "web-7d9f-abc"), &web));
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
