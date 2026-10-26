//! Хронология workload'а для `observe_workload` (спец. §10.4): сравнение соседних образцов
//! состояния — реплики, поды, готовность, рестарты контейнеров, выкладка, события `Warning`.

use std::collections::BTreeMap;

use glafira_mcp_core::BackendError;
use serde::Serialize;

use crate::model::{ObjectRef, PodState, WorkloadState};

pub const MIN_DURATION_S: u32 = 10;
/// Ниже таймаута запроса 60 с, принятого по умолчанию в клиентах на TypeScript SDK.
pub const MAX_DURATION_S: u32 = 50;
pub const DEFAULT_DURATION_S: u32 = 30;
/// Пауза между образцами.
pub const INTERVAL_S: u32 = 2;
pub const MAX_EVENTS: usize = 500;
/// Сколько сверх окна ждать последний образец: вместе с MAX_DURATION_S вызов укладывается в 60 с.
pub const READ_GRACE_S: u32 = 3;

/// Реплики workload'а.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Replicas {
    pub desired: Option<i32>,
    pub ready: Option<i32>,
    pub available: Option<i32>,
}

impl From<&WorkloadState> for Replicas {
    fn from(s: &WorkloadState) -> Self {
        Self {
            desired: s.desired,
            ready: s.ready,
            available: s.available,
        }
    }
}

/// Состояние в начале и в конце наблюдения: без событий `Warning` (они — в хронологии).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    #[serde(flatten)]
    pub replicas: Replicas,
    pub updated: Option<i32>,
    pub pods: Vec<PodState>,
}

impl From<&WorkloadState> for Snapshot {
    fn from(s: &WorkloadState) -> Self {
        Self {
            replicas: Replicas::from(s),
            updated: s.updated,
            pods: s.pods.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum EventKind {
    #[serde(rename_all = "camelCase")]
    ReplicasChanged { from: Replicas, to: Replicas },
    #[serde(rename_all = "camelCase")]
    PodAdded { pod: String },
    #[serde(rename_all = "camelCase")]
    PodRemoved { pod: String },
    #[serde(rename_all = "camelCase")]
    PodReady { pod: String },
    #[serde(rename_all = "camelCase")]
    PodNotReady { pod: String },
    #[serde(rename_all = "camelCase")]
    ContainerRestarted {
        pod: String,
        container: String,
        restarts: i32,
        reason: Option<String>,
        exit_code: Option<i32>,
    },
    /// Обновлённых реплик стало больше или меньше: идёт выкладка.
    #[serde(rename_all = "camelCase")]
    RolloutProgress {
        updated: Option<i32>,
        desired: Option<i32>,
    },
    #[serde(rename_all = "camelCase")]
    WarningEvent {
        reason: String,
        object: ObjectRef,
        message: String,
        count: i32,
    },
    #[serde(rename_all = "camelCase")]
    SampleFailed { code: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub at_ms: u64,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub replica_changes: usize,
    pub pods_added: usize,
    pub pods_removed: usize,
    pub container_restarts: usize,
    pub warnings: usize,
    /// Время, когда готовых реплик было меньше желаемого: интервал до следующего удачного образца.
    pub below_desired_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    pub started_at_ms: u64,
    pub duration_ms: u64,
    pub samples: usize,
    pub initial: Option<Snapshot>,
    #[serde(rename = "final")]
    pub last: Option<Snapshot>,
    pub events: Vec<Event>,
    pub truncated: bool,
    pub summary: Summary,
}

#[derive(Debug, Default)]
pub struct Recorder {
    prev: Option<(WorkloadState, u64)>,
    initial: Option<Snapshot>,
    /// Сколько раз уже случилось каждое событие `Warning` (по ключу события).
    warnings: BTreeMap<String, i32>,
    events: Vec<Event>,
    truncated: bool,
    samples: usize,
    summary: Summary,
}

fn below_desired(s: &WorkloadState) -> bool {
    s.ready.unwrap_or(0) < s.desired.unwrap_or(0)
}

impl Recorder {
    fn push(&mut self, at_ms: u64, kind: EventKind) {
        match &kind {
            EventKind::ReplicasChanged { .. } => self.summary.replica_changes += 1,
            EventKind::PodAdded { .. } => self.summary.pods_added += 1,
            EventKind::PodRemoved { .. } => self.summary.pods_removed += 1,
            EventKind::ContainerRestarted { .. } => self.summary.container_restarts += 1,
            EventKind::WarningEvent { .. } => self.summary.warnings += 1,
            _ => {}
        }
        if self.events.len() >= MAX_EVENTS {
            self.truncated = true;
        } else {
            self.events.push(Event { at_ms, kind });
        }
    }

    pub fn sample(&mut self, at_ms: u64, s: &WorkloadState) {
        self.samples += 1;
        let Some((prev, prev_at)) = self.prev.take() else {
            // События до начала наблюдения — точка отсчёта, а не новость.
            self.warnings = s
                .warnings
                .iter()
                .map(|w| (w.key.clone(), w.count))
                .collect();
            self.initial = Some(Snapshot::from(s));
            self.prev = Some((s.clone(), at_ms));
            return;
        };
        if below_desired(&prev) {
            self.summary.below_desired_ms += at_ms.saturating_sub(prev_at);
        }
        let (from, to) = (Replicas::from(&prev), Replicas::from(s));
        if from != to {
            self.push(at_ms, EventKind::ReplicasChanged { from, to });
        }
        if prev.updated != s.updated {
            self.push(
                at_ms,
                EventKind::RolloutProgress {
                    updated: s.updated,
                    desired: s.desired,
                },
            );
        }
        let old: BTreeMap<&str, &PodState> =
            prev.pods.iter().map(|p| (p.name.as_str(), p)).collect();
        let new: BTreeMap<&str, &PodState> = s.pods.iter().map(|p| (p.name.as_str(), p)).collect();
        let mut kinds = Vec::new();
        for name in old.keys().filter(|n| !new.contains_key(*n)) {
            kinds.push(EventKind::PodRemoved {
                pod: name.to_string(),
            });
        }
        for (name, p) in &new {
            let Some(before) = old.get(name) else {
                kinds.push(EventKind::PodAdded {
                    pod: name.to_string(),
                });
                continue;
            };
            let restarted: BTreeMap<&str, i32> = before
                .containers
                .iter()
                .map(|c| (c.name.as_str(), c.restarts))
                .collect();
            for c in &p.containers {
                if c.restarts > restarted.get(c.name.as_str()).copied().unwrap_or(0) {
                    kinds.push(EventKind::ContainerRestarted {
                        pod: name.to_string(),
                        container: c.name.clone(),
                        restarts: c.restarts,
                        reason: c.last_reason.clone(),
                        exit_code: c.last_exit_code,
                    });
                }
            }
            match (before.ready, p.ready) {
                (false, true) => kinds.push(EventKind::PodReady {
                    pod: name.to_string(),
                }),
                (true, false) => kinds.push(EventKind::PodNotReady {
                    pod: name.to_string(),
                }),
                _ => {}
            }
        }
        for w in &s.warnings {
            let seen = self.warnings.insert(w.key.clone(), w.count).unwrap_or(0);
            if w.count > seen {
                kinds.push(EventKind::WarningEvent {
                    reason: w.reason.clone(),
                    object: w.object.clone(),
                    message: w.message.clone(),
                    count: w.count,
                });
            }
        }
        for k in kinds {
            self.push(at_ms, k);
        }
        self.prev = Some((s.clone(), at_ms));
    }

    /// Образец не прочитан: в хронологии — ошибка, состояние не меняется.
    pub fn failed(&mut self, at_ms: u64, e: BackendError) {
        self.push(
            at_ms,
            EventKind::SampleFailed {
                code: e.code,
                message: e.message,
            },
        );
    }

    pub fn finish(self, started_at_ms: u64, duration_ms: u64) -> Observation {
        let mut summary = self.summary;
        if let Some((last, at)) = &self.prev
            && below_desired(last)
        {
            summary.below_desired_ms += duration_ms.saturating_sub(*at);
        }
        Observation {
            started_at_ms,
            duration_ms,
            samples: self.samples,
            initial: self.initial,
            last: self.prev.as_ref().map(|(s, _)| Snapshot::from(s)),
            events: self.events,
            truncated: self.truncated,
            summary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContainerRestarts, Warning};

    fn pod(name: &str, ready: bool, restarts: i32) -> PodState {
        PodState {
            name: name.into(),
            phase: "Running".into(),
            ready,
            containers: vec![ContainerRestarts {
                name: "web".into(),
                restarts,
                last_reason: (restarts > 0).then(|| "OOMKilled".into()),
                last_exit_code: (restarts > 0).then_some(137),
            }],
        }
    }

    fn warning(key: &str, count: i32) -> Warning {
        Warning {
            key: key.into(),
            reason: "BackOff".into(),
            object: ObjectRef {
                kind: "Pod".into(),
                name: "web-1".into(),
            },
            message: "Back-off restarting failed container".into(),
            count,
        }
    }

    fn state(
        ready: i32,
        updated: i32,
        pods: Vec<PodState>,
        warnings: Vec<Warning>,
    ) -> WorkloadState {
        WorkloadState {
            desired: Some(2),
            ready: Some(ready),
            available: Some(ready),
            updated: Some(updated),
            pods,
            warnings,
        }
    }

    fn kinds(o: &Observation) -> Vec<String> {
        o.events
            .iter()
            .map(|e| {
                let v = serde_json::to_value(&e.kind).unwrap();
                v["kind"].as_str().unwrap().to_string()
            })
            .collect()
    }

    #[test]
    fn timeline_of_a_rollout_with_a_crashing_pod() {
        let mut r = Recorder::default();
        r.sample(
            0,
            &state(
                2,
                2,
                vec![pod("web-1", true, 0), pod("web-2", true, 0)],
                vec![warning("old", 3)],
            ),
        );
        r.sample(
            2000,
            &state(
                1,
                3,
                vec![
                    pod("web-1", false, 1),
                    pod("web-2", true, 0),
                    pod("web-3", false, 0),
                ],
                vec![warning("old", 3), warning("new", 1)],
            ),
        );
        r.failed(4000, BackendError::new("TIMEOUT", "slow"));
        r.sample(
            6000,
            &state(
                2,
                3,
                vec![pod("web-1", true, 1), pod("web-3", true, 0)],
                vec![warning("old", 4)],
            ),
        );
        let o = r.finish(100, 8000);
        assert_eq!(
            kinds(&o),
            [
                "replicasChanged",
                "rolloutProgress",
                "containerRestarted",
                "podNotReady",
                "podAdded",
                "warningEvent",
                "sampleFailed",
                "replicasChanged",
                "podRemoved",
                "podReady",
                "podReady",
                "warningEvent",
            ]
        );
        assert_eq!(o.samples, 3);
        assert_eq!(
            o.summary.below_desired_ms, 4000,
            "2000–6000 мс: готов один из двух"
        );
        assert_eq!(o.summary.container_restarts, 1);
        assert_eq!(
            o.summary.warnings, 2,
            "событие до начала не считается, его повтор — да"
        );
        assert_eq!(o.initial.as_ref().unwrap().pods.len(), 2);
        assert_eq!(o.last.as_ref().unwrap().pods.len(), 2);
        let restart = serde_json::to_value(&o.events[2]).unwrap();
        assert_eq!(restart["reason"], "OOMKilled");
        assert_eq!(restart["exitCode"], 137);
        assert_eq!(restart["atMs"], 2000);
    }

    #[test]
    fn events_are_capped() {
        let mut r = Recorder::default();
        r.sample(0, &state(2, 2, vec![], vec![]));
        for i in 0..(MAX_EVENTS + 10) {
            r.failed(i as u64, BackendError::new("TIMEOUT", "slow"));
        }
        let o = r.finish(0, 1);
        assert_eq!(o.events.len(), MAX_EVENTS);
        assert!(o.truncated);
    }
}
