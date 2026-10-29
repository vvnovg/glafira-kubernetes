//! Текущие requests и limits контейнеров (спец. §7.3): из шаблона подов workload'а — именно его
//! меняет форма. Если в шаблоне значения нет, а работающий под получил его от `LimitRange` по
//! умолчанию, оно тоже есть — с пометкой.

use k8s_openapi::api::core::v1::{Container, Pod, PodSpec};
use serde::Serialize;

use crate::k8s::quantity::parse_quantity;

/// CPU в ядрах и память в байтах; `None` — не задано.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Amounts {
    pub cpu: Option<f64>,
    pub memory: Option<f64>,
}

impl Amounts {
    fn of(
        map: Option<
            &std::collections::BTreeMap<
                String,
                k8s_openapi::apimachinery::pkg::api::resource::Quantity,
            >,
        >,
    ) -> Self {
        let q = |k: &str| {
            map.and_then(|m| m.get(k))
                .and_then(|v| parse_quantity(&v.0))
        };
        Self {
            cpu: q("cpu"),
            memory: q("memory"),
        }
    }

    /// Значения `pod`, которых нет здесь.
    fn missing_from(self, pod: Self) -> Self {
        Self {
            cpu: self.cpu.is_none().then_some(pod.cpu).flatten(),
            memory: self.memory.is_none().then_some(pod.memory).flatten(),
        }
    }

    /// Без тех значений, которые заданы в `other`.
    fn without(self, other: Self) -> Self {
        Self {
            cpu: other.cpu.is_none().then_some(self.cpu).flatten(),
            memory: other.memory.is_none().then_some(self.memory).flatten(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerCurrent {
    pub name: String,
    /// Init-контейнер: показывается, рекомендация не считается.
    pub init: bool,
    /// Нативный sidecar (init с `restartPolicy: Always`): работает всё время, как основной.
    pub sidecar: bool,
    pub requests: Amounts,
    pub limits: Amounts,
    /// Значения работающего пода, которых нет в шаблоне: их поставил `LimitRange` по умолчанию.
    /// Request ресурса, у которого в шаблоне есть limit, сюда не входит: его приравнивает к limit
    /// сам API-сервер ещё до `LimitRange`.
    pub limit_range_requests: Amounts,
    pub limit_range_limits: Amounts,
    /// Имена переменных окружения (значения не читаются): подсказки о JVM и Go (спец. §7.3).
    pub env_names: Vec<String>,
}

impl ContainerCurrent {
    /// Рекомендация считается: основной контейнер или нативный sidecar.
    pub fn recommended(&self) -> bool {
        !self.init || self.sidecar
    }

    /// Действующее значение: из шаблона, иначе от `LimitRange`, иначе limit шаблона (без request
    /// API-сервер ставит его равным limit).
    pub fn effective_requests(&self) -> Amounts {
        Amounts {
            cpu: self
                .requests
                .cpu
                .or(self.limit_range_requests.cpu)
                .or(self.limits.cpu),
            memory: self
                .requests
                .memory
                .or(self.limit_range_requests.memory)
                .or(self.limits.memory),
        }
    }

    pub fn effective_limits(&self) -> Amounts {
        Amounts {
            cpu: self.limits.cpu.or(self.limit_range_limits.cpu),
            memory: self.limits.memory.or(self.limit_range_limits.memory),
        }
    }
}

fn resources(c: &Container) -> (Amounts, Amounts) {
    let r = c.resources.as_ref();
    (
        Amounts::of(r.and_then(|r| r.requests.as_ref())),
        Amounts::of(r.and_then(|r| r.limits.as_ref())),
    )
}

/// Контейнер `name` первого пода, где он есть (работающие поды — первыми).
fn in_pods<'a>(pods: &'a [Pod], name: &str, init: bool) -> Option<&'a Container> {
    let mut sorted: Vec<&Pod> = pods.iter().collect();
    sorted.sort_by_key(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) != Some("Running"));
    sorted.into_iter().find_map(|p| {
        let spec = p.spec.as_ref()?;
        let list = if init {
            spec.init_containers.as_ref()?
        } else {
            &spec.containers
        };
        list.iter().find(|c| c.name == name)
    })
}

/// Контейнеры шаблона (сначала init) с текущими значениями.
pub fn current(template: Option<&PodSpec>, pods: &[Pod]) -> Vec<ContainerCurrent> {
    let Some(spec) = template else {
        return Vec::new();
    };
    let init = spec.init_containers.iter().flatten().map(|c| (c, true));
    let main = spec.containers.iter().map(|c| (c, false));
    init.chain(main)
        .map(|(c, init)| {
            let (requests, limits) = resources(c);
            let (pod_requests, pod_limits) = in_pods(pods, &c.name, init)
                .map(resources)
                .unwrap_or_default();
            ContainerCurrent {
                name: c.name.clone(),
                init,
                sidecar: init && c.restart_policy.as_deref() == Some("Always"),
                limit_range_requests: requests.missing_from(pod_requests).without(limits),
                limit_range_limits: limits.missing_from(pod_limits),
                requests,
                limits,
                env_names: c.env.iter().flatten().map(|e| e.name.clone()).collect(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(v: serde_json::Value) -> PodSpec {
        serde_json::from_value(v).unwrap()
    }

    fn pod(phase: &str, containers: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "p"}, "spec": {"containers": containers}, "status": {"phase": phase}
        }))
        .unwrap()
    }

    #[test]
    fn template_values_and_limit_range_defaults() {
        let t = spec(serde_json::json!({
            "initContainers": [
                {"name": "migrate", "image": "m"},
                {"name": "proxy", "image": "p", "restartPolicy": "Always",
                 "resources": {"requests": {"cpu": "50m"}}}
            ],
            "containers": [{"name": "app", "image": "a",
                "env": [{"name": "JAVA_OPTS", "value": "-Xmx1g"}],
                "resources": {"requests": {"cpu": "250m"}, "limits": {"cpu": "500m"}}}]
        }));
        let pods = [
            pod(
                "Pending",
                serde_json::json!([{"name": "app", "image": "a",
                "resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}]),
            ),
            pod(
                "Running",
                serde_json::json!([{"name": "app", "image": "a",
                "resources": {"requests": {"cpu": "250m", "memory": "256Mi"},
                              "limits": {"cpu": "500m", "memory": "512Mi"}}}]),
            ),
        ];
        // Память не задана ни request, ни limit: оба значения поставил LimitRange.
        let c = current(Some(&t), &pods);
        assert_eq!(
            c.iter()
                .map(|c| (c.name.as_str(), c.init, c.sidecar, c.recommended()))
                .collect::<Vec<_>>(),
            [
                ("migrate", true, false, false),
                ("proxy", true, true, true),
                ("app", false, false, true)
            ]
        );
        let app = &c[2];
        assert_eq!(
            app.requests,
            Amounts {
                cpu: Some(0.25),
                memory: None
            }
        );
        assert_eq!(
            app.limits,
            Amounts {
                cpu: Some(0.5),
                memory: None
            }
        );
        assert_eq!(
            app.limit_range_requests,
            Amounts {
                cpu: None,
                memory: Some(268_435_456.0)
            },
            "от работающего пода, а не от Pending"
        );
        assert_eq!(
            app.limit_range_limits,
            Amounts {
                cpu: None,
                memory: Some(536_870_912.0)
            }
        );
        assert_eq!(
            app.effective_requests(),
            Amounts {
                cpu: Some(0.25),
                memory: Some(268_435_456.0)
            }
        );
        assert_eq!(
            app.effective_limits(),
            Amounts {
                cpu: Some(0.5),
                memory: Some(536_870_912.0)
            }
        );
        assert_eq!(app.env_names, ["JAVA_OPTS"]);
        assert!(current(None, &pods).is_empty());
    }

    #[test]
    fn a_request_equal_to_the_limit_is_api_server_defaulting_not_limit_range() {
        let t = spec(serde_json::json!({
            "containers": [
                {"name": "capped", "image": "a",
                 "resources": {"limits": {"cpu": "1", "memory": "1Gi"}}},
                {"name": "mixed", "image": "a",
                 "resources": {"requests": {"cpu": "100m"}, "limits": {"memory": "512Mi"}}},
                {"name": "bare", "image": "a"}
            ]
        }));
        // Request без request в шаблоне API-сервер приравнивает к limit (SetDefaults_Pod) ещё до
        // LimitRange; значения «bare» поставил LimitRange.
        let pods = [pod(
            "Running",
            serde_json::json!([
                {"name": "capped", "image": "a", "resources": {
                    "requests": {"cpu": "1", "memory": "1Gi"},
                    "limits": {"cpu": "1", "memory": "1Gi"}}},
                {"name": "mixed", "image": "a", "resources": {
                    "requests": {"cpu": "100m", "memory": "512Mi"},
                    "limits": {"cpu": "2", "memory": "512Mi"}}},
                {"name": "bare", "image": "a", "resources": {
                    "requests": {"cpu": "200m", "memory": "128Mi"},
                    "limits": {"cpu": "1", "memory": "256Mi"}}}
            ]),
        )];
        let c = current(Some(&t), &pods);
        let by = |n: &str| c.iter().find(|c| c.name == n).unwrap();
        let both = |cpu, memory| Amounts {
            cpu: Some(cpu),
            memory: Some(memory),
        };

        let capped = by("capped");
        assert_eq!(capped.limit_range_requests, Amounts::default());
        assert_eq!(capped.limit_range_limits, Amounts::default());
        assert_eq!(capped.requests, Amounts::default(), "шаблон не меняется");
        assert_eq!(capped.effective_requests(), both(1.0, 1_073_741_824.0));
        assert_eq!(capped.effective_limits(), both(1.0, 1_073_741_824.0));

        // CPU: request в шаблоне; память: request от limit. LimitRange поставил только limit CPU.
        let mixed = by("mixed");
        assert_eq!(mixed.limit_range_requests, Amounts::default());
        assert_eq!(
            mixed.limit_range_limits,
            Amounts {
                cpu: Some(2.0),
                memory: None
            }
        );
        assert_eq!(mixed.effective_requests(), both(0.1, 536_870_912.0));
        assert_eq!(mixed.effective_limits(), both(2.0, 536_870_912.0));

        let bare = by("bare");
        assert_eq!(bare.limit_range_requests, both(0.2, 134_217_728.0));
        assert_eq!(bare.limit_range_limits, both(1.0, 268_435_456.0));
        assert_eq!(bare.effective_requests(), both(0.2, 134_217_728.0));
        assert_eq!(bare.effective_limits(), both(1.0, 268_435_456.0));
    }
}
