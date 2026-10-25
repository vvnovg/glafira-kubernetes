//! Селекторы меток: строкой для `labelSelector` и проверкой меток объекта (PDB, Service).

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;

/// Селектор строкой, как его понимает `labelSelector`: `app=web,tier in (a,b),!legacy`.
/// Пустой селектор — пустая строка (подходит всё).
pub fn selector_string(s: &LabelSelector) -> String {
    let mut parts: Vec<String> = s
        .match_labels
        .iter()
        .flatten()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    for e in s.match_expressions.iter().flatten() {
        let values = e.values.clone().unwrap_or_default().join(",");
        parts.push(match e.operator.as_str() {
            "In" => format!("{} in ({values})", e.key),
            "NotIn" => format!("{} notin ({values})", e.key),
            "Exists" => e.key.clone(),
            "DoesNotExist" => format!("!{}", e.key),
            other => format!("{} {other} ({values})", e.key),
        });
    }
    parts.join(",")
}

/// Селектор из пар `ключ=значение` (Service, ReplicationController, DeploymentConfig).
pub fn map_selector_string(m: &BTreeMap<String, String>) -> String {
    m.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Подходят ли метки под селектор. Пустой селектор подходит всем; неизвестный оператор — никому.
pub fn selector_matches(s: &LabelSelector, labels: &BTreeMap<String, String>) -> bool {
    let labels_ok = s
        .match_labels
        .iter()
        .flatten()
        .all(|(k, v)| labels.get(k) == Some(v));
    let expressions_ok = s.match_expressions.iter().flatten().all(|e| {
        let values = e.values.clone().unwrap_or_default();
        let value = labels.get(&e.key);
        match e.operator.as_str() {
            "In" => value.is_some_and(|v| values.contains(v)),
            "NotIn" => value.is_none_or(|v| !values.contains(v)),
            "Exists" => value.is_some(),
            "DoesNotExist" => value.is_none(),
            _ => false,
        }
    });
    labels_ok && expressions_ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

    fn selector() -> LabelSelector {
        let req = |key: &str, op: &str, values: &[&str]| LabelSelectorRequirement {
            key: key.into(),
            operator: op.into(),
            values: (!values.is_empty()).then(|| values.iter().map(|v| v.to_string()).collect()),
        };
        LabelSelector {
            match_labels: Some(BTreeMap::from([("app".to_string(), "web".to_string())])),
            match_expressions: Some(vec![
                req("tier", "In", &["a", "b"]),
                req("env", "NotIn", &["prod"]),
                req("team", "Exists", &[]),
                req("legacy", "DoesNotExist", &[]),
            ]),
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn selector_as_a_string() {
        assert_eq!(
            selector_string(&selector()),
            "app=web,tier in (a,b),env notin (prod),team,!legacy"
        );
        assert_eq!(selector_string(&LabelSelector::default()), "");
        assert_eq!(
            map_selector_string(&labels(&[("app", "web"), ("deploymentconfig", "web")])),
            "app=web,deploymentconfig=web"
        );
    }

    #[test]
    fn labels_against_a_selector() {
        let s = selector();
        assert!(selector_matches(
            &s,
            &labels(&[("app", "web"), ("tier", "a"), ("team", "x")])
        ));
        assert!(!selector_matches(
            &s,
            &labels(&[("app", "web"), ("tier", "c"), ("team", "x")])
        ));
        assert!(!selector_matches(
            &s,
            &labels(&[
                ("app", "web"),
                ("tier", "a"),
                ("team", "x"),
                ("env", "prod")
            ])
        ));
        assert!(!selector_matches(
            &s,
            &labels(&[
                ("app", "web"),
                ("tier", "a"),
                ("team", "x"),
                ("legacy", "1")
            ])
        ));
        assert!(selector_matches(&LabelSelector::default(), &labels(&[])));
    }
}
