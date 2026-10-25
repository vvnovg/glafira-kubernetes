//! Общее в ответах чтения: часть экрана (`Part`), условия объектов, время. Время уходит во
//! фронтенд секундами Unix.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use serde::Serialize;

use crate::error::{AppError, AppResult};

/// Часть экрана: данные или причина, почему их нет, — права или ошибка чтения (спец. §15,
/// «Частичные результаты»). Остальной экран показывается.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Part<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

impl<T> Part<T> {
    pub fn ok(data: T) -> Self {
        Self {
            data: Some(data),
            error: None,
        }
    }

    pub fn err(error: AppError) -> Self {
        Self {
            data: None,
            error: Some(error),
        }
    }
}

impl<T> From<AppResult<T>> for Part<T> {
    fn from(r: AppResult<T>) -> Self {
        match r {
            Ok(t) => Self::ok(t),
            Err(e) => Self::err(e),
        }
    }
}

/// Условие объекта (`status.conditions[]`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionView {
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition: Option<i64>,
}

impl ConditionView {
    pub fn is_true(&self) -> bool {
        self.status == "True"
    }
}

/// Условия из JSON (`status.conditions` любого вида объекта — типизированного или из
/// `DynamicObject`): одинаковые поля у всех.
pub fn conditions(v: &serde_json::Value) -> Vec<ConditionView> {
    let text = |c: &serde_json::Value, k: &str| c[k].as_str().map(String::from);
    v.as_array()
        .map(|list| {
            list.iter()
                .filter_map(|c| {
                    Some(ConditionView {
                        kind: text(c, "type")?,
                        status: text(c, "status").unwrap_or_default(),
                        reason: text(c, "reason").filter(|r| !r.is_empty()),
                        message: text(c, "message").filter(|m| !m.is_empty()),
                        last_transition: c["lastTransitionTime"].as_str().and_then(rfc3339),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Условия типизированного объекта: через JSON, как у `DynamicObject`.
pub fn conditions_of<T: Serialize>(list: &Option<Vec<T>>) -> Vec<ConditionView> {
    match list {
        Some(l) => conditions(&serde_json::to_value(l).unwrap_or_default()),
        None => Vec::new(),
    }
}

/// Условие вида `kind`.
pub fn condition<'a>(list: &'a [ConditionView], kind: &str) -> Option<&'a ConditionView> {
    list.iter().find(|c| c.kind == kind)
}

/// Значение условия как `bool`: `True` / `False`, иначе (`Unknown`, нет условия) — `None`.
pub fn condition_flag(list: &[ConditionView], kind: &str) -> Option<bool> {
    condition(list, kind).and_then(|c| match c.status.as_str() {
        "True" => Some(true),
        "False" => Some(false),
        _ => None,
    })
}

/// Секунды Unix из времени Kubernetes.
pub fn seconds(t: &Time) -> i64 {
    t.0.as_second()
}

/// Когда создан объект.
pub fn created(meta: &ObjectMeta) -> Option<i64> {
    meta.creation_timestamp.as_ref().map(seconds)
}

/// Секунды Unix из строки RFC 3339 (время в JSON объектов без типа).
pub fn rfc3339(s: &str) -> Option<i64> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|t| t.unix_timestamp())
}

/// Имя объекта (пустое, если его нет).
pub fn name(meta: &ObjectMeta) -> String {
    meta.name.clone().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditions_from_any_object() {
        let v = serde_json::json!([
            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-10-25T10:00:00Z"},
            {"type": "MemoryPressure", "status": "False", "reason": "", "message": "ok"},
            {"status": "True"}
        ]);
        let c = conditions(&v);
        assert_eq!(c.len(), 2, "условие без типа пропускается");
        assert_eq!(c[0].last_transition, Some(1_792_922_400));
        assert!(c[0].is_true());
        assert_eq!(c[1].reason, None, "пустая причина — нет причины");
        assert_eq!(c[1].message.as_deref(), Some("ok"));
        assert_eq!(condition_flag(&c, "Ready"), Some(true));
        assert_eq!(condition_flag(&c, "MemoryPressure"), Some(false));
        assert_eq!(condition_flag(&c, "DiskPressure"), None);
        assert!(conditions(&serde_json::Value::Null).is_empty());
    }

    #[test]
    fn parts_serialize_data_or_error() {
        let ok: Part<u32> = Ok(3).into();
        assert_eq!(
            serde_json::to_value(&ok).unwrap(),
            serde_json::json!({"data": 3})
        );
        let err: Part<u32> = Err(AppError::internal_with("x")).into();
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(v["error"]["key"], "errors.internal");
        assert!(v.get("data").is_none());
    }

    #[test]
    fn times() {
        assert_eq!(rfc3339("1970-01-01T00:01:00Z"), Some(60));
        assert_eq!(rfc3339("вчера"), None);
        let meta = ObjectMeta {
            creation_timestamp: Some(Time("2026-10-25T10:00:00Z".parse().unwrap())),
            ..Default::default()
        };
        assert_eq!(created(&meta), Some(1_792_922_400));
    }
}
