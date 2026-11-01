//! Схемы параметров инструментов для LLM: подсхемы `$defs` подставляются вместо ссылок `$ref`.
//! Парсеры вызовов инструментов у локальных серверов моделей (vLLM `qwen3_coder`) приводят типы
//! аргументов по схеме, но ссылок не раскрывают: объект, описанный одной ссылкой, приходит
//! строкой с JSON, и вызов отклоняется.

use serde_json::{Map, Value};

/// Схема без `$ref` и `$defs`. Соседние со ссылкой ключи (`description`) остаются поверх
/// подставленной подсхемы. Рекурсивную схему подставить нельзя — она возвращается как есть.
pub fn inline_refs(schema: &Map<String, Value>) -> Map<String, Value> {
    let defs = match schema.get("$defs") {
        Some(Value::Object(defs)) => defs,
        _ => return schema.clone(),
    };
    let mut path = Vec::new();
    match inline(&Value::Object(schema.clone()), defs, &mut path) {
        Some(Value::Object(mut out)) => {
            out.remove("$defs");
            out
        }
        _ => schema.clone(),
    }
}

/// `None` — ссылка на неизвестную подсхему или цикл.
fn inline<'a>(
    node: &Value,
    defs: &'a Map<String, Value>,
    path: &mut Vec<&'a str>,
) -> Option<Value> {
    match node {
        Value::Object(obj) => {
            let mut out = match obj.get("$ref").and_then(Value::as_str) {
                Some(r) => {
                    let (name, def) = defs.get_key_value(r.strip_prefix("#/$defs/")?)?;
                    if path.contains(&name.as_str()) {
                        return None;
                    }
                    path.push(name);
                    let target = inline(def, defs, path);
                    path.pop();
                    match target? {
                        Value::Object(target) => target,
                        _ => return None,
                    }
                }
                None => Map::new(),
            };
            for (k, v) in obj {
                if k != "$ref" && k != "$defs" {
                    out.insert(k.clone(), inline(v, defs, path)?);
                }
            }
            Some(Value::Object(out))
        }
        Value::Array(items) => items
            .iter()
            .map(|v| inline(v, defs, path))
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        other => Some(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn nested_refs_are_replaced_by_their_definitions() {
        let schema = obj(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {
                "Kind": {"type": "string", "enum": ["Deployment", "Job"]},
                "Ref": {
                    "type": "object",
                    "properties": {"kind": {"$ref": "#/$defs/Kind", "description": "Kind."}},
                    "required": ["kind"]
                }
            },
            "type": "object",
            "properties": {
                "workload": {"$ref": "#/$defs/Ref"},
                "many": {"type": "array", "items": {"$ref": "#/$defs/Kind"}},
                "maybe": {"anyOf": [{"$ref": "#/$defs/Ref"}, {"type": "null"}]}
            }
        }));
        let ref_schema = json!({
            "type": "object",
            "properties": {
                "kind": {"type": "string", "enum": ["Deployment", "Job"], "description": "Kind."}
            },
            "required": ["kind"]
        });
        assert_eq!(
            Value::Object(inline_refs(&schema)),
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "workload": ref_schema,
                    "many": {"type": "array", "items": {"type": "string", "enum": ["Deployment", "Job"]}},
                    "maybe": {"anyOf": [ref_schema, {"type": "null"}]}
                }
            })
        );
    }

    #[test]
    fn schema_without_defs_is_unchanged() {
        let schema = obj(json!({"type": "object", "properties": {"name": {"type": "string"}}}));
        assert_eq!(inline_refs(&schema), schema);
    }

    #[test]
    fn recursive_or_dangling_schema_is_returned_as_is() {
        let recursive = obj(json!({
            "$defs": {"Node": {"type": "object", "properties": {"next": {"$ref": "#/$defs/Node"}}}},
            "type": "object",
            "properties": {"root": {"$ref": "#/$defs/Node"}}
        }));
        assert_eq!(inline_refs(&recursive), recursive);
        let dangling = obj(json!({
            "$defs": {},
            "type": "object",
            "properties": {"root": {"$ref": "#/$defs/Missing"}}
        }));
        assert_eq!(inline_refs(&dangling), dangling);
    }
}
