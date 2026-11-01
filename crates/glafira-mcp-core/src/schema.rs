//! Схемы параметров инструментов для LLM: подсхемы `$defs` подставляются вместо ссылок `$ref`.
//! Парсеры вызовов инструментов у локальных серверов моделей (vLLM `qwen3_coder`) приводят типы
//! аргументов по схеме, но ссылок не раскрывают: объект, описанный одной ссылкой, приходит
//! строкой с JSON, и вызов отклоняется. Здесь же — терпимый разбор аргументов и подсказка об их форме.

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

/// Приводит аргументы к схеме без `$ref`, не меняя смысла: строка с JSON-объектом или массивом там,
/// где схема строку не допускает, становится значением (парсер `qwen3_coder` присылает так вложенные
/// объекты); `null` и пустой ключ убираются (поле с `null` — то же, что отсутствующее, у обязательного
/// поля ошибка разбора остаётся). Остальные лишние ключи не трогаются: их пропускает serde.
pub fn normalize_args(args: &mut Map<String, Value>, schema: &Map<String, Value>) {
    for (key, value) in args.iter_mut() {
        if let Some(prop) = property(schema, key) {
            normalize(value, prop);
        }
    }
    args.retain(|k, v| !k.is_empty() && !v.is_null());
}

fn normalize(value: &mut Value, schema: &Map<String, Value>) {
    if let Value::String(text) = value
        && !allows_string(schema)
        && let Ok(parsed @ (Value::Object(_) | Value::Array(_))) = serde_json::from_str(text)
    {
        *value = parsed;
    }
    match value {
        Value::Object(obj) => normalize_args(obj, schema),
        Value::Array(items) => {
            if let Some(Value::Object(item)) = schema.get("items") {
                items.iter_mut().for_each(|i| normalize(i, item));
            }
        }
        _ => {}
    }
}

/// Схема поля объекта: в `properties` самой схемы или одной из ветвей `anyOf`/`oneOf`/`allOf`.
fn property<'a>(schema: &'a Map<String, Value>, key: &str) -> Option<&'a Map<String, Value>> {
    if let Some(p) = schema
        .get("properties")
        .and_then(|p| p.get(key))
        .and_then(Value::as_object)
    {
        return Some(p);
    }
    branches(schema).find_map(|b| property(b, key))
}

fn branches(schema: &Map<String, Value>) -> impl Iterator<Item = &Map<String, Value>> {
    ["anyOf", "oneOf", "allOf"]
        .into_iter()
        .filter_map(|k| schema.get(k).and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_object)
}

/// Схема без типа (и без ветвей) допускает что угодно, в том числе строку.
fn allows_string(schema: &Map<String, Value>) -> bool {
    match schema.get("type") {
        Some(Value::String(t)) => t == "string",
        Some(Value::Array(ts)) => ts.iter().any(|t| t == "string"),
        _ => {
            let mut any = branches(schema).peekable();
            any.peek().is_none() || any.any(allows_string)
        }
    }
}

/// Набросок аргументов по схеме без `$ref` для подсказки при ошибке разбора:
/// `{"connectionId": "string", "limit?": "integer"}` — необязательный ключ с `?`, перечисление через
/// `|`, скалярные варианты через ` | `, варианты-объекты — `{"oneOf": […]}`, массив — `[элемент]`.
pub fn shape(schema: &Map<String, Value>) -> Value {
    if let Some(Value::Object(props)) = schema.get("properties") {
        let required: Vec<&str> = schema
            .get("required")
            .and_then(Value::as_array)
            .map(|r| r.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let fields = props.iter().filter_map(|(k, v)| {
            let name = if required.contains(&k.as_str()) {
                k.clone()
            } else {
                format!("{k}?")
            };
            Some((name, shape(v.as_object()?)))
        });
        return Value::Object(fields.collect());
    }
    if let Some(Value::Array(values)) = schema.get("enum") {
        let names: Vec<String> = values
            .iter()
            .map(|v| v.as_str().map_or(v.to_string(), str::to_owned))
            .collect();
        return Value::String(names.join("|"));
    }
    if let Some(c) = schema.get("const") {
        return c.clone();
    }
    let alternatives: Vec<Value> = branches(schema)
        .filter(|b| b.get("type").and_then(Value::as_str) != Some("null"))
        .map(shape)
        .collect();
    match alternatives.len() {
        0 => {}
        1 => return alternatives.into_iter().next().unwrap_or_default(),
        _ if alternatives.iter().all(Value::is_string) => {
            let names: Vec<&str> = alternatives.iter().filter_map(Value::as_str).collect();
            return Value::String(names.join(" | "));
        }
        _ => return serde_json::json!({ "oneOf": alternatives }),
    }
    match schema.get("type") {
        Some(Value::String(t)) if t == "array" => {
            let item = schema
                .get("items")
                .and_then(Value::as_object)
                .map(shape)
                .unwrap_or_default();
            Value::Array(vec![item])
        }
        Some(Value::String(t)) => Value::String(t.clone()),
        Some(Value::Array(ts)) => {
            let names: Vec<&str> = ts
                .iter()
                .filter_map(Value::as_str)
                .filter(|t| *t != "null")
                .collect();
            Value::String(names.join(" | "))
        }
        _ => Value::String("any".into()),
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

    fn search_schema() -> Map<String, Value> {
        obj(json!({
            "type": "object",
            "properties": {
                "connectionId": {"type": "string"},
                "workload": {
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "enum": ["Deployment", "Job"]},
                        "name": {"type": "string"}
                    },
                    "required": ["kind", "name"]
                },
                "filter": {
                    "type": "object",
                    "properties": {
                        "contains": {"type": ["string", "null"]},
                        "caseSensitive": {"type": ["boolean", "null"]}
                    }
                },
                "targets": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"name": {"type": "string"}, "ref": {"type": "object"}}}
                },
                "change": {
                    "anyOf": [
                        {"type": "object", "properties": {"setResources": {"type": "object", "properties": {"limits": {"type": "object"}}}}},
                        {"type": "object", "properties": {"scale": {"type": "integer"}}}
                    ]
                },
                "note": {"type": "string"},
                "limit": {"type": ["integer", "null"]}
            },
            "required": ["connectionId", "workload"]
        }))
    }

    #[test]
    fn json_strings_become_objects_where_the_schema_wants_no_string() {
        let mut args = obj(json!({
            "connectionId": "c1",
            "workload": "{\"kind\": \"Deployment\", \"name\": \"web\"}",
            "filter": {"contains": "boom"},
            "targets": "[{\"name\": \"a\", \"ref\": \"{\\\"x\\\": 1}\"}]",
            "change": "{\"setResources\": \"{\\\"limits\\\": {}}\"}",
            "note": "{\"kept\": true}"
        }));
        normalize_args(&mut args, &search_schema());
        assert_eq!(
            Value::Object(args),
            json!({
                "connectionId": "c1",
                "workload": {"kind": "Deployment", "name": "web"},
                "filter": {"contains": "boom"},
                "targets": [{"name": "a", "ref": {"x": 1}}],
                "change": {"setResources": {"limits": {}}},
                "note": "{\"kept\": true}"
            })
        );
    }

    #[test]
    fn nulls_empty_keys_and_bad_json_are_handled() {
        let mut args = obj(json!({
            "connectionId": "c1",
            "": "",
            "limit": null,
            "workload": "{not json",
            "filter": {"contains": null, "caseSensitive": true},
            "unknown": 1
        }));
        normalize_args(&mut args, &search_schema());
        assert_eq!(
            Value::Object(args),
            json!({
                "connectionId": "c1",
                "workload": "{not json",
                "filter": {"caseSensitive": true},
                "unknown": 1
            })
        );
    }

    #[test]
    fn shape_sketches_the_arguments() {
        assert_eq!(
            shape(&search_schema()),
            json!({
                "change?": {"oneOf": [{"setResources?": {"limits?": "object"}}, {"scale?": "integer"}]},
                "connectionId": "string",
                "filter?": {"caseSensitive?": "boolean", "contains?": "string"},
                "limit?": "integer",
                "note?": "string",
                "targets?": [{"name?": "string", "ref?": "object"}],
                "workload": {"kind": "Deployment|Job", "name": "string"}
            })
        );
        let scalars =
            obj(json!({"anyOf": [{"type": "string"}, {"type": "integer"}, {"type": "null"}]}));
        assert_eq!(shape(&scalars), json!("string | integer"));
        assert_eq!(shape(&obj(json!({}))), json!("any"));
    }
}
