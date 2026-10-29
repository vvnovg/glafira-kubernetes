//! Поддельный Prometheus стенда kind (`dev/kind/metrics.yaml`): HTTP-сервер теста на рабочей машине,
//! к которому прокси API-сервера ходит через под-заглушку и перенаправление `setup.sh`. Отвечает
//! записанными ответами Prometheus HTTP API по тексту запроса.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Порт поддельного Prometheus на рабочей машине: на него ведёт перенаправление стенда.
pub const PORT: u16 = 19090;
pub const CPU_P95: f64 = 0.012;
pub const MEMORY_MAX: f64 = 30.0 * 1024.0 * 1024.0;
pub const THROTTLING: f64 = 0.4;

/// Ответ Prometheus HTTP API по тексту запроса: контейнер app — p95 CPU 12m, максимум памяти
/// 30 Mi при лимите 32 Mi, троттлинг 40 %, данные за всё окно; запросы проекта — по подам `pods`.
fn answer(path: &str, p: &HashMap<String, String>, pods: &[String]) -> Value {
    let q = p.get("query").map_or("", String::as_str);
    let num = |k: &str| p.get(k).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    if path == "/api/v1/query_range" {
        let (start, end) = (num("start"), num("end"));
        let series = |v: f64| {
            json!([{"metric": {"container": "app"},
                "values": [[start, v.to_string()], [end, v.to_string()]]}])
        };
        let result = if q.contains("\"event\"") {
            json!([])
        } else if q.contains("container_memory_working_set_bytes") {
            series(MEMORY_MAX)
        } else {
            series(CPU_P95)
        };
        return json!({"status": "success", "data": {"resultType": "matrix", "result": result}});
    }
    let t = num("time");
    let sample = |metric: Value, v: f64| json!({"metric": metric, "value": [t, v.to_string()]});
    let stats = |list: &[(&str, f64)]| -> Vec<Value> {
        list.iter()
            .map(|(s, v)| sample(json!({"container": "app", "stat": s}), *v))
            .collect()
    };
    let per_pod = |v: f64| -> Vec<Value> {
        pods.iter()
            .map(|pod| sample(json!({"pod": pod, "container": "app"}), v))
            .collect()
    };
    let mi = 1024.0 * 1024.0;
    let result = if q == "vector(1)" {
        vec![sample(json!({}), 1.0)]
    } else if q.starts_with("count(kube_pod_owner") {
        vec![sample(json!({}), 3.0)]
    } else if q.contains("\"cpu_p50\"") {
        stats(&[
            ("cpu_p50", 0.008),
            ("cpu_p95", CPU_P95),
            ("cpu_p99", 0.015),
            ("cpu_max", 0.02),
        ])
    } else if q.contains("\"memory_p95\"") {
        stats(&[
            ("memory_p95", 20.0 * mi),
            ("memory_p99", 22.0 * mi),
            ("memory_max", MEMORY_MAX),
        ])
    } else if q.contains("\"throttling_mean\"") {
        stats(&[("throttling_mean", THROTTLING), ("throttling_p95", 0.6)])
    } else if q.contains("\"restarts\"") {
        stats(&[("oom_count", 0.0), ("restarts", 0.0)])
    } else if q.contains("\"steps\"") {
        vec![
            sample(json!({"stat": "steps"}), 1e6),
            sample(json!({"stat": "first"}), t - 31.0 * 86_400.0),
        ]
    } else if q.contains("container_cpu_cfs_throttled_periods_total") {
        per_pod(THROTTLING)
    } else if q.contains("quantile_over_time(0.95") {
        per_pod(CPU_P95)
    } else if q.contains("container_memory_working_set_bytes") {
        per_pod(MEMORY_MAX)
    } else {
        Vec::new()
    };
    json!({"status": "success", "data": {"resultType": "vector", "result": result}})
}

/// Отказ в форме ошибки Prometheus HTTP API: код, строка статуса и тело.
fn failure(code: u16, error: &str) -> (u16, &'static str, Value) {
    let text = if code == 404 {
        "Not Found"
    } else {
        "Method Not Allowed"
    };
    (
        code,
        text,
        json!({"status": "error", "errorType": "bad_data", "error": error}),
    )
}

/// Что стенд отвечает на запрос: только GET `/api/v1/query` и `/api/v1/query_range` — как делает
/// источник метрик Glafira (`metrics/http.rs`); остальное — 405 для не-GET и 404 для других путей.
/// Прокси API-сервера отдаёт заглушке путь после `/proxy` как есть, без своего префикса.
fn respond(method: &str, path: &str, query: &str, pods: &[String]) -> (u16, &'static str, Value) {
    if method != "GET" {
        return failure(405, &format!("{method} не разрешён: только GET"));
    }
    if !matches!(path, "/api/v1/query" | "/api/v1/query_range") {
        return failure(404, &format!("нет пути {path}"));
    }
    let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    (200, "OK", answer(path, &params, pods))
}

/// Поддельный Prometheus рядом с kubeconfig стенда: по запросу на соединение. На Linux адрес из
/// `host-address` — шлюз сети kind, то есть адрес этой машины; Docker Desktop доставляет
/// host.docker.internal на её loopback, а самого адреса у машины нет: тогда сервер слушает loopback.
/// Любая другая ошибка привязки (в первую очередь «адрес занят») — остановка теста с портом в тексте.
pub async fn serve(kubeconfig: &Path, pods: Arc<Mutex<Vec<String>>>) {
    let file = kubeconfig.with_file_name("host-address");
    let host = std::fs::read_to_string(&file).unwrap_or_else(|e| {
        panic!(
            "нет {}: {e}; запустите dev/kind/setup.sh (он пишет адрес рабочей машины)",
            file.display()
        )
    });
    let host = host.trim();
    let listener = match TcpListener::bind((host, PORT)).await {
        Ok(l) => l,
        Err(e) if e.kind() == ErrorKind::AddrNotAvailable => TcpListener::bind(("127.0.0.1", PORT))
            .await
            .unwrap_or_else(|e| {
                panic!("порт {PORT} на 127.0.0.1 для поддельного Prometheus недоступен: {e}")
            }),
        Err(e) => panic!(
            "порт {PORT} на {host} для поддельного Prometheus недоступен: {e}; освободите его (lsof -nP -iTCP:{PORT})"
        ),
    };
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let pods = pods.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&buf);
                let mut words = head.split_whitespace();
                let method = words.next().unwrap_or("");
                let target = words.next().unwrap_or("/");
                let (path, query) = target.split_once('?').unwrap_or((target, ""));
                let (code, text, body) = respond(method, path, query, &pods.lock().unwrap());
                let body = body.to_string();
                let allow = if code == 405 { "Allow: GET\r\n" } else { "" };
                let reply = format!(
                    "HTTP/1.1 {code} {text}\r\nContent-Type: application/json\r\n{allow}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(reply.as_bytes()).await;
            });
        }
    });
}
