//! Поддельный Prometheus стенда kind (`dev/kind/metrics.yaml`): HTTP-сервер теста на рабочей машине,
//! к которому прокси API-сервера ходит через под-заглушку и перенаправление `setup.sh`. Отвечает
//! записанными ответами Prometheus HTTP API по тексту запроса.

use std::collections::HashMap;
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
    if path.ends_with("/query_range") {
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

/// Поддельный Prometheus рядом с kubeconfig стенда: по запросу на соединение. На Linux адрес из
/// `host-address` — шлюз сети kind, то есть адрес этой машины; Docker Desktop доставляет
/// host.docker.internal на её loopback.
pub async fn serve(kubeconfig: &Path, pods: Arc<Mutex<Vec<String>>>) {
    let host = std::fs::read_to_string(kubeconfig.with_file_name("host-address")).unwrap();
    let listener = match TcpListener::bind((host.trim(), PORT)).await {
        Ok(l) => l,
        Err(_) => TcpListener::bind(("127.0.0.1", PORT)).await.unwrap(),
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
                let target = head.split_whitespace().nth(1).unwrap_or("/");
                let (path, query) = target.split_once('?').unwrap_or((target, ""));
                let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
                    .into_owned()
                    .collect();
                let body = answer(path, &params, &pods.lock().unwrap()).to_string();
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(reply.as_bytes()).await;
            });
        }
    });
}
