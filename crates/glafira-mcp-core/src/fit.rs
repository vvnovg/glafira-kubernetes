//! Списки в ответах (спец. §10.4, §10.7): фильтр по подстроке, страницы, предел размера ответа.

use serde::Serialize;

/// Предел размера ответа по умолчанию. Клиенту с малым контекстом (локальная модель) пользователь
/// задаёт меньший — [`RESPONSE_LIMITS_KB`]; инструмент узнаёт его из [`response_limit`].
pub const MAX_RESPONSE_BYTES: usize = 100 * 1024;
/// Пределы на выбор в настройках MCP, КБ.
pub const RESPONSE_LIMITS_KB: [usize; 3] = [100, 48, 24];
/// Страховка в обвязке инструментов: ответ больше предела и этого запаса (на `truncated` и
/// `hint`) не отправляется — обрезка списков не справилась.
pub const HARD_LIMIT_MARGIN: usize = 1024;

tokio::task_local! {
    static RESPONSE_LIMIT: usize;
}

/// Предел размера ответа текущего вызова (обвязка задаёт его через [`with_response_limit`]); вне
/// обвязки — [`MAX_RESPONSE_BYTES`].
pub fn response_limit() -> usize {
    RESPONSE_LIMIT
        .try_with(|l| *l)
        .unwrap_or(MAX_RESPONSE_BYTES)
}

/// Выполняет `f` с пределом размера ответа `limit`.
pub async fn with_response_limit<F: std::future::Future>(limit: usize, f: F) -> F::Output {
    RESPONSE_LIMIT.scope(limit, f).await
}
pub const DEFAULT_LIMIT: usize = 200;
pub const MAX_LIMIT: usize = 500;

/// Подстрока без учёта регистра; пустой фильтр пропускает всё.
pub fn matches_filter(name: &str, filter: Option<&str>) -> bool {
    match filter.map(str::trim).filter(|f| !f.is_empty()) {
        None => true,
        Some(f) => name.to_lowercase().contains(&f.to_lowercase()),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Page<T> {
    pub total: usize,
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<usize>,
}

pub fn page<T>(items: Vec<T>, offset: usize, limit: usize) -> Page<T> {
    let total = items.len();
    let items: Vec<T> = items.into_iter().skip(offset).take(limit).collect();
    let end = offset.saturating_add(items.len());
    Page {
        total,
        next_offset: (end < total).then_some(end),
        items,
    }
}

/// `limit` в 1..=MAX_LIMIT (по умолчанию DEFAULT_LIMIT).
pub fn checked_limit(limit: Option<u32>) -> Result<usize, String> {
    match limit {
        None => Ok(DEFAULT_LIMIT),
        Some(l) if (1..=MAX_LIMIT as u32).contains(&l) => Ok(l as usize),
        Some(l) => Err(format!("limit must be between 1 and {MAX_LIMIT}, got {l}")),
    }
}

/// Обрезает главный список ответа, пока JSON не уложится в предел вызова ([`response_limit`]).
/// Возвращает true, если что-то обрезано.
pub fn fit<T: Serialize, I>(value: &mut T, list: fn(&mut T) -> &mut Vec<I>) -> bool {
    let limit = response_limit();
    let mut truncated = false;
    loop {
        let size = serde_json::to_vec(&*value).map_or(usize::MAX, |v| v.len());
        if size <= limit {
            return truncated;
        }
        let l = list(value);
        if l.is_empty() {
            return truncated;
        }
        let keep = (l.len() * limit / size).min(l.len() - 1);
        l.truncate(keep);
        truncated = true;
    }
}

pub const TRUNCATED_HINT: &str =
    "The response was truncated to the size limit: narrow the filter or page with offset";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_and_pages() {
        assert!(matches_filter("Orders.Events", Some("orders")));
        assert!(matches_filter("x", Some("  ")));
        assert!(!matches_filter("payments", Some("orders")));
        let p = page((0..10).collect::<Vec<_>>(), 4, 3);
        assert_eq!(
            (p.total, p.items, p.next_offset),
            (10, vec![4, 5, 6], Some(7))
        );
        let last = page((0..10).collect::<Vec<_>>(), 8, 3);
        assert_eq!((last.items, last.next_offset), (vec![8, 9], None));
        assert_eq!(checked_limit(None), Ok(DEFAULT_LIMIT));
        assert!(checked_limit(Some(0)).is_err());
        assert!(checked_limit(Some(501)).is_err());
    }

    #[derive(Serialize)]
    struct Big {
        items: Vec<String>,
    }

    fn items(b: &mut Big) -> &mut Vec<String> {
        &mut b.items
    }

    #[test]
    fn fit_truncates_to_limit() {
        let mut small = Big {
            items: vec!["a".into(); 10],
        };
        assert!(!fit(&mut small, items));
        assert_eq!(small.items.len(), 10);
        let mut big = Big {
            items: vec!["x".repeat(1000); 500],
        };
        assert!(fit(&mut big, items));
        let size = serde_json::to_vec(&big).unwrap().len();
        assert!(size <= MAX_RESPONSE_BYTES && big.items.len() > 50, "{size}");
    }
    #[tokio::test]
    async fn fit_follows_the_limit_of_the_call() {
        let big = || Big {
            items: (0..2000)
                .map(|i| format!("item-{i:05}-{}", "x".repeat(40)))
                .collect(),
        };
        let mut wide = big();
        assert!(fit(&mut wide, items));
        let mut narrow = big();
        let limited = with_response_limit(24 * 1024, async move {
            assert_eq!(response_limit(), 24 * 1024);
            assert!(fit(&mut narrow, items));
            narrow
        })
        .await;
        let size = |b: &Big| serde_json::to_vec(b).unwrap().len();
        assert!(size(&limited) <= 24 * 1024, "{}", size(&limited));
        assert!(size(&wide) > 90 * 1024 && size(&wide) <= MAX_RESPONSE_BYTES);
        assert_eq!(response_limit(), MAX_RESPONSE_BYTES);
        assert_eq!(RESPONSE_LIMITS_KB, [100, 48, 24]);
    }
}
