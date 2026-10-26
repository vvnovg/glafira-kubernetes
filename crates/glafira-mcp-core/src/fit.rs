//! Списки в ответах (спец. §10.4, §10.7): фильтр по подстроке, страницы, предел размера ответа.

use serde::Serialize;

pub const MAX_RESPONSE_BYTES: usize = 100 * 1024;
/// Страховка в обвязке инструментов: ответ больше этого (предел + запас на `truncated` и
/// `hint`) не отправляется — обрезка списков не справилась.
pub const HARD_LIMIT_BYTES: usize = MAX_RESPONSE_BYTES + 1024;
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

/// Обрезает главный список ответа, пока JSON не уложится в [`MAX_RESPONSE_BYTES`].
/// Возвращает true, если что-то обрезано.
pub fn fit<T: Serialize, I>(value: &mut T, list: fn(&mut T) -> &mut Vec<I>) -> bool {
    let mut truncated = false;
    loop {
        let size = serde_json::to_vec(&*value).map_or(usize::MAX, |v| v.len());
        if size <= MAX_RESPONSE_BYTES {
            return truncated;
        }
        let l = list(value);
        if l.is_empty() {
            return truncated;
        }
        let keep = (l.len() * MAX_RESPONSE_BYTES / size).min(l.len() - 1);
        l.truncate(keep);
        truncated = true;
    }
}

pub const TRUNCATED_HINT: &str =
    "The response was truncated to 100 KB: narrow the filter or page with offset";

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
}
