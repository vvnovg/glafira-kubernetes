//! Quantity Kubernetes числом: `500m` → 0,5, `128Mi` → 134 217 728, `2e3` → 2000. Нужно,
//! чтобы считать использование квот (и дальше — ресурсы контейнеров, этап O3).

/// Значение quantity или `None`, если строка — не quantity.
pub fn parse_quantity(q: &str) -> Option<f64> {
    let q = q.trim();
    let (number, suffix) = q.split_at(number_end(q.as_bytes()));
    let multiplier = match suffix {
        "" => 1.0,
        "n" => 1e-9,
        "u" => 1e-6,
        "m" => 1e-3,
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        "Ki" => 1024.0,
        "Mi" => 1024f64.powi(2),
        "Gi" => 1024f64.powi(3),
        "Ti" => 1024f64.powi(4),
        "Pi" => 1024f64.powi(5),
        "Ei" => 1024f64.powi(6),
        _ => return None,
    };
    if number.is_empty() {
        return None;
    }
    number.parse::<f64>().ok().map(|n| n * multiplier)
}

/// Где кончается число: знак, цифры и точка, затем показатель `e3` (`E` без цифр — суффикс).
fn number_end(b: &[u8]) -> usize {
    let digits = |mut i: usize| {
        while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
            i += 1;
        }
        i
    };
    let mut i = digits(usize::from(
        b.first().is_some_and(|c| *c == b'+' || *c == b'-'),
    ));
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        let mut j = i + 1;
        if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
            j += 1;
        }
        if j < b.len() && b[j].is_ascii_digit() {
            i = digits(j);
        }
    }
    i
}

/// Ядра строкой quantity: `250m`, `2`.
pub fn format_cpu(cores: f64) -> String {
    let milli = (cores * 1000.0).round() as i64;
    if milli % 1000 == 0 {
        format!("{}", milli / 1000)
    } else {
        format!("{milli}m")
    }
}

/// Байты строкой quantity: `2Gi`, `512Mi`, иначе — округлённые мебибайты.
pub fn format_memory(bytes: f64) -> String {
    const MI: f64 = 1024.0 * 1024.0;
    let b = bytes.round();
    if b != 0.0 && b % (1024.0 * MI) == 0.0 {
        format!("{}Gi", (b / (1024.0 * MI)) as i64)
    } else {
        format!("{}Mi", (b / MI).round() as i64)
    }
}

/// Целое с допуском на погрешность дробей (`700m` как f64 — 0.7000000000000001): почти целое —
/// оно само, иначе вверх (`up`) или вниз.
fn round_toward(x: f64, up: bool) -> f64 {
    let near = x.round();
    if (x - near).abs() < 1e-6 {
        near
    } else if up {
        x.ceil()
    } else {
        x.floor()
    }
}

/// Ядра строкой quantity, как `format_cpu`, но милликоры округляются в сторону `up`: граница в
/// тексте остаётся допустимым значением (максимум — вниз, минимум — вверх).
pub fn format_cpu_toward(cores: f64, up: bool) -> String {
    format_cpu(round_toward(cores * 1000.0, up) / 1000.0)
}

/// Байты строкой quantity, как `format_memory`, но мебибайты округляются в сторону `up`:
/// максимум `1G` — `953Mi` (а не `954Mi`, который сам выше максимума), минимум `100M` — `96Mi`.
pub fn format_memory_toward(bytes: f64, up: bool) -> String {
    const MI: f64 = 1024.0 * 1024.0;
    format_memory(round_toward(bytes / MI, up) * MI)
}

/// Доля `used` от `hard`; `None`, если что-то не разобрано или лимит нулевой.
pub fn ratio(used: &str, hard: &str) -> Option<f64> {
    let hard = parse_quantity(hard).filter(|h| *h > 0.0)?;
    Some(parse_quantity(used)? / hard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_are_rounded_toward_the_allowed_side() {
        // Максимум — вниз, минимум — вверх; ровные значения не меняются.
        assert_eq!(format_memory_toward(1e9, false), "953Mi");
        assert_eq!(format_memory_toward(1e9, true), "954Mi");
        assert_eq!(format_memory_toward(1e8, true), "96Mi");
        assert_eq!(format_memory_toward(1e8, false), "95Mi");
        assert_eq!(
            format_memory_toward(2.0 * 1024.0 * 1024.0 * 1024.0, false),
            "2Gi"
        );
        assert_eq!(format_memory_toward(256.0 * 1024.0 * 1024.0, true), "256Mi");
        assert_eq!(format_cpu_toward(0.0015, false), "1m");
        assert_eq!(format_cpu_toward(0.0015, true), "2m");
        assert_eq!(format_cpu_toward(0.7, true), "700m");
        assert_eq!(format_cpu_toward(0.7, false), "700m");
        assert_eq!(format_cpu_toward(2.0, false), "2");
    }

    #[test]
    fn quantities() {
        for (q, v) in [
            ("1", 1.0),
            ("500m", 0.5),
            ("2.5", 2.5),
            ("128Mi", 134_217_728.0),
            ("1Gi", 1_073_741_824.0),
            ("1k", 1000.0),
            ("2e3", 2000.0),
            ("1.5G", 1.5e9),
            ("100n", 1e-7),
            ("1E", 1e18),
            ("1Ei", 1024f64.powi(6)),
            ("2E3", 2000.0),
        ] {
            let got = parse_quantity(q).unwrap();
            assert!((got - v).abs() < 1e-9 * v.max(1.0), "{q}: {got}");
        }
        for bad in ["", "Mi", "1Xi", "e3", "abc"] {
            assert_eq!(parse_quantity(bad), None, "{bad}");
        }
    }

    #[test]
    fn quantities_back_to_text() {
        assert_eq!(format_cpu(0.25), "250m");
        assert_eq!(format_cpu(2.0), "2");
        assert_eq!(format_cpu(0.0101), "10m");
        assert_eq!(format_memory(512.0 * 1024.0 * 1024.0), "512Mi");
        assert_eq!(format_memory(2.0 * 1024.0 * 1024.0 * 1024.0), "2Gi");
        assert_eq!(format_memory(1_000_000_000.0), "954Mi");
    }

    #[test]
    fn ratios() {
        assert_eq!(ratio("500m", "1"), Some(0.5));
        assert_eq!(ratio("9", "10"), Some(0.9));
        assert_eq!(ratio("1Gi", "2Gi"), Some(0.5));
        assert_eq!(ratio("1", "0"), None);
        assert_eq!(ratio("x", "1"), None);
    }
}
