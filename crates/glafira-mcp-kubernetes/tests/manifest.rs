/// Архитектурная гарантия «только чтение» (спец. §4.1): крейт не может дотянуться до кластера
/// и до изменяющих функций приложения, потому что не зависит от них.
#[test]
fn crate_cannot_reach_the_cluster_or_the_app() {
    let manifest =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).unwrap();
    let mut section = String::new();
    let mut deps = Vec::new();
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            section = line.to_string();
        } else if section.contains("dependencies")
            && !line.starts_with('#')
            && let Some((name, _)) = line.split_once('=')
        {
            deps.push(name.trim().to_string());
        }
    }
    assert!(
        deps.contains(&"glafira-mcp-core".to_string()),
        "разбор манифеста: {deps:?}"
    );
    for d in &deps {
        let forbidden = d == "kube"
            || d.starts_with("kube-")
            || d.starts_with("k8s-")
            || [
                "reqwest",
                "hyper",
                "tauri",
                "glafira-kubernetes",
                "glafira_kubernetes",
            ]
            .iter()
            .any(|f| d.contains(f));
        assert!(!forbidden, "запрещённая зависимость {d}");
    }
}
