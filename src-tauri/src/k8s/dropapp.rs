//! Объекты Platform V DropApp (спека O5 §1): их CRD (все `v1alpha1`) в `k8s-openapi` нет, поэтому
//! они читаются как `DynamicObject`, а нужные поля берутся из JSON. Здесь — их `ApiResource` и разбор.

use kube::api::{ApiResource, DynamicObject, GroupVersionKind};

fn resource(group: &str, kind: &str, plural: &str) -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk(group, "v1alpha1", kind), plural)
}

/// `ClusterMetadata`: имя кластера и версия DropApp. Имя объекта и область CRD документация
/// не называет — объекты читаются списком по всем namespace.
pub fn cluster_metadatas() -> ApiResource {
    resource("config.dropapp.ru", "ClusterMetadata", "clustermetadatas")
}

/// Версия DropApp из `ClusterMetadata` (`spec.dappVersion`) как есть: `5.1.2`. Нет поля или
/// пустая строка — `None`.
pub fn dapp_version(o: &DynamicObject) -> Option<String> {
    o.data["spec"]["dappVersion"]
        .as_str()
        .filter(|v| !v.is_empty())
        .map(String::from)
}
