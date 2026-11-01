//! Проекты (спец. §5.1, §6.1): у OpenShift и DropApp 5.x — объекты `Project` своих групп, у
//! остальных — namespace. Если
//! кластер не отдаёт пользователю список, показываются проекты из профиля: ручной список и
//! избранные.

use std::collections::BTreeSet;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{ApiResource, GroupVersionKind, ListParams};
use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::dropapp;
use crate::k8s::read::ReadClient;
use crate::platform::ProjectSource;
use crate::storage::profile::ConnectionProfile;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRow {
    pub name: String,
    /// Отображаемое имя проекта OpenShift (`openshift.io/display-name`).
    pub display_name: Option<String>,
    /// `Active` или `Terminating`; у проектов из профиля — неизвестно.
    pub phase: Option<String>,
    pub favorite: bool,
    /// Проект из профиля, а не из списка кластера.
    pub from_profile: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectList {
    pub items: Vec<ProjectRow>,
    pub truncated: bool,
    /// Почему кластер не дал список (обычно нет прав): тогда в списке только проекты из профиля.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

pub fn projects() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("project.openshift.io", "v1", "Project"),
        "projects",
    )
}

/// Ресурс `Project` источника; у namespace — `None`.
pub fn project_resource(source: ProjectSource) -> Option<ApiResource> {
    match source {
        ProjectSource::Openshift => Some(projects()),
        ProjectSource::Dropapp => Some(dropapp::projects()),
        ProjectSource::Namespaces => None,
    }
}

/// Проекты кластера: имя, отображаемое имя и фаза.
async fn listed(r: &ReadClient, source: ProjectSource) -> AppResult<(Vec<ProjectRow>, bool)> {
    let row = |name: String, display_name: Option<String>, phase: Option<String>| ProjectRow {
        name,
        display_name,
        phase,
        favorite: false,
        from_profile: false,
    };
    if let Some(ar) = project_resource(source) {
        let l = r.list_dynamic(&ar, None, &ListParams::default()).await?;
        let rows = l
            .items
            .into_iter()
            .map(|p| {
                let display = p
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("openshift.io/display-name"))
                    .filter(|d| !d.is_empty())
                    .cloned();
                let phase = p.data["status"]["phase"].as_str().map(String::from);
                row(p.metadata.name.unwrap_or_default(), display, phase)
            })
            .collect();
        Ok((rows, l.truncated))
    } else {
        let l = r.list_cluster::<Namespace>(&ListParams::default()).await?;
        let rows = l
            .items
            .into_iter()
            .map(|n| {
                let phase = n.status.and_then(|s| s.phase);
                row(n.metadata.name.unwrap_or_default(), None, phase)
            })
            .collect();
        Ok((rows, l.truncated))
    }
}

/// Список проектов для дерева; избранные отмечены. Нет прав на список — проекты из профиля.
pub async fn list_projects(
    r: &ReadClient,
    source: ProjectSource,
    profile: &ConnectionProfile,
) -> AppResult<ProjectList> {
    let favorites: BTreeSet<&str> = profile
        .favorite_namespaces
        .iter()
        .map(String::as_str)
        .collect();
    let (mut items, truncated, error) = match listed(r, source).await {
        Ok((items, truncated)) => (items, truncated, None),
        Err(e) if e.code == ErrorCode::Forbidden => {
            let names: BTreeSet<&str> = profile
                .manual_namespaces
                .iter()
                .chain(&profile.favorite_namespaces)
                .map(String::as_str)
                .collect();
            let items = names
                .into_iter()
                .map(|n| ProjectRow {
                    name: n.to_string(),
                    display_name: None,
                    phase: None,
                    favorite: false,
                    from_profile: true,
                })
                .collect();
            (items, false, Some(e))
        }
        Err(e) => return Err(e),
    };
    for p in &mut items {
        p.favorite = favorites.contains(p.name.as_str());
    }
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(ProjectList {
        items,
        truncated,
        error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::storage::profile::sample_profile;
    use std::time::Duration;

    fn profile() -> ConnectionProfile {
        let mut p = sample_profile("a");
        p.favorite_namespaces = vec!["team-b".into(), "gone".into()];
        p.manual_namespaces = vec!["team-c".into(), "team-b".into()];
        p
    }

    #[tokio::test]
    async fn openshift_projects_with_display_names_and_favorites() {
        let (client, _) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "project.openshift.io/v1", "kind": "ProjectList",
                "metadata": {}, "items": [
                    {"metadata": {"name": "team-b"}, "status": {"phase": "Active"}},
                    {"metadata": {"name": "team-a",
                        "annotations": {"openshift.io/display-name": "Команда A"}},
                        "status": {"phase": "Terminating"}}
                ]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_projects(&r, ProjectSource::Openshift, &profile())
            .await
            .unwrap();
        assert!(l.error.is_none() && !l.truncated);
        let names: Vec<_> = l.items.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            ["team-a", "team-b"],
            "только проекты кластера, по имени"
        );
        assert_eq!(l.items[0].display_name.as_deref(), Some("Команда A"));
        assert_eq!(l.items[0].phase.as_deref(), Some("Terminating"));
        assert!(!l.items[0].favorite && l.items[1].favorite);
    }

    /// Проекты DropApp — из `projects.dropapp.ru`: имя проекта — имя namespace.
    #[tokio::test]
    async fn dropapp_projects() {
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "projects.dropapp.ru/v1alpha1", "kind": "ProjectList",
                "metadata": {}, "items": [
                    {"metadata": {"name": "team-b"}},
                    {"metadata": {"name": "team-a"}, "status": {"phase": "Active"}}
                ]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_projects(&r, ProjectSource::Dropapp, &profile())
            .await
            .unwrap();
        let rows: Vec<_> = l
            .items
            .iter()
            .map(|p| {
                (
                    p.name.as_str(),
                    p.phase.as_deref(),
                    p.favorite,
                    p.from_profile,
                )
            })
            .collect();
        assert_eq!(
            rows,
            [
                ("team-a", Some("Active"), false, false),
                ("team-b", None, true, false)
            ]
        );
        drop(r);
        assert_eq!(
            server.await.unwrap()[0].path(),
            "/apis/projects.dropapp.ru/v1alpha1/projects"
        );
    }

    #[tokio::test]
    async fn without_rights_the_projects_of_the_profile() {
        let (client, server) = mock_client(|_| {
            status(
                403,
                "Forbidden",
                r#"namespaces is forbidden: User "u" cannot list resource "namespaces" in API group "" at the cluster scope"#,
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_projects(&r, ProjectSource::Namespaces, &profile())
            .await
            .unwrap();
        assert_eq!(l.error.unwrap().key, "errors.k8s.forbiddenCluster");
        let names: Vec<_> = l
            .items
            .iter()
            .map(|p| (p.name.as_str(), p.favorite))
            .collect();
        assert_eq!(names, [("gone", true), ("team-b", true), ("team-c", false)]);
        assert!(l.items.iter().all(|p| p.from_profile));
        drop(r);
        assert_eq!(server.await.unwrap()[0].path(), "/api/v1/namespaces");
    }

    #[tokio::test]
    async fn other_errors_are_errors() {
        let (client, _) = mock_client(|_| status(401, "Unauthorized", "Unauthorized"));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let e = list_projects(&r, ProjectSource::Namespaces, &profile())
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::SessionExpired);
    }
}
