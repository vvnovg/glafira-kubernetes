use tauri::State;

use crate::error::AppResult;
use crate::k8s::nodes::{NodeDetails, NodeRow};
use crate::k8s::openshift::OperatorRow;
use crate::k8s::overview::ClusterOverview;
use crate::k8s::read::Listed;
use crate::state::AppState;

/// Обзор кластера (спец. §6.2).
#[tauri::command]
pub async fn get_cluster_overview(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<ClusterOverview> {
    state
        .read(&connection_id, |c| async move {
            Ok(crate::k8s::overview::cluster_overview(&c.reader(), &c.info).await)
        })
        .await
}

#[tauri::command]
pub async fn list_nodes(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<Listed<NodeRow>> {
    state
        .read(&connection_id, |c| async move {
            crate::k8s::nodes::list_nodes(&c.reader()).await
        })
        .await
}

#[tauri::command]
pub async fn get_node(
    state: State<'_, AppState>,
    connection_id: String,
    name: String,
) -> AppResult<NodeDetails> {
    state
        .read(&connection_id, |c| async move {
            crate::k8s::nodes::get_node(&c.reader(), &name).await
        })
        .await
}

/// Все ClusterOperators (OpenShift).
#[tauri::command]
pub async fn list_cluster_operators(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<Vec<OperatorRow>> {
    state
        .read(&connection_id, |c| async move {
            crate::platform::adapters::cluster_operators(&c.reader()).await
        })
        .await
}
