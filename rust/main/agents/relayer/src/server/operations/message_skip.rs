use std::{cmp::Reverse, collections::HashMap};

use axum::{extract::State, http::StatusCode, routing, Json, Router};
use derive_new::new;
use hyperlane_base::db::{HyperlaneDb, HyperlaneRocksDB};
use hyperlane_core::H256;
use serde::{Deserialize, Serialize};

use crate::{msg::op_queue::OperationPriorityQueue, settings::matching_list::MatchingList};

const MESSAGE_SKIP_API_BASE: &str = "/message_skip";

#[derive(Clone, Debug, new)]
pub struct ServerState {
    op_queues: HashMap<u32, OperationPriorityQueue>,
    dbs: HashMap<u32, HyperlaneRocksDB>,
    max_retries: u32,
}

impl ServerState {
    pub fn router(self) -> Router {
        Router::new()
            .route(MESSAGE_SKIP_API_BASE, routing::post(handler))
            .with_state(self)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct MessageSkipRequest {
    pub pattern: MatchingList,
    #[serde(default = "default_true")]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MessageSkipFailure {
    pub id: H256,
    pub origin_domain: u32,
    pub error: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MessageSkipSample {
    pub id: H256,
    pub origin_domain: u32,
    pub destination_domain: u32,
    pub retry_count: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MessageSkipResponse {
    pub dry_run: bool,
    pub max_retries: u32,
    pub evaluated: usize,
    pub matched: usize,
    pub removed: usize,
    pub failed: usize,
    pub matched_ids: Vec<H256>,
    pub failures: Vec<MessageSkipFailure>,
    pub samples: Vec<MessageSkipSample>,
}

fn default_true() -> bool {
    true
}

fn validate_request(payload: &MessageSkipRequest) -> Result<(), (StatusCode, String)> {
    if payload
        .pattern
        .0
        .as_ref()
        .map(|rules| rules.is_empty())
        .unwrap_or(true)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "message_skip requires a non-empty pattern".to_string(),
        ));
    }
    Ok(())
}

async fn handler(
    State(state): State<ServerState>,
    Json(payload): Json<MessageSkipRequest>,
) -> Result<Json<MessageSkipResponse>, (StatusCode, String)> {
    validate_request(&payload)?;

    let response = apply_message_skip(
        &state.op_queues,
        &state.dbs,
        state.max_retries,
        &payload.pattern,
        payload.dry_run,
    )
    .await;

    Ok(Json(response))
}

pub async fn apply_message_skip(
    op_queues: &HashMap<u32, OperationPriorityQueue>,
    dbs: &HashMap<u32, HyperlaneRocksDB>,
    max_retries: u32,
    pattern: &MatchingList,
    dry_run: bool,
) -> MessageSkipResponse {
    let mut response = MessageSkipResponse {
        dry_run,
        max_retries,
        evaluated: 0,
        matched: 0,
        removed: 0,
        failed: 0,
        matched_ids: Vec::new(),
        failures: Vec::new(),
        samples: Vec::new(),
    };

    for queue in op_queues.values() {
        let mut queue = queue.lock().await;
        let mut retained = std::collections::BinaryHeap::new();

        for Reverse(op) in queue.drain() {
            response.evaluated += 1;

            if !pattern.op_matches(&op) {
                retained.push(Reverse(op));
                continue;
            }

            response.matched += 1;
            response.matched_ids.push(op.id());
            if response.samples.len() < 20 {
                response.samples.push(MessageSkipSample {
                    id: op.id(),
                    origin_domain: op.origin_domain_id(),
                    destination_domain: op.destination_domain().id(),
                    retry_count: op.get_retries(),
                });
            }

            if dry_run {
                retained.push(Reverse(op));
                continue;
            }

            let persist_result = dbs
                .get(&op.origin_domain_id())
                .ok_or_else(|| {
                    format!(
                        "No Hyperlane DB found for origin domain {}",
                        op.origin_domain_id()
                    )
                })
                .and_then(|db| {
                    db.store_pending_message_retry_count_by_message_id(&op.id(), &max_retries)
                        .map_err(|err| err.to_string())
                });

            match persist_result {
                Ok(()) => {
                    op.decrement_metric_if_exists();
                    response.removed += 1;
                }
                Err(error) => {
                    response.failed += 1;
                    response.failures.push(MessageSkipFailure {
                        id: op.id(),
                        origin_domain: op.origin_domain_id(),
                        error,
                    });
                    retained.push(Reverse(op));
                }
            }
        }

        queue.append(&mut retained);
    }

    response
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use axum::{
        body::Body,
        http::{header::CONTENT_TYPE, Method, Request, Response, StatusCode},
    };
    use hyperlane_base::db::{HyperlaneDb, DB};
    use hyperlane_core::{HyperlaneDomain, KnownHyperlaneDomain, QueueOperation};
    use tokio::sync::{broadcast::Sender, Mutex};
    use tower::ServiceExt;

    use crate::{
        msg::op_queue::{
            test::{dummy_metrics_and_label, MockPendingOperation},
            OpQueue,
        },
        server::ENDPOINT_MESSAGES_QUEUE_SIZE,
        test_utils::request::parse_body_to_json,
    };

    use super::*;

    #[derive(Debug)]
    struct TestServerSetup {
        app: Router,
        op_queue: OperationPriorityQueue,
        dbs: HashMap<u32, HyperlaneRocksDB>,
        origin_domain: HyperlaneDomain,
        destination_domain: HyperlaneDomain,
    }

    fn setup_test_server(include_origin_db: bool) -> TestServerSetup {
        let origin_domain = HyperlaneDomain::Known(KnownHyperlaneDomain::Ethereum);
        let destination_domain = HyperlaneDomain::Known(KnownHyperlaneDomain::Arbitrum);

        let (metrics, queue_metrics_label) = dummy_metrics_and_label();
        let broadcaster = Sender::new(ENDPOINT_MESSAGES_QUEUE_SIZE);
        let op_queue = OpQueue::new(
            metrics,
            queue_metrics_label,
            Arc::new(Mutex::new(broadcaster.subscribe())),
        );

        let mut op_queues = HashMap::new();
        op_queues.insert(destination_domain.id(), op_queue.queue.clone());

        let mut dbs = HashMap::new();
        if include_origin_db {
            let temp_dir = tempfile::tempdir().unwrap();
            let db = DB::from_path(temp_dir.path()).unwrap();
            let base_db = HyperlaneRocksDB::new(&origin_domain, db);
            dbs.insert(origin_domain.id(), base_db);
            std::mem::forget(temp_dir);
        }

        let app = ServerState::new(op_queues, dbs.clone(), 66).router();

        TestServerSetup {
            app,
            op_queue: op_queue.queue,
            dbs,
            origin_domain,
            destination_domain,
        }
    }

    async fn send_request(app: Router, body: &serde_json::Value) -> Response<Body> {
        let request = Request::builder()
            .uri(MESSAGE_SKIP_API_BASE)
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/json")
            .body(serde_json::to_string(body).expect("Failed to serialize body"))
            .expect("Failed to build request");
        app.oneshot(request).await.expect("Failed to send request")
    }

    fn make_operation(
        id: &str,
        origin_domain: HyperlaneDomain,
        destination_domain: HyperlaneDomain,
        retry_count: u32,
    ) -> QueueOperation {
        Box::new(
            MockPendingOperation::new(1, destination_domain)
                .with_id(id)
                .with_origin_domain(origin_domain)
                .with_retry_count(retry_count),
        ) as QueueOperation
    }

    #[tokio::test]
    async fn test_message_skip_requires_non_empty_pattern() {
        let TestServerSetup { app, .. } = setup_test_server(true);
        let response = send_request(app, &serde_json::json!({ "pattern": null })).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_message_skip_dry_run_keeps_queue_and_db_untouched() {
        let TestServerSetup {
            app,
            op_queue,
            dbs,
            origin_domain,
            destination_domain,
        } = setup_test_server(true);

        let message_id = "0x1acbee9798118b11ebef0d94b0a2936eafd58e3bfab91b05da875825c4a1c39b";
        op_queue.lock().await.push(Reverse(make_operation(
            message_id,
            origin_domain.clone(),
            destination_domain.clone(),
            30,
        )));

        let response = send_request(
            app,
            &serde_json::json!({
                "pattern": [{ "messageid": message_id }],
                "dry_run": true
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: MessageSkipResponse = parse_body_to_json(response.into_body()).await;
        assert!(body.dry_run);
        assert_eq!(body.matched, 1);
        assert_eq!(body.removed, 0);
        assert_eq!(op_queue.lock().await.len(), 1);
        let retry_count = dbs
            .get(&origin_domain.id())
            .unwrap()
            .retrieve_pending_message_retry_count_by_message_id(&message_id.parse().unwrap())
            .unwrap();
        assert!(retry_count.is_none());
    }

    #[tokio::test]
    async fn test_message_skip_apply_removes_queue_item_and_persists_retry_count() {
        let TestServerSetup {
            app,
            op_queue,
            dbs,
            origin_domain,
            destination_domain,
        } = setup_test_server(true);

        let skipped_id = "0x1acbee9798118b11ebef0d94b0a2936eafd58e3bfab91b05da875825c4a1c39b";
        let kept_id = "0x51e7be221ce90a49dee46ca0d0270c48d338a7b9d85c2a89d83fac0816571914";
        op_queue.lock().await.push(Reverse(make_operation(
            skipped_id,
            origin_domain.clone(),
            destination_domain.clone(),
            30,
        )));
        op_queue.lock().await.push(Reverse(make_operation(
            kept_id,
            origin_domain.clone(),
            destination_domain.clone(),
            31,
        )));

        let response = send_request(
            app,
            &serde_json::json!({
                "pattern": [{ "messageid": skipped_id }],
                "dry_run": false
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: MessageSkipResponse = parse_body_to_json(response.into_body()).await;
        assert!(!body.dry_run);
        assert_eq!(body.matched, 1);
        assert_eq!(body.removed, 1);
        assert_eq!(body.failed, 0);
        assert_eq!(op_queue.lock().await.len(), 1);
        let retry_count = dbs
            .get(&origin_domain.id())
            .unwrap()
            .retrieve_pending_message_retry_count_by_message_id(&skipped_id.parse().unwrap())
            .unwrap();
        assert_eq!(retry_count, Some(66));
    }

    #[tokio::test]
    async fn test_message_skip_apply_keeps_item_when_origin_db_missing() {
        let TestServerSetup {
            app,
            op_queue,
            origin_domain,
            destination_domain,
            ..
        } = setup_test_server(false);

        let message_id = "0x1acbee9798118b11ebef0d94b0a2936eafd58e3bfab91b05da875825c4a1c39b";
        op_queue.lock().await.push(Reverse(make_operation(
            message_id,
            origin_domain.clone(),
            destination_domain.clone(),
            30,
        )));

        let response = send_request(
            app,
            &serde_json::json!({
                "pattern": [{ "messageid": message_id }],
                "dry_run": false
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: MessageSkipResponse = parse_body_to_json(response.into_body()).await;
        assert_eq!(body.matched, 1);
        assert_eq!(body.removed, 0);
        assert_eq!(body.failed, 1);
        assert_eq!(op_queue.lock().await.len(), 1);
    }
}
