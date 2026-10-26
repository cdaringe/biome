use super::*;
use crate::server_test_utils::{CHANNEL_BUFFER_SIZE, client_handler, uri};
use anyhow::Result;
use futures::channel::mpsc::channel;
use futures::{SinkExt, StreamExt, pin_mut, poll};
use std::str::FromStr;
use tower_lsp_server::jsonrpc::Response;
use tower_lsp_server::ls_types as lsp;

#[tokio::test]
async fn document_notifications_wait_for_initial_configuration() -> Result<()> {
    for method in ["didOpen", "didChange", "didSave", "didClose"] {
        let factory = ServerFactory::default();
        let (mut service, client) = factory.create().into_inner();
        let (mut stream, mut sink) = client.split();
        service
            .call(
                JsonRpcRequest::build("initialize")
                    .id(0)
                    .params(json!({ "rootUri": uri!(""), "capabilities": {} }))
                    .finish(),
            )
            .await?;

        let initialized = service.call(
            JsonRpcRequest::build("initialized")
                .params(json!({}))
                .finish(),
        );
        pin_mut!(initialized);
        assert!(poll!(&mut initialized).is_pending());
        let configuration = stream.next().await.unwrap();
        assert_eq!(configuration.method(), "workspace/configuration");

        let notification = service.call(
            JsonRpcRequest::build(format!("textDocument/{method}"))
                .params(json!({
                    "textDocument": {
                        "uri": uri!("document.js"),
                        "languageId": "javascript",
                        "version": 0,
                        "text": "let value = 1;"
                    },
                    "contentChanges": []
                }))
                .finish(),
        );
        pin_mut!(notification);
        assert!(poll!(&mut notification).is_pending(), "{method}");
        assert!(stream.next().now_or_never().is_none());

        sink.send(Response::from_ok(
            configuration.id().unwrap().clone(),
            json!([{}]),
        ))
        .await?;
        let (sender, _receiver) = channel(CHANNEL_BUFFER_SIZE);
        let reader = tokio::spawn(client_handler(stream, sink, sender));

        let (initialized, notification) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures::future::join(initialized, notification),
        )
        .await?;
        initialized?;
        notification?;
        reader.abort();
    }
    Ok(())
}
