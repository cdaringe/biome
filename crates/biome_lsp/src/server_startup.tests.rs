use super::*;
use crate::server_test_utils::{CHANNEL_BUFFER_SIZE, client_handler, uri};
use anyhow::Result;
use biome_service::workspace::GetFileContentParams;
use futures::channel::mpsc::channel;
use futures::{SinkExt, StreamExt, pin_mut, poll};
use std::str::FromStr;
use tower_lsp_server::jsonrpc::Response;
use tower_lsp_server::ls_types as lsp;

#[tokio::test]
async fn document_notifications_wait_for_initial_configuration() -> Result<()> {
    for scenario in ["open", "change", "close"] {
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

        let did_open = service.call(
            JsonRpcRequest::build("textDocument/didOpen")
                .params(json!({
                    "textDocument": {
                        "uri": uri!("document.js"),
                        "languageId": "javascript",
                        "version": 0,
                        "text": "let value = 1;"
                    }
                }))
                .finish(),
        );
        pin_mut!(did_open);
        assert!(poll!(&mut did_open).is_pending());
        assert!(stream.next().now_or_never().is_none());

        let request = match scenario {
            "change" => Some(
                JsonRpcRequest::build("textDocument/didChange")
                    .params(json!({
                        "textDocument": { "uri": uri!("document.js"), "version": 1 },
                        "contentChanges": [{ "text": "let value = 2;" }]
                    }))
                    .finish(),
            ),
            "close" => Some(
                JsonRpcRequest::build("textDocument/didClose")
                    .params(json!({ "textDocument": { "uri": uri!("document.js") } }))
                    .finish(),
            ),
            _ => None,
        };
        let follow_up = async {
            if let Some(request) = request {
                service.call(request).await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        pin_mut!(follow_up);
        if scenario != "open" {
            assert!(poll!(&mut follow_up).is_pending(), "{scenario}");
        }

        sink.send(Response::from_ok(
            configuration.id().unwrap().clone(),
            json!([{}]),
        ))
        .await?;
        let (sender, _receiver) = channel(CHANNEL_BUFFER_SIZE);
        let reader = tokio::spawn(client_handler(stream, sink, sender));

        let (initialized, did_open, follow_up) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures::future::join3(initialized, did_open, follow_up),
        )
        .await?;
        initialized?;
        did_open?;
        follow_up?;
        let session = factory
            .sessions
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone();
        assert!(session.has_initialized());
        let document = session.document(&uri!("document.js"));
        if scenario == "close" {
            assert!(document.is_none());
        } else {
            let document = document.unwrap();
            let content = session.workspace().get_file_content(GetFileContentParams {
                project_key: document.project_key,
                path: session.file_path(&uri!("document.js"))?,
            })?;
            assert_eq!(
                content,
                if scenario == "change" {
                    "let value = 2;"
                } else {
                    "let value = 1;"
                }
            );
        }
        reader.abort();
    }
    Ok(())
}
