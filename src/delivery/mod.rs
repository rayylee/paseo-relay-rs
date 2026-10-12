pub mod writer;

use std::time::Instant;

use axum::extract::ws::Message;

use writer::{WriteError, WriterHandle};

/// Deliver one payload to every destination writer concurrently. Succeeds when
/// at least one destination accepted the frame; with no destinations the frame
/// is dropped successfully. Each writer owns the
/// deadline and must be allowed to observe it, so every task is awaited.
pub async fn deliver(
    writers: Vec<WriterHandle>,
    message: Message,
    deadline: Instant,
) -> Result<(), WriteError> {
    if writers.is_empty() {
        return Ok(());
    }

    let tasks: Vec<_> = writers
        .into_iter()
        .map(|writer| {
            let message = message.clone();
            tokio::spawn(async move {
                let (token, _source_done) = writer.reserve(deadline).await?;
                writer.write(token, message, deadline).await
            })
        })
        .collect();

    let mut first_error = None;
    let mut any_ok = false;
    for task in tasks {
        match task.await {
            Ok(Ok(())) => any_ok = true,
            Ok(Err(error)) => first_error = first_error.or(Some(error)),
            Err(_join) => first_error = first_error.or(Some(WriteError::DestinationClosed)),
        }
    }

    if any_ok {
        Ok(())
    } else {
        Err(first_error.unwrap_or(WriteError::DestinationClosed))
    }
}
