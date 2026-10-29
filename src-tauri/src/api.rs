//! Opt-in HTTP access to the running app's shared, warm transcription engine.
//! No microphone, history, clipboard, desktop paste, or LLM calls on this path.

use axum::{
    body::{to_bytes, Bytes},
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{
    io::Cursor,
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tauri::{AppHandle, Manager};
use tokio::sync::{oneshot, Semaphore};

use crate::{
    managers::transcription::TranscriptionManager,
    transcription_coordinator::TranscriptionCoordinator,
};

const MAX_BODY: usize = 10 * 1024 * 1024;
const MAX_SAMPLES: usize = 300 * 16_000;

#[derive(Debug)]
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

trait Transcriber: Send + Sync {
    fn health(&self) -> Value;
    fn transcribe(&self, samples: Vec<f32>) -> Result<String, ApiError>;
}

struct HandyTranscriber(AppHandle);

/// Owns the coordinator's processing slot even if an HTTP client disconnects.
struct Admission {
    app: AppHandle,
    manager: Arc<TranscriptionManager>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.manager.set_api_busy(false);
        self.app
            .state::<TranscriptionCoordinator>()
            .notify_processing_finished();
    }
}

impl Transcriber for HandyTranscriber {
    fn health(&self) -> Value {
        let tm = self.0.state::<Arc<TranscriptionManager>>();
        json!({
            "status": if tm.is_model_loaded() { "ready" } else { "loading" },
            "model_loaded": tm.is_model_loaded(),
            "model": tm.get_current_model(),
            "backend": tm.current_backend(),
            "keep_warm": tm.api_keeps_warm(),
            "busy": tm.api_busy() || tm.job_active() || crate::tray::dictation_in_progress(&self.0),
        })
    }

    fn transcribe(&self, samples: Vec<f32>) -> Result<String, ApiError> {
        if !self.0.state::<TranscriptionCoordinator>().try_acquire_api() {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Handy is busy with desktop dictation or a file. Try again.".into(),
            ));
        }
        let tm = self.0.state::<Arc<TranscriptionManager>>().inner().clone();
        let _admission = Admission {
            app: self.0.clone(),
            manager: tm.clone(),
        };
        // The usual loading flag/condvar lets transcribe wait for this load.
        // If already loaded, this is a no-op and reuses the same worker.
        tm.initiate_model_load();
        tm.transcribe(samples)
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
    }
}

#[derive(Clone)]
struct ApiState {
    backend: Arc<dyn Transcriber>,
    token: Arc<str>,
    slots: Arc<Semaphore>,
}

fn router(backend: Arc<dyn Transcriber>, token: String) -> Router {
    let state = ApiState {
        backend,
        token: token.into(),
        slots: Arc::new(Semaphore::new(1)),
    };
    Router::new()
        .route("/health", get(health))
        .route("/transcribe", post(transcribe))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state)
}

async fn authorize(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    if !state.token.is_empty() {
        let supplied = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if !bool::from(supplied.as_bytes().ct_eq(state.token.as_bytes())) {
            return ApiError(
                StatusCode::UNAUTHORIZED,
                "Invalid or missing access token.".into(),
            )
            .into_response();
        }
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

async fn health(State(state): State<ApiState>) -> Json<Value> {
    Json(state.backend.health())
}

async fn transcribe(
    State(state): State<ApiState>,
    request: Request,
) -> Result<Json<Value>, ApiError> {
    // One upload/inference at a time. Holding the permit in the blocking worker
    // bounds work even when the caller cancels its HTTP request.
    let permit = state.slots.clone().try_acquire_owned().map_err(|_| {
        ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            "A phone request is already running. Try again.".into(),
        )
    })?;
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if !content_type.eq_ignore_ascii_case("audio/wav")
        && !content_type.eq_ignore_ascii_case("audio/x-wav")
    {
        return Err(ApiError(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Send Content-Type: audio/wav.".into(),
        ));
    }
    let bytes = tokio::time::timeout(
        Duration::from_secs(20),
        to_bytes(request.into_body(), MAX_BODY),
    )
    .await
    .map_err(|_| {
        ApiError(
            StatusCode::REQUEST_TIMEOUT,
            "Audio upload timed out.".into(),
        )
    })?
    .map_err(|_| {
        ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Invalid upload or audio exceeds 10 MiB.".into(),
        )
    })?;
    let samples = decode_wav(bytes)?;
    let audio_seconds = samples.len() as f64 / 16000.0;
    let backend = state.backend;
    let text = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        backend.transcribe(samples)
    })
    .await
    .map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Transcription worker failed.".into(),
        )
    })??;
    Ok(Json(json!({"text": text, "audio_seconds": audio_seconds})))
}

fn decode_wav(bytes: Bytes) -> Result<Vec<f32>, ApiError> {
    let bad = |message: &str| ApiError(StatusCode::UNPROCESSABLE_ENTITY, message.into());
    let mut reader =
        hound::WavReader::new(Cursor::new(bytes)).map_err(|_| bad("Invalid WAV audio."))?;
    let spec = reader.spec();
    if spec.channels != 1
        || spec.sample_rate != 16000
        || spec.bits_per_sample != 16
        || spec.sample_format != hound::SampleFormat::Int
    {
        return Err(bad("Expected mono, 16 kHz, PCM16 WAV audio."));
    }
    let count = reader.duration() as usize;
    if count == 0 || count > MAX_SAMPLES {
        return Err(bad(
            "Audio must contain between one sample and five minutes.",
        ));
    }
    let samples: Result<Vec<f32>, _> = reader
        .samples::<i16>()
        .take(MAX_SAMPLES + 1)
        .map(|sample| sample.map(|s| s as f32 / 32768.0))
        .collect();
    let samples = samples.map_err(|_| bad("WAV audio is truncated or invalid."))?;
    if samples.len() != count {
        return Err(bad("WAV audio is truncated."));
    }
    Ok(samples)
}

pub struct ApiServer {
    stop: Mutex<Option<oneshot::Sender<()>>>,
    manager: Arc<TranscriptionManager>,
}

impl ApiServer {
    pub fn stop(&self) {
        self.manager.set_api_keep_warm(false);
        if let Ok(mut sender) = self.stop.lock() {
            if let Some(sender) = sender.take() {
                let _ = sender.send(());
            }
        }
    }
}

fn validate_config(address: SocketAddr, token: &str) -> anyhow::Result<()> {
    if !address.ip().is_loopback() && token.trim().is_empty() {
        anyhow::bail!("Set HANDY_API_TOKEN before serving on a non-loopback address.");
    }
    Ok(())
}

pub fn start(app: &AppHandle, address: SocketAddr) -> anyhow::Result<()> {
    let token = std::env::var("HANDY_API_TOKEN").unwrap_or_default();
    validate_config(address, &token)?;
    // Bind before pinning the model: failure must not silently leave the app
    // showing a running API or change its memory policy.
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    let manager = app.state::<Arc<TranscriptionManager>>().inner().clone();
    manager.set_api_keep_warm(true);
    manager.initiate_model_load();
    let (stop, stopped) = oneshot::channel();
    app.manage(ApiServer {
        stop: Mutex::new(Some(stop)),
        manager: manager.clone(),
    });
    let router = router(Arc::new(HandyTranscriber(app.clone())), token);
    tauri::async_runtime::spawn(async move {
        let result = async {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            log::info!("Dictator API listening on {address}; shared model will stay warm");
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        }
        .await;
        manager.set_api_keep_warm(false);
        if let Err(error) = result {
            log::error!("Dictator API stopped: {error}");
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    struct Fake(AtomicUsize);
    impl Transcriber for Fake {
        fn health(&self) -> Value {
            json!({"model_loaded": true, "keep_warm": true})
        }
        fn transcribe(&self, samples: Vec<f32>) -> Result<String, ApiError> {
            assert_eq!(samples.len(), 160);
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok("A real transcript.".into())
        }
    }
    fn wav() -> Vec<u8> {
        let mut out = Cursor::new(Vec::new());
        {
            let mut w = hound::WavWriter::new(
                &mut out,
                hound::WavSpec {
                    channels: 1,
                    sample_rate: 16000,
                    bits_per_sample: 16,
                    sample_format: hound::SampleFormat::Int,
                },
            )
            .unwrap();
            for _ in 0..160 {
                w.write_sample(0i16).unwrap();
            }
            w.finalize().unwrap();
        }
        out.into_inner()
    }
    #[test]
    fn remote_listener_requires_authentication() {
        assert!(validate_config("100.77.235.51:8765".parse().unwrap(), "").is_err());
        assert!(validate_config("100.77.235.51:8765".parse().unwrap(), "token").is_ok());
        assert!(validate_config("127.0.0.1:8765".parse().unwrap(), "").is_ok());
    }
    #[test]
    fn rejects_truncated_and_wrong_format_audio() {
        assert_eq!(
            decode_wav(Bytes::from_static(b"bad")).unwrap_err().0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let mut bytes = wav();
        bytes.pop();
        assert!(decode_wav(bytes.into()).is_err());
        let mut bytes = wav();
        bytes[24..28].copy_from_slice(&44100u32.to_le_bytes());
        assert!(decode_wav(bytes.into()).is_err());
    }
    #[tokio::test]
    async fn auth_precedes_upload_and_transcription() {
        let backend = Arc::new(Fake(AtomicUsize::new(0)));
        let app = router(backend.clone(), "token".into());
        let response = app
            .oneshot(
                Request::post("/transcribe")
                    .body(Body::from(wav()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(backend.0.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn repeated_requests_reuse_backend_and_return_the_phone_contract() {
        let backend = Arc::new(Fake(AtomicUsize::new(0)));
        let app = router(backend.clone(), "token".into());
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/transcribe")
                        .header("Authorization", "Bearer token")
                        .header("Content-Type", "audio/wav")
                        .body(Body::from(wav()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap()["text"],
                "A real transcript."
            );
        }
        assert_eq!(backend.0.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn malformed_uploads_never_reach_the_model() {
        let backend = Arc::new(Fake(AtomicUsize::new(0)));
        let app = router(backend.clone(), String::new());
        for (content_type, body, status) in [
            ("text/plain", wav(), StatusCode::UNSUPPORTED_MEDIA_TYPE),
            (
                "audio/wav",
                b"bad audio".to_vec(),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "audio/wav",
                vec![0u8; MAX_BODY + 1],
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/transcribe")
                        .header("Content-Type", content_type)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status);
        }
        assert_eq!(backend.0.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn only_one_phone_request_can_run_at_a_time() {
        use std::sync::atomic::AtomicBool;
        struct Blocking {
            started: tokio::sync::Notify,
            release: AtomicBool,
        }
        impl Transcriber for Blocking {
            fn health(&self) -> Value {
                json!({})
            }
            fn transcribe(&self, _: Vec<f32>) -> Result<String, ApiError> {
                self.started.notify_one();
                while !self.release.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Ok("Done".into())
            }
        }
        let backend = Arc::new(Blocking {
            started: tokio::sync::Notify::new(),
            release: AtomicBool::new(false),
        });
        let app = router(backend.clone(), String::new());
        let request = || {
            Request::post("/transcribe")
                .header("Content-Type", "audio/wav")
                .body(Body::from(wav()))
                .unwrap()
        };
        let first = tokio::spawn(app.clone().oneshot(request()));
        backend.started.notified().await;
        let response = app.oneshot(request()).await.unwrap();
        backend.release.store(true, Ordering::Release);
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
    }
}
