# Dictator API fork

This local branch adds an opt-in HTTP server to the running Handy app. It shares Handy's `TranscriptionManager`, engine supervisor, selected model, language options, dictionary and filler-word cleanup. No process-per-request CLI wrapper is involved. The native model worker stays alive and loaded between requests.

The API does not record on the Mac, paste on the Mac, add phone audio to Handy's history, or invoke the optional LLM post-processing provider. Uploaded audio is decoded in memory and passed to the existing batch transcription path. No model is downloaded by the endpoint.

## Start

Build the app with the parent project's `scripts/build-handy.sh` (or the normal Handy Tauri build). Quit the existing Handy instance, then use `scripts/start-handy-api.sh`. Closing the window leaves Handy running in the tray and keeps the API available. Quit Handy to stop it.

Equivalent direct launch:

```sh
HANDY_API_TOKEN='<your token>' handy --api-listen 100.77.235.51:8765
```

`--api-listen` must be supplied when the first app instance starts. It is disabled by default and not persisted. Set `HANDY_API_TOKEN` for authentication; it is required for any non-loopback bind address. The launch helper creates a random token in the parent `server/handy-api.token`, with owner-only permissions, and disables the upstream auto-updater for the fork process. Put this token in Dictator's Access token field. Keep the Android endpoint at `http://100.77.235.51:8765/transcribe`.

Both idle-timeout unloading and immediate-after-transcription unloading are suppressed while the API is serving. This is a runtime override; the user's saved idle setting is unchanged. Manual model unload/switch remains possible between requests. The next request reloads the selected model if needed; a normal model switch or native-worker recovery can therefore still incur a load.

## HTTP contract

- `POST /transcribe`: raw `audio/wav` bytes, mono, 16 kHz, 16-bit PCM. Maximum five minutes / 10 MiB.
- `Authorization: Bearer <token>` on both routes when a token is configured.
- Success: `{"text":"Transcript", "audio_seconds": 3.25}`.
- Failure: non-2xx with `{"error":"Explanation"}`.
- `GET /health`: reports loaded model, backend, busy state and the keep-warm override. `model_loaded: true` indicates the engine is ready; loading/model errors leave it false. The HTTP listener can be reachable while the model is still loading.

Only one phone upload/inference is admitted at once; a second receives 429. Active desktop recording/processing or a queued/running file job causes 503. An admitted API request reserves the desktop coordinator's processing slot; keyboard presses follow the existing busy/remembered-key behaviour and can start desktop recording when it finishes. File work queued afterwards waits until the API releases the slot. Model switching/deletion/unloading is refused during inference.

Disconnecting/cancelling the phone request does not globally cancel Handy's shared engine: admitted inference may finish, and its slot is held until it does. The Android app already ignores cancelled replies. Handy's own desktop Cancel can interrupt the shared inference. The model remains warm after either case. Uploads time out after 20 seconds; inference uses Handy's existing worker supervision and timeouts.

## Implementation

- `src-tauri/src/api.rs`: authenticated HTTP routes, bounded WAV parsing, blocking-worker handoff, coordinator admission, graceful listener stop.
- `src-tauri/src/managers/transcription.rs`: runtime model pin and API-busy state, preserving normal idle behaviour when disabled.
- `src-tauri/src/transcription_coordinator.rs`: shared processing admission, so phone and desktop take turns.
- `src-tauri/src/managers/transcription_jobs.rs`: file jobs yield to the API slot.
- `src-tauri/src/cli.rs` / `lib.rs`: optional startup flag and app-lifetime wiring.

Tests cover HTTP authentication, malformed and oversized audio, repeated requests through the same backend, concurrent admission, remote-bind configuration, and coordinator exclusion/cleanup. Runtime warm-model validation requires the built app and an installed model.

Validated on this Mac: 327 library tests passed, 18 upstream native fault-injection tests ignored; frontend build/lint passed; Clippy completed with existing upstream warnings. The release bundle successfully served three real speech requests using the installed Parakeet Unified EN 0.6B Q8 model on Metal. The 5.81-second sample decoded correctly; first request took 0.45 seconds and the next 0.09 seconds. Health remained `model_loaded: true` / `keep_warm: true`, and the same native worker PID was observed before and after a further request. The Android app has been updated to check `/health` and configured with the API token.

Upstream base: `7bfb2e2c934b798bcf6e3627653aed2439996f89` (`main`, inspected 29 October 2026). This is a local fork branch, `dictator-api`; no upstream PR has been opened.

Build note for this Mac: only Xcode Command Line Tools are installed. Upstream therefore builds its optional Apple Intelligence post-processing bridge with stubs. Parakeet/Whisper transcription and this API are unaffected; Apple Intelligence polishing requires a build with full Xcode. Other configured post-processing providers retain their upstream implementation, although the phone API intentionally does not invoke them.
