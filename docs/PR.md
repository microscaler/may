# PR: `recv_with_timeout` for `may::sync::spsc`

**Branch:** `may-sleep` → `master`  
**Tracking issue:** may-redis timeout API (blocked on this merge)

---

## Summary

Adds `Receiver::recv_with_timeout(Duration) -> Result<T, RecvError>` to `may::sync::spsc`, integrating with the scheduler timer wheel so coroutines can wait on an spsc response channel with a bounded deadline—without spin loops or ignored timeout parameters.

This unblocks **[may-redis](https://github.com/microscaler/may_redis)** (`execute_with_timeout`, pub/sub `recv_message_timeout`) and any other may consumer using the may_postgres request/response spsc pattern.

---

## Downstream consumer: [may-redis](https://github.com/microscaler/may_redis)

Companion changes (pending merge to `may_redis` `main`) wire this API into the Redis client’s request/response path—the same spsc pattern as [`may_postgres`](https://github.com/microscaler/may_postgres): connection loop sends responses on a per-request `spsc::Receiver`.

### Dependency

| Location | What |
|----------|------|
| [`Cargo.toml`](https://github.com/microscaler/may_redis/blob/main/Cargo.toml#L15) | `may = { git = "https://github.com/microscaler/may.git", branch = "may-sleep", ... }` until this PR merges |

### Call sites (`recv_with_timeout`)

| API | File | Role |
|-----|------|------|
| **`execute_with_timeout`** | [`src/client/client_timeout.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/client_timeout.rs#L36-L67) | Opens spsc channel per command, sends `Request` to connection loop, **`rx.recv_with_timeout(timeout)`** on response |
| **`execute` / default timeout** | [`src/client/client.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/client.rs#L248-L253) | Delegates every command to `execute_with_timeout(cmd, default_timeout)` (30s default) |
| **`recv_message_timeout`** | [`src/client/pubsub_client.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/pubsub_client.rs#L118-L133) | Push-message channel from connection loop; replaces prior `try_recv` + deadline spin loop |

Error mapping in may-redis:

- `RecvError::Timeout` → `RedisError::Connection("timeout")` ([`client_timeout.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/client_timeout.rs#L11-L17))
- Pub/sub timeout → `Connection("pub/sub recv timed out after …")` ([`pubsub_client.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/pubsub_client.rs#L125-L128))

### End-to-end flow (command timeout)

```
RedisClient::execute_with_timeout
  → connection.send(Request { data, spsc::Sender })
  → connection-loop coroutine writes to Redis, reads RESP, tx.send(response)
  → rx.recv_with_timeout(timeout)   ← this PR
  → FromRedisValue decode
```

Pub/sub uses a dedicated push channel populated by the connection loop when Redis sends out-of-band messages ([`PubSubClient::connect_for_pubsub`](https://github.com/microscaler/may_redis/blob/main/src/client/pubsub_client.rs#L34-L45)).

### Tests in may-redis

| Test | File | Proves |
|------|------|--------|
| `test_integration_execute_with_timeout_fires` | [`integration_timeout.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/client_tests/integration_timeout.rs#L9-L34) | BLPOP blocks server-side; client returns within ~100ms with `"timeout"` |
| `test_integration_pubsub_recv_message_timeout_fires` | [`integration_timeout.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/client_tests/integration_timeout.rs#L37-L61) | Subscribed channel idle; `recv_message_timeout(100ms)` returns promptly |
| TLS / execute timeout scenarios | [`tls_tests/execute_timeout.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/client_tests/tls_tests/execute_timeout.rs) | Happy-path commands under `execute_with_timeout` |

### Related docs (may-redis)

- [`docs/timeout.md`](./timeout.md) (this repo) — PRD §10 downstream integration
- [Epic 14 integration gap analysis](https://github.com/microscaler/may_redis/blob/main/docs/Epics/Epic_14/INTEGRATION_GAP_ANALYSIS.md) — `execute_with_timeout` scenarios
- [llmwiki: may-coroutine pattern](https://github.com/microscaler/may_redis/blob/main/llmwiki/topics/may-coroutine-pattern.md) — prefer `recv()` / `recv_with_timeout` over `try_recv` spin loops

### Not yet wired (follow-up)

- [`Pipeline::execute_raw`](https://github.com/microscaler/may_redis/blob/main/src/client/pipeline.rs) — still uses unbounded `rx.recv()` per pipelined response; needs per-response `recv_with_timeout` if pipeline timeouts are exposed

---

## Problem

`spsc::recv()` blocks forever. Callers that need deadlines today must:

- Ignore the timeout and block anyway (**may-redis today**), or
- Poll with `try_recv()` + `yield_now()` + wall-clock checks (cooperative but not scheduler-integrated, flaky under load), or
- Use OS `thread::sleep` inside coroutines (blocks threads, not cooperative).

None of these are acceptable for production Redis client timeouts or Sesame-IDAM migration.

---

## Solution

### API

```rust
pub enum RecvError {
    Disconnected,
    Timeout, // new
}

impl<T> Receiver<T> {
    pub fn recv_with_timeout(&self, timeout: Duration) -> Result<T, RecvError>;
}
```

| Result | Meaning |
|--------|---------|
| `Ok(T)` | Message received before deadline |
| `Err(RecvError::Timeout)` | Deadline elapsed, queue empty |
| `Err(RecvError::Disconnected)` | All senders dropped |

Blocking `recv()` / `try_recv()` behavior is unchanged.

### Implementation

- **Coroutine path:** `EventSource` (`RecvTimeoutSource`) + `yield_with` + `Scheduler::add_timer`
- **Shared wakeup:** `timeout_wait: Arc<AtomicOption<CoroutineImpl>>` lets **both** the timer and `InnerQueue::send()` race safely (`wait_co.take()` / `Arc::take()` — at most one wakeup)
- **Thread path:** existing `wait_co` blocker + `thread::park_timeout` (unchanged pattern)
- **Cancel:** registers with `co_cancel_data` like `sleep` / `Park`

See [`docs/timeout.md`](./timeout.md) for architecture diagrams, race analysis, and downstream wiring notes.

---

## Files changed

| File | Change |
|------|--------|
| `src/sync/spsc.rs` | `RecvError`, `RecvTimeoutSource`, `recv_with_timeout`, `timeout_wait`, 24 new unit tests + all 41 pre-existing active tests |
| `docs/timeout.md` | PRD / design doc |
| `docs/PR.md` | This document |

---

## Test plan

### Unit tests (`may`)

```bash
cargo test sync::spsc::tests::recv_with_timeout
# 24 tests — coroutine + thread paths
```

| Scenario | Covered |
|----------|---------|
| Fast path (data already queued) | ✅ |
| Timer expiry (~50ms, bounded elapsed) | ✅ |
| Delayed send before deadline | ✅ |
| `Duration::ZERO` | ✅ |
| Disconnect (immediate + during wait) | ✅ |
| Send unblocks parked receiver (race with timer) | ✅ |
| Coroutine cancel during wait | ✅ |
| Timeout → late send → next recv OK | ✅ |
| Alternating `recv_with_timeout` / `recv()` | ✅ |
| Stress: 50× timeout/send cycles (`RUST_TEST_STRESS`) | ✅ |
| Thread-path mirrors | ✅ |

Full suite:

```bash
cargo test --all-features
```

### Downstream ([may-redis](https://github.com/microscaler/may_redis), companion PR)

Consumes this branch via git dependency (see [Dependency](#dependency) above). After this PR merges, pin `rev = "<merge commit>"` instead of `branch = "may-sleep"`.

```bash
cargo test --features test --lib client::client_tests::integration_timeout -- --test-threads=1
```

| Integration test | Link | Scenario |
|------------------|------|----------|
| `test_integration_execute_with_timeout_fires` | [`integration_timeout.rs#L9`](https://github.com/microscaler/may_redis/blob/main/src/client/client_tests/integration_timeout.rs#L9-L34) | BLPOP stall, 100ms client timeout |
| `test_integration_pubsub_recv_message_timeout_fires` | [`integration_timeout.rs#L37`](https://github.com/microscaler/may_redis/blob/main/src/client/client_tests/integration_timeout.rs#L37-L61) | Idle subscribe, 100ms timeout |

---

## Breaking changes

**None** for existing callers.

- `recv()`, `try_recv()`, `send()` unchanged
- New public types: `RecvError` (spsc-specific), `recv_with_timeout`
- Internal: `std::sync::mpsc::RecvError` import renamed to `StdRecvError` (not public)

---

## Rollout

1. **Merge this PR** to [`microscaler/may`](https://github.com/microscaler/may) `master`
2. Tag / bump [`may-redis`](https://github.com/microscaler/may_redis) dependency off `branch = "may-sleep"` to `rev` or crates.io when published
3. Merge [may-redis companion PR](https://github.com/microscaler/may_redis) — [`client_timeout.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/client_timeout.rs), [`pubsub_client.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/pubsub_client.rs), [`integration_timeout.rs`](https://github.com/microscaler/may_redis/blob/main/src/client/client_tests/integration_timeout.rs)

---

## Checklist

- [x] `recv_with_timeout` coroutine + thread paths
- [x] Timer integrated via scheduler (not spin)
- [x] 24 unit tests passing
- [x] Design doc (`docs/timeout.md`)
- [ ] Reviewer: confirm `timeout_wait` + timer race handling
- [ ] CI green on `may-sleep`
- [ ] may-redis follow-up PR after merge
