# PR: `recv_with_timeout` for `may::sync::spsc`

**Branch:** `may-sleep` → `master`  
**Tracking issue:** may-redis timeout API (blocked on this merge)

---

## Summary

Adds `Receiver::recv_with_timeout(Duration) -> Result<T, RecvError>` to `may::sync::spsc`, integrating with the scheduler timer wheel so coroutines can wait on an spsc response channel with a bounded deadline—without spin loops or ignored timeout parameters.

This unblocks **may-redis** (`execute_with_timeout`, pub/sub `recv_message_timeout`) and any other may consumer using the may_postgres request/response spsc pattern.

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
| `src/sync/spsc.rs` | `RecvError`, `RecvTimeoutSource`, `recv_with_timeout`, `timeout_wait`, 24 unit tests |
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

### Downstream (`may-redis`, separate PR after merge)

may-redis on branch `main` (staged, not yet merged) consumes:

```toml
may = { git = "https://github.com/microscaler/may.git", branch = "may-sleep", ... }
```

After this PR merges, may-redis should pin to a released version or `rev = "<merge commit>"`.

```bash
cargo test --features test --lib client::client_tests::integration_timeout -- --test-threads=1
```

- `test_integration_execute_with_timeout_fires` — BLPOP stall, 100ms client timeout
- `test_integration_pubsub_recv_message_timeout_fires` — idle subscribe, 100ms timeout

---

## Breaking changes

**None** for existing callers.

- `recv()`, `try_recv()`, `send()` unchanged
- New public types: `RecvError` (spsc-specific), `recv_with_timeout`
- Internal: `std::sync::mpsc::RecvError` import renamed to `StdRecvError` (not public)

---

## Rollout

1. **Merge this PR** to `microscaler/may` `master`
2. Tag / bump may-redis dependency off `branch = "may-sleep"` to `rev` or crates.io when published
3. Merge may-redis timeout wiring PR (`execute_with_timeout`, `recv_message_timeout`, integration tests)

---

## Checklist

- [x] `recv_with_timeout` coroutine + thread paths
- [x] Timer integrated via scheduler (not spin)
- [x] 24 unit tests passing
- [x] Design doc (`docs/timeout.md`)
- [ ] Reviewer: confirm `timeout_wait` + timer race handling
- [ ] CI green on `may-sleep`
- [ ] may-redis follow-up PR after merge
