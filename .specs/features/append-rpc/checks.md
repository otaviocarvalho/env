# WriteFile append flag - checks

Profile: light

## Intent

Guest `WriteFile` today opens `O_TRUNC` unconditionally, so every write replaces the file.
Long-lived session transcripts (E-03/E-04) must grow line by line across many writes; without
append, each poll would erase the transcript it is writing. This feature adds a `bool append`
field (proto field 4) to `WriteFileRequest`: when set on the first stream message the guest
opens `O_CREATE|O_WRONLY|O_APPEND` instead of `O_CREATE|O_WRONLY|O_TRUNC`. The api proxies
`WriteFileRequest` opaquely, so no api-side code changes. No one-way doors: additive proto
field, additive flag branch, existing default behavior pinned by a regression test.

1. WHEN append=true writes to an existing file THEN the service SHALL preserve prior bytes and write new chunks after them (bytes_written counts only new bytes)
2. WHEN append=true writes across multiple stream chunks THEN the service SHALL write every chunk in order after the existing content
3. WHEN append=true targets a nonexistent path THEN the service SHALL create the file with the requested mode and write the chunks
4. WHEN append is unset (default false) on an existing file THEN the service SHALL truncate prior content (existing behavior preserved)
5. WHEN an append write crosses the client->api->router->guest boundary THEN the api SHALL forward the append flag unchanged and the guest SHALL append

## Checks

### S1 - Append semantics · 7 files · ~30k

**C1** - Append to an existing file preserves content and appends in order (AC 1, AC 2)
Proof: `go test ./guest/filesystem/ -run 'TestAppendPreservesExistingContent' -count=1 -v` -> `--- PASS`

**C2** - Append to a nonexistent path creates the file with the requested mode (AC 3)
Proof: `go test ./guest/filesystem/ -run 'TestAppendCreatesNewFile' -count=1 -v` -> `--- PASS` (asserts 0640 perms + content)

**C3** - Default write still truncates (AC 4)
Proof: `go test ./guest/filesystem/ -run 'TestDefaultWriteStillTruncates' -count=1 -v` -> `--- PASS`

**C4** - Append flag crosses the api proxy and appends at the guest (AC 5)
Proof: `go test ./internal/apiservice/ -run 'TestProxyAppendWrite' -count=1 -v` -> `--- PASS` (real guest server behind the router, metadata -> api -> guest)

**C5** - Generated code contains proto field 4 (`varint,4,opt,name=append`) and `GetAppend`, with zero regen drift elsewhere (AC 5)
Proof: `git diff --stat proto/` -> 2 files (`guest.proto` +3, `guest.pb.go` +14/-3), and `git diff proto/ateenv/v1alpha/guest.pb.go | grep -c "GetAppend"` -> 1; `go build ./...` exits 0

## Coverage

| Set (size) | Member -> proof | Unproven |
| --- | --- | --- |
| Write paths (2) | default truncate C3 · append C1 | - |
| Append targets (2) | existing file C1 · missing file C2 | - |
| Append stream shapes (2) | single chunk C2 · multi-chunk C1/C2 | - |
| Flag propagation hops (1 boundary) | api proxy C4 (real router + guest server) | - |

## Swept

- validation: n/a - flag is bool; missing path on first message already InvalidArgument (existing `TestWriteFileMissingPath`)
- failure modes: existing - permission/open failures return their existing status codes; append branch only changes open flags
- idempotency: n/a - file append is intentionally not idempotent (at-least-once transcript writes; duplicates are visible, not harmful)
- authorization: existing - rootDir confinement (`resolveAndValidatePath`) runs before the open, unchanged
- concurrency: existing - guest serves one actor per worker; concurrent appends to one file were already unsynchronized for writes
- data lifecycle: n/a - no new persistent state
- dependency failure: existing - disk errors surface as Internal from the shared write path
- state transitions: C1, C2, C3 (file content state before/after)
- observability: n/a - no new metrics/events; existing write path logging unchanged

## Handoff

- Single slice, one builder (done in session).
