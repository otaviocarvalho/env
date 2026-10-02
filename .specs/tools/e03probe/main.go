// e03probe measures transcript-egress paths through ate-env (ADR E-03).
//
// Subcommands:
//
//	p1 [N] [rate_ms]               writer echoes N JSONL lines to stdout at the given
//	                               cadence; receiver consumes StreamProcessOutputs(Follow)
//	                               and reports arrival distribution vs nominal emit time.
//	p2 [N] [rate_ms] [interval_ms] writer appends N lines to /tmp/tr2.jsonl; poller
//	                               ReadFiles the whole file every interval.
//	p3 [N] [rate_ms] [interval_ms] writer rotates /tmp/seg/tr.K.jsonl every 40 lines;
//	                               poller reads only unseen segments until a miss.
//	micro                          round-trip costs: shell echo, StartProcess->first
//	                               chunk, ReadFile at 4KB/256KB/1MB.
//
// Results print as JSON lines on stdout.
//
// Line timestamps: the guest's busybox date has no sub-second resolution, so the
// writer emits lines at a KNOWN cadence (seq-encoded) and all latency/staleness
// math derives from receiver-side wall clock plus the nominal emit schedule.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
)

const (
	endpoint = "127.0.0.1:7777"
	atespace = "ate-env"
	envID    = "e03-probe"
	// ~230B body per line, realistic assistant-prose size.
	filler = `The transcript line carries assistant prose of realistic length; it exists to give every JSONL record a body near three hundred bytes for honest bandwidth math.`
)

func mdCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", atespace)
}

func emit(v map[string]any) {
	v["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

// writerScript emits N JSONL lines to stdout (redir == "-") or appends them to
// a path, one line every rateUs microseconds, embedding seq for receiver-side math.
func writerScript(n, rateUs int, target string) string {
	out := ""
	if target != "-" {
		out = "rm -f " + target + "\n"
	}
	line := `printf '{"seq":'"$i"',"content":"` + filler + `"}\n'`
	if target != "-" {
		line += " >> " + target
	}
	return out + `i=0
while [ $i -lt ` + strconv.Itoa(n) + ` ]; do
  ` + line + `
  i=$((i + 1))
  usleep ` + strconv.Itoa(rateUs) + `
done
`
}

// p1: transcript lines flow to the process's stdout; consumer holds the stream.
func p1(n, rateUs int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		emit(map[string]any{"probe": "p1", "error": err.Error()})
		return
	}
	defer conn.Close()
	proc := ateenvv1alpha.NewProcessServiceClient(conn)

	sr, err := proc.StartProcess(mdCtx(ctx), &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", writerScript(n, rateUs, "-")},
	})
	if err != nil {
		emit(map[string]any{"probe": "p1", "error": "start: " + err.Error()})
		return
	}
	stream, err := proc.StreamProcessOutputs(mdCtx(ctx), &ateenvv1alpha.StreamProcessOutputsRequest{ProcessId: sr.GetProcessId(), Follow: true})
	if err != nil {
		emit(map[string]any{"probe": "p1", "error": "stream: " + err.Error()})
		return
	}

	t0 := time.Now()
	chunks, lines, bytesTotal, lastSeq := 0, 0, 0, -1
	var maxInterChunkMS float64
	prev := t0
	for {
		ch, err := stream.Recv()
		if err != nil {
			break
		}
		now := time.Now()
		chunks++
		d := ch.GetData()
		bytesTotal += len(d)
		lines += bytes.Count(d, []byte("\n"))
		if s := maxSeqIn(d); s > lastSeq {
			if lastSeq >= 0 {
				if g := now.Sub(prev).Seconds() * 1e3; g > maxInterChunkMS {
					maxInterChunkMS = g
				}
			}
			lastSeq = s
			prev = now
		}
	}
	durS := time.Since(t0).Seconds()
	nominal := float64(n) * float64(rateUs) / 1e6
	emit(map[string]any{"probe": "p1", "event": "summary", "lines": lines, "chunks": chunks,
		"bytes": bytesTotal, "dur_s": durS, "nominal_s": nominal, "lag_s": durS - nominal,
		"last_seq": lastSeq, "max_interchunk_ms": maxInterChunkMS})
}

func maxSeqIn(d []byte) int {
	max, off := -1, 0
	for {
		p := bytes.Index(d[off:], []byte(`"seq":`))
		if p < 0 {
			return max
		}
		k := off + p + 6
		v := 0
		for k < len(d) && d[k] >= '0' && d[k] <= '9' {
			v = v*10 + int(d[k]-'0')
			k++
		}
		if v > max {
			max = v
		}
		off = k
	}
}

// readWhole pulls an entire file via ReadFile. Returns bytes, lines, maxSeq, ms.
// A stalled stream (observed once after env resume) aborts via per-read timeout.
func readWhole(ctx context.Context, fs ateenvv1alpha.FileSystemServiceClient, path string) (int, int, int, float64, error) {
	t := time.Now()
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	stream, err := fs.ReadFile(mdCtx(rctx), &ateenvv1alpha.ReadFileRequest{Path: path})
	if err != nil {
		return 0, 0, -1, 0, err
	}
	var buf bytes.Buffer
	var rcvErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ch, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				rcvErr = err
				return
			}
			buf.Write(ch.GetData())
		}
	}()
	select {
	case <-done:
	case <-rctx.Done():
		return 0, 0, -1, 0, fmt.Errorf("readfile stalled: %w", rctx.Err())
	}
	if rcvErr != nil {
		return 0, 0, -1, 0, rcvErr
	}
	ms := time.Since(t).Seconds() * 1e3
	data := buf.Bytes()
	return buf.Len(), bytes.Count(data, []byte("\n")), maxSeqIn(data), ms, nil
}

// p2: transcript appended to one file; poller re-reads the WHOLE file each interval.
func p2(n, rateUs, intervalMs int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	conn, _ := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	proc := ateenvv1alpha.NewProcessServiceClient(conn)
	fs := ateenvv1alpha.NewFileSystemServiceClient(conn)

	if sr, err := proc.StartProcess(mdCtx(ctx), &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", writerScript(n, rateUs, "/tmp/tr2.jsonl")},
	}); err != nil {
		emit(map[string]any{"probe": "p2", "error": "writer: " + err.Error()})
		return
	} else {
		emit(map[string]any{"probe": "p2", "event": "writer_started", "pid": sr.GetProcessId()})
	}

	t0 := time.Now()
	polls, wireBytes, maxSeq := 0, 0, -1
	var caughtAt time.Time
	for time.Since(t0) < 10*time.Minute {
		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
		sz, _, seq, ms, err := readWhole(ctx, fs, "/tmp/tr2.jsonl")
		if err != nil {
			continue // not created yet
		}
		polls++
		wireBytes += sz
		if seq > maxSeq {
			maxSeq = seq
		}
		emit(map[string]any{"probe": "p2", "event": "poll", "i": polls, "read_ms": ms, "bytes": sz, "max_seq": seq})
		if seq >= n-1 {
			caughtAt = time.Now()
			break
		}
	}
	if polls == 0 {
		emit(map[string]any{"probe": "p2", "error": "no successful poll"})
		return
	}
	emit(map[string]any{"probe": "p2", "event": "summary", "polls": polls, "wire_bytes": wireBytes,
		"final_seq": maxSeq, "dur_s": time.Since(t0).Seconds(), "caught_after_s": since(t0, caughtAt),
		"note": "wire_bytes grows O(n): every poll re-reads the full transcript"})
}

// fileExists probes path presence via a shell round-trip (~20ms). Used instead of
// relying on ReadFile NotFound: on a warm connection a missing-path ReadFile can
// stall until timeout instead of erroring (alpha bug, see ADR E-03).
func fileExists(ctx context.Context, proc ateenvv1alpha.ProcessServiceClient, path string) bool {
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	sr, err := proc.StartProcess(mdCtx(pctx), &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", "[ -f " + path + " ] && echo y || echo n"}})
	if err != nil {
		return false
	}
	st, err := proc.StreamProcessOutputs(mdCtx(pctx), &ateenvv1alpha.StreamProcessOutputsRequest{ProcessId: sr.GetProcessId()})
	if err != nil {
		return false
	}
	var out []byte
	for {
		ch, err := st.Recv()
		if err != nil {
			break
		}
		out = append(out, ch.GetData()...)
	}
	return len(out) > 0 && out[0] == 'y'
}

// p3: rotated segments; poller reads only unseen segment files until a miss.
func p3(n, rateUs, intervalMs int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	conn, _ := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	proc := ateenvv1alpha.NewProcessServiceClient(conn)
	fs := ateenvv1alpha.NewFileSystemServiceClient(conn)

	if _, err := proc.StartProcess(mdCtx(ctx), &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", "rm -rf /tmp/seg; mkdir -p /tmp/seg"}}); err != nil {
		emit(map[string]any{"probe": "p3", "error": "clean: " + err.Error()})
		return
	}
	if _, err := proc.StartProcess(mdCtx(ctx), &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", writerScript(n, rateUs, `"/tmp/seg/tr.$((i / 40)).jsonl"`)},
	}); err != nil {
		emit(map[string]any{"probe": "p3", "error": "writer: " + err.Error()})
		return
	}

	t0 := time.Now()
	polls, missPolls, wireBytes, maxSeq := 0, 0, 0, -1
	nextSeg := 0
	var caughtAt time.Time
	seg := func(k int) string { return fmt.Sprintf("/tmp/seg/tr.%d.jsonl", k) }
	for time.Since(t0) < 10*time.Minute {
		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
		newBytes, newMs := 0, 0.0
		// Re-read from nextSeg; the tail segment may still be growing, so advance
		// past segment K only once segment K+1 already exists.
		for {
			if !fileExists(ctx, proc, seg(nextSeg)) {
				missPolls++
				break
			}
			sz, _, seq, ms, err := readWhole(ctx, fs, seg(nextSeg))
			if err != nil || sz == 0 {
				missPolls++
				break
			}
			polls++
			wireBytes += sz
			newBytes += sz
			newMs += ms
			if seq > maxSeq {
				maxSeq = seq
			}
			if maxSeq >= n-1 {
				caughtAt = time.Now()
				break
			}
			if !fileExists(ctx, proc, seg(nextSeg+1)) {
				break
			}
			nextSeg++
		}
		if newBytes > 0 {
			emit(map[string]any{"probe": "p3", "event": "poll", "i": polls, "read_ms": newMs, "bytes": newBytes, "next_seg": nextSeg})
		}
		if !caughtAt.IsZero() {
			break
		}
	}
	emit(map[string]any{"probe": "p3", "event": "summary", "polls": polls, "miss_polls": missPolls,
		"wire_bytes": wireBytes, "next_seg": nextSeg, "final_seq": maxSeq, "dur_s": time.Since(t0).Seconds(),
		"caught_after_s": since(t0, caughtAt),
		"note": "wire_bytes ~O(new data only); miss polls are one cheap RPC each"})
}

func since(t0, t time.Time) float64 {
	if t.IsZero() {
		return -1
	}
	return t.Sub(t0).Seconds()
}

// micro: per-call round-trip cost through api -> atenet router -> guest.
func micro() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn, _ := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	proc := ateenvv1alpha.NewProcessServiceClient(conn)
	fs := ateenvv1alpha.NewFileSystemServiceClient(conn)

	// payload files for ReadFile bench
	for _, sz := range []int{4096, 262144, 1048576} {
		ws, err := fs.WriteFile(mdCtx(ctx))
		if err == nil {
			err = ws.Send(&ateenvv1alpha.WriteFileRequest{
				Path: fmt.Sprintf("/tmp/bench_%d.bin", sz), Chunk: bytes.Repeat([]byte("x"), sz), Mode: 0o644})
		}
		if err == nil {
			_, err = ws.CloseAndRecv()
		}
		if err != nil {
			emit(map[string]any{"probe": "micro", "error": "setup write: " + err.Error()})
			return
		}
	}

	// full shell round-trip (start + stream to EOF) x20
	var shellMS []float64
	for i := 0; i < 20; i++ {
		t := time.Now()
		sr, err := proc.StartProcess(mdCtx(ctx), &ateenvv1alpha.StartProcessRequest{Command: []string{"sh", "-c", "echo hi"}})
		if err != nil {
			emit(map[string]any{"probe": "micro", "error": err.Error()})
			return
		}
		st, err := proc.StreamProcessOutputs(mdCtx(ctx), &ateenvv1alpha.StreamProcessOutputsRequest{ProcessId: sr.GetProcessId()})
		if err != nil {
			emit(map[string]any{"probe": "micro", "error": err.Error()})
			return
		}
		for {
			if _, err := st.Recv(); err != nil {
				break
			}
		}
		shellMS = append(shellMS, time.Since(t).Seconds()*1e3)
	}
	emit(map[string]any{"probe": "micro", "op": "shell_roundtrip_x20", "mean_ms": mean(shellMS), "max_ms": maxF(shellMS)})

	// ReadFile sizes x10 each
	for _, sz := range []int{4096, 262144, 1048576} {
		var ms []float64
		for i := 0; i < 10; i++ {
			_, _, _, m, err := readWhole(ctx, fs, fmt.Sprintf("/tmp/bench_%d.bin", sz))
			if err != nil {
				emit(map[string]any{"probe": "micro", "error": err.Error()})
				return
			}
			ms = append(ms, m)
		}
		m := mean(ms)
		emit(map[string]any{"probe": "micro", "op": "readfile_x10", "bytes": sz, "mean_ms": m, "max_ms": maxF(ms),
			"mbps": float64(sz) / 1e6 / (m / 1e3)})
	}
}

func maxF(xs []float64) float64 {
	m := xs[0]
	for _, x := range xs {
		if x > m {
			m = x
		}
	}
	return m
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: e03probe p1|p2|p3|micro [N] [rate_ms] [interval_ms]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "p1":
		p1(atoiOr(os.Args, 2, 300), atoiOr(os.Args, 3, 50)*1000)
	case "p2":
		p2(atoiOr(os.Args, 2, 300), atoiOr(os.Args, 3, 50)*1000, atoiOr(os.Args, 4, 1000))
	case "p3":
		p3(atoiOr(os.Args, 2, 300), atoiOr(os.Args, 3, 50)*1000, atoiOr(os.Args, 4, 1000))
	case "micro":
		micro()
	}
}

func atoiOr(args []string, i, def int) int {
	if i < len(args) {
		if v, err := strconv.Atoi(args[i]); err == nil {
			return v
		}
	}
	return def
}
