// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Wedge bisector: warm vs fresh connection ReadFile on a missing path.
//
// S1: fresh conn per call, missing path  -> expected fast NotFound
// S2: one conn, good read then missing   -> reproduces the stall?
// S3: one conn, missing then missing     -> does the first error poison the conn?
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var endpoint = flag.String("endpoint", "localhost:8080", "ate-env api endpoint")
var mode = flag.String("mode", "s2", "s1|s2|s3")
var timeout = flag.Duration("timeout", 10*time.Second, "per-call timeout")
var count = flag.Int("n", 150, "op count for load mode")

// readResultQuiet is readResult without output; returns the outcome duration.
func readResultQuiet(fs ateenvv1alpha.FileSystemServiceClient, path string) time.Duration {
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", atespace)
	t0 := time.Now()
	st, err := fs.ReadFile(ctx, &ateenvv1alpha.ReadFileRequest{Path: path})
	if err != nil {
		return time.Since(t0)
	}
	for {
		if _, err := st.Recv(); err != nil {
			break
		}
	}
	return time.Since(t0)
}

const (
	atespace = "ate-env"
	envID    = "e03-probe"
)

const missing = "/tmp/definitely-missing-e03wedge.bin"

func dial() *grpc.ClientConn {
	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return conn
}

// readResult runs one ReadFile on the given conn and reports outcome.
func readResult(tag string, conn *grpc.ClientConn) {
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", atespace)
	fs := ateenvv1alpha.NewFileSystemServiceClient(conn)
	t0 := time.Now()
	st, err := fs.ReadFile(ctx, &ateenvv1alpha.ReadFileRequest{Path: missing})
	if err != nil {
		fmt.Printf("%s: err after %v: %v\n", tag, time.Since(t0).Round(time.Millisecond), status.Code(err))
		return
	}
	n := 0
	for {
		_, err := st.Recv()
		if err == io.EOF {
			fmt.Printf("%s: EOF after %v (%d bytes)\n", tag, time.Since(t0).Round(time.Millisecond), n)
			return
		}
		if err != nil {
			fmt.Printf("%s: err mid-stream after %v: %v\n", tag, time.Since(t0).Round(time.Millisecond), status.Code(err))
			return
		}
		n++
	}
}

func main() {
	flag.Parse()
	switch *mode {
	case "s1":
		for i := 1; i <= 3; i++ {
			conn := dial()
			readResult(fmt.Sprintf("s1.%d(fresh)", i), conn)
			conn.Close()
		}
	case "s2":
		conn := dial()
		defer conn.Close()
		fs := ateenvv1alpha.NewFileSystemServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", atespace)
		t0 := time.Now()
		wctx, wcancel := context.WithTimeout(context.Background(), *timeout)
		defer wcancel()
		wctx = metadata.AppendToOutgoingContext(wctx, "x-env-id", envID, "x-env-atespace", atespace)
		wcli, err := fs.WriteFile(wctx)
		if err != nil {
			fmt.Printf("s2.warm-good: write stream err: %v\n", err)
			return
		}
		if err := wcli.Send(&ateenvv1alpha.WriteFileRequest{Path: "/tmp/e03wedge-good.bin", Chunk: []byte("hello")}); err != nil {
			fmt.Printf("s2.warm-good: write send err: %v\n", err)
			return
		}
		if _, err := wcli.CloseAndRecv(); err != nil {
			fmt.Printf("s2.warm-good: write err: %v\n", err)
			return
		}
		st, err := fs.ReadFile(ctx, &ateenvv1alpha.ReadFileRequest{Path: "/tmp/e03wedge-good.bin"})
		if err != nil {
			fmt.Printf("s2.warm-good: err after %v: %v\n", time.Since(t0).Round(time.Millisecond), status.Code(err))
		} else {
			n := 0
			for {
				_, err := st.Recv()
				if err != nil {
					break
				}
				n++
			}
			fmt.Printf("s2.warm-good: EOF after %v (%d chunks)\n", time.Since(t0).Round(time.Millisecond), n)
		}
		readResult("s2.warm-missing", conn)
	case "s3":
		conn := dial()
		defer conn.Close()
		readResult("s3.missing-1", conn)
		readResult("s3.missing-2", conn)
		readResult("s3.missing-3", conn)
	case "getenv":
		conn := dial()
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", atespace)
		env := ateenvv1alpha.NewEnvironmentServiceClient(conn)
		resp, err := env.GetEnvironment(ctx, &ateenvv1alpha.GetEnvironmentRequest{Id: envID, Atespace: atespace})
		if err != nil {
			fmt.Printf("getenv: err: %v\n", status.Code(err))
			return
		}
		fmt.Printf("getenv: state=%s\n", resp.GetEnvironment().GetStatus())
	case "suspend":
		conn := dial()
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", atespace)
		env := ateenvv1alpha.NewEnvironmentServiceClient(conn)
		if _, err := env.SuspendEnvironment(ctx, &ateenvv1alpha.SuspendEnvironmentRequest{Id: envID, Atespace: atespace}); err != nil {
			fmt.Printf("suspend: err: %v\n", status.Code(err))
			return
		}
		fmt.Println("suspend: ok")
	case "load":
		// One conn, n sequential stream ops mixing StartProcess+stream-recv,
		// ReadFile(missing), ReadFile(good). Print per-op latency; flag any op
		// exceeding 5s (stream-slot exhaustion wedges at the server's per-conn
		// concurrent stream limit instead of erroring).
		n := 150
		flag.Parse()
		if *count > 0 {
			n = *count
		}
		conn := dial()
		defer conn.Close()
		fs := ateenvv1alpha.NewFileSystemServiceClient(conn)
		proc := ateenvv1alpha.NewProcessServiceClient(conn)
		for i := 1; i <= n; i++ {
			t0 := time.Now()
			pctx, pcancel := context.WithTimeout(context.Background(), 8*time.Second)
			pctx = metadata.AppendToOutgoingContext(pctx, "x-env-id", envID, "x-env-atespace", atespace)
			sr, err := proc.StartProcess(pctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"sh", "-c", "echo hi"}})
			if err == nil {
				st, err := proc.StreamProcessOutputs(pctx, &ateenvv1alpha.StreamProcessOutputsRequest{ProcessId: sr.GetProcessId()})
				if err == nil {
					for {
						if _, err := st.Recv(); err != nil {
							break
						}
					}
				}
			}
			pcancel()
			d1 := time.Since(t0)
			t1 := time.Now()
			readResultQuiet(fs, missing)
			d2 := time.Since(t1)
			worst := d1
			if d2 > worst {
				worst = d2
			}
			mark := ""
			if worst > 5*time.Second {
				mark = "  <-- WEDGE"
			}
			fmt.Printf("load i=%d proc=%v read=%v%s\n", i, d1.Round(time.Millisecond), d2.Round(time.Millisecond), mark)
			if mark != "" {
				fmt.Printf("load: wedged at i=%d after %v total\n", i, time.Since(t0).Round(time.Second))
				return
			}
		}
		fmt.Printf("load: completed %d ops, no wedge\n", n)
	case "soak":
		// Composite E-02/E-03 scenario: follow-stream open while polling reads,
		// then suspend mid-run; watch for any op that wedges (hits full timeout
		// on two consecutive attempts instead of erroring).
		conn := dial()
		defer conn.Close()
		fs := ateenvv1alpha.NewFileSystemServiceClient(conn)
		proc := ateenvv1alpha.NewProcessServiceClient(conn)
		mctx := func() context.Context {
			ctx, _ := context.WithTimeout(context.Background(), 30*time.Second) //nolint:govet
			return metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", atespace)
		}
		sr, err := proc.StartProcess(mctx(), &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sh", "-c", "for i in $(seq 1 40); do echo line-$i; sleep 0.05; done"}})
		if err != nil {
			fmt.Printf("soak: writer err: %v\n", err)
			return
		}
		followDone := make(chan error, 1)
		go func() {
			st, err := proc.StreamProcessOutputs(mctx(), &ateenvv1alpha.StreamProcessOutputsRequest{ProcessId: sr.GetProcessId(), Follow: true})
			if err != nil {
				followDone <- err
				return
			}
			for {
				if _, err := st.Recv(); err != nil {
					followDone <- err
					return
				}
			}
		}()
		suspended := false
		wedges := 0
		for i := 1; i <= 60; i++ {
			if i == 15 && !suspended {
				suspended = true
				sconn := dial()
				sctx, scancel := context.WithTimeout(context.Background(), 60*time.Second)
				sctx = metadata.AppendToOutgoingContext(sctx, "x-env-id", envID, "x-env-atespace", atespace)
				_, err := ateenvv1alpha.NewEnvironmentServiceClient(sconn).SuspendEnvironment(sctx, &ateenvv1alpha.SuspendEnvironmentRequest{Id: envID, Atespace: atespace})
				scancel()
				sconn.Close()
				fmt.Printf("soak: suspend mid-run err=%v\n", status.Code(err))
			}
			t0 := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			ctx = metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", atespace)
			st, err := fs.ReadFile(ctx, &ateenvv1alpha.ReadFileRequest{Path: "/tmp/definitely-missing-e03wedge.bin"})
			outcome := "NotFound"
			if err == nil {
				_, err = st.Recv()
				outcome = "stream"
			}
			if err == nil || status.Code(err) != codes.DeadlineExceeded {
				wedges = 0
				fmt.Printf("soak i=%d %v in %v (follow done=%v)\n", i, outcome, time.Since(t0).Round(time.Millisecond), followDone)
				if err == nil {
					for {
						if _, err := st.Recv(); err != nil {
							break
						}
					}
				}
				cancel()
				continue
			}
			// First timeout: retry once with a long window to see if it ever lands.
			cancel()
			t1 := time.Now()
			ctx2, cancel2 := context.WithTimeout(context.Background(), 45*time.Second)
			ctx2 = metadata.AppendToOutgoingContext(ctx2, "x-env-id", envID, "x-env-atespace", atespace)
			st2, err2 := fs.ReadFile(ctx2, &ateenvv1alpha.ReadFileRequest{Path: "/tmp/definitely-missing-e03wedge.bin"})
			if err2 == nil || status.Code(err2) != codes.DeadlineExceeded {
				fmt.Printf("soak i=%d slow-but-landed: %v after %v\n", i, status.Code(err2), time.Since(t1).Round(time.Millisecond))
				if err2 == nil {
					for {
						if _, err := st2.Recv(); err != nil {
							break
						}
					}
				}
				cancel2()
				continue
			}
			wedges++
			fmt.Printf("soak i=%d WEDGE: two consecutive timeouts (%v, then %v)\n", i, time.Since(t0).Round(time.Millisecond), time.Since(t1).Round(time.Millisecond))
			cancel2()
			if wedges >= 2 {
				fmt.Printf("soak: WEDGED at i=%d\n", i)
				return
			}
		}
		fmt.Println("soak: completed 60 reads, no wedge")
	default:
		panic("unknown mode " + *mode)
	}
}
