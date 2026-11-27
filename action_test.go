package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mozership/probe-slurm/internal/runner"
	"github.com/mozership/probe/sdk"
)

// cluster stands in for Slurm: it answers each command by its name, and
// records the commands and where they ran.
type cluster struct {
	mu      sync.Mutex
	answer  func(argv []string, stdin []byte) runner.Output
	ran     [][]string
	stdin   []string
	dialled []connection
}

func (c *cluster) Run(_ context.Context, argv []string, stdin []byte) (runner.Output, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ran = append(c.ran, argv)
	c.stdin = append(c.stdin, string(stdin))
	return c.answer(argv, stdin), nil
}

func (c *cluster) Close() error { return nil }

func newAction(t *testing.T, c *cluster) *Action {
	t.Helper()
	return &Action{
		getenv: func(k string) string {
			if k == "USER" {
				return "alice"
			}
			return ""
		},
		dial: func(_ context.Context, conn connection, _ func(string) string) (runner.Runner, error) {
			c.dialled = append(c.dialled, conn)
			return c, nil
		},
		ledgerDir: t.TempDir(),
	}
}

func queueJSON(id int64, state string, exit int) []byte {
	return fmt.Appendf(nil, `{"jobs": [{"job_id": %d, "name": "probe", "user_name": "alice", "partition": "debug",
		"job_state": [%q], "state_reason": "None", "nodes": "c1",
		"submit_time": {"set": true, "number": 1764000000}, "start_time": {"set": true, "number": 1764000004},
		"end_time": {"set": true, "number": 1764000010},
		"exit_code": {"return_code": {"set": true, "number": %d}, "signal": {"id": {"set": false, "number": 0}}},
		"standard_output": "slurm-%%j.out", "current_working_directory": "/home/alice"}], "errors": []}`, id, state, exit)
}

func TestRunStep_submit(t *testing.T) {
	c := &cluster{answer: func(argv []string, _ []byte) runner.Output {
		return runner.Output{Stdout: []byte("123\n")}
	}}
	a := newAction(t, c)
	ret, _, err := a.RunStep(sdk.Call{
		With: map[string]any{
			"host": "login.example.com",
			"submit": map[string]any{
				"script":    "#!/bin/bash\nsrun hostname\n",
				"partition": "debug",
				"time":      "1h30m",
				"nodes":     float64(2),
				"options":   []any{"--exclusive"},
			},
		},
		Step: sdk.Step{RunID: "run-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["job_id"] != int64(123) || ret["status"] != 0 {
		t.Errorf("RunStep() = %v", ret)
	}
	want := []string{"sbatch", "--parsable", "--job-name=probe", "--partition=debug", "--time=01:30:00", "--nodes=2", "--exclusive"}
	if !reflect.DeepEqual(c.ran[0], want) {
		t.Errorf("ran %q, want %q", c.ran[0], want)
	}
	if c.stdin[0] != "#!/bin/bash\nsrun hostname\n" {
		t.Errorf("sent the script %q", c.stdin[0])
	}
	if c.dialled[0] != (connection{Host: "login.example.com", Port: 22, User: "alice"}) {
		t.Errorf("dialled %+v", c.dialled[0])
	}
	req := ret["req"].(map[string]any)
	if req["command"] != "sbatch --parsable --job-name=probe --partition=debug --time=01:30:00 --nodes=2 --exclusive" {
		t.Errorf("req.command = %v", req["command"])
	}

	// The run is over: its cleanup cancels the job, where it was submitted.
	c.answer = func([]string, []byte) runner.Output { return runner.Output{} }
	if err := a.Cleanup(sdk.CleanupCall{RunID: "run-1"}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"scancel", "123"}; !reflect.DeepEqual(c.ran[1], want) {
		t.Errorf("cleanup ran %q", c.ran[1])
	}
	if c.dialled[1] != c.dialled[0] {
		t.Errorf("cleanup dialled %+v", c.dialled[1])
	}
	// Called again, it has nothing left to do.
	if err := a.Cleanup(sdk.CleanupCall{RunID: "run-1"}); err != nil || len(c.ran) != 2 {
		t.Errorf("second Cleanup() = %v, ran %q", err, c.ran)
	}
}

func TestRunStep_submitKeep(t *testing.T) {
	c := &cluster{answer: func([]string, []byte) runner.Output { return runner.Output{Stdout: []byte("5\n")} }}
	a := newAction(t, c)
	script := filepath.Join(t.TempDir(), "job.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrue\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	with := map[string]any{"submit": map[string]any{"file": script, "keep": true, "name": "kept"}}
	if _, _, err := a.RunStep(sdk.Call{With: with, Step: sdk.Step{RunID: "run-2"}}); err != nil {
		t.Fatal(err)
	}
	if c.stdin[0] != "#!/bin/sh\ntrue\n" {
		t.Errorf("sent %q", c.stdin[0])
	}
	if err := a.Cleanup(sdk.CleanupCall{RunID: "run-2"}); err != nil || len(c.ran) != 1 {
		t.Errorf("Cleanup() = %v, ran %q", err, c.ran)
	}
}

func TestRunStep_submitRefused(t *testing.T) {
	c := &cluster{answer: func([]string, []byte) runner.Output {
		return runner.Output{Code: 1, Stderr: []byte("sbatch: error: invalid partition specified: nope\n")}
	}}
	ret, _, err := newAction(t, c).RunStep(sdk.Call{With: map[string]any{"submit": map[string]any{"script": "#!/bin/sh\n", "partition": "nope"}}})
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["error"] != "sbatch: error: invalid partition specified: nope" || ret["status"] != 1 {
		t.Errorf("RunStep() = %v", ret)
	}
}

func TestRunStep_jobWaits(t *testing.T) {
	states := []string{"PENDING", "RUNNING", "COMPLETED"}
	c := &cluster{}
	c.answer = func(argv []string, _ []byte) runner.Output {
		switch argv[0] {
		case "squeue":
			state := states[0]
			if len(states) > 1 {
				states = states[1:]
			}
			return runner.Output{Stdout: queueJSON(9, state, 0)}
		case "head":
			return runner.Output{Stdout: []byte("c1\n")}
		}
		return runner.Output{Code: 127}
	}
	a := newAction(t, c)
	ret, _, err := a.RunStep(sdk.Call{With: map[string]any{"job": "9", "wait": "1m", "poll": "1s", "output": true}})
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["state"] != "COMPLETED" || res["done"] != true || ret["status"] != 0 {
		t.Errorf("RunStep() = %v", ret)
	}
	if res["wait_sec"] != int64(4) || res["elapsed_sec"] != int64(6) || res["start_time"] != "2025-11-24T16:00:04Z" {
		t.Errorf("times in %v", res)
	}
	if res["stdout"] != "c1\n" || res["stdout_truncated"] != false || res["stdout_path"] != "/home/alice/slurm-9.out" {
		t.Errorf("output in %v", res)
	}
	if n := len(c.ran); n != 4 {
		t.Errorf("ran %q", c.ran)
	}
}

func TestRunStep_jobWithoutWait(t *testing.T) {
	c := &cluster{answer: func([]string, []byte) runner.Output { return runner.Output{Stdout: queueJSON(9, "RUNNING", 0)} }}
	start := time.Now()
	ret, _, err := newAction(t, c).RunStep(sdk.Call{With: map[string]any{"job": float64(9)}})
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["state"] != "RUNNING" || res["done"] != false || ret["status"] != 1 || time.Since(start) > 5*time.Second {
		t.Errorf("RunStep() = %v", ret)
	}
	if _, ok := res["stdout"]; ok {
		t.Errorf("res has stdout without output: %v", res)
	}
}

func TestRunStep_jobFailed(t *testing.T) {
	c := &cluster{answer: func([]string, []byte) runner.Output { return runner.Output{Stdout: queueJSON(9, "FAILED", 2)} }}
	ret, _, err := newAction(t, c).RunStep(sdk.Call{With: map[string]any{"job": 9, "wait": "10s"}})
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["exit_code"] != int64(2) || ret["status"] != 1 || len(c.ran) != 1 {
		t.Errorf("RunStep() = %v, ran %q", ret, c.ran)
	}
}

func TestRunStep_cancel(t *testing.T) {
	c := &cluster{answer: func(argv []string, _ []byte) runner.Output {
		if argv[0] == "squeue" {
			return runner.Output{Stdout: queueJSON(77, "RUNNING", 0)}
		}
		return runner.Output{}
	}}
	ret, _, err := newAction(t, c).RunStep(sdk.Call{With: map[string]any{"cancel": "77"}})
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["cancelled"] != true || res["job_id"] != int64(77) || res["state"] != "RUNNING" || ret["status"] != 0 {
		t.Errorf("RunStep() = %v", ret)
	}
}

func TestRunStep_partitionsAndNodes(t *testing.T) {
	c := &cluster{answer: func(argv []string, _ []byte) runner.Output {
		if argv[3] == "partition" {
			return runner.Output{Stdout: []byte(`{"partitions": [{"name": "debug", "partition": {"state": ["UP"]}, "nodes": {"total": 2}}]}`)}
		}
		return runner.Output{Stdout: []byte(`{"nodes": [{"name": "c1", "state": ["IDLE"], "partitions": ["debug"], "cpus": 2}, {"name": "c2", "state": ["DOWN", "NOT_RESPONDING"], "partitions": ["debug"], "reason": "gone", "cpus": 2}]}`)}
	}}
	a := newAction(t, c)
	ret, _, err := a.RunStep(sdk.Call{With: map[string]any{"partitions": "debug", "bin_dir": "/opt/slurm/bin"}})
	if err != nil {
		t.Fatal(err)
	}
	p := ret["res"].(map[string]any)["partitions"].([]any)[0].(map[string]any)
	if p["state"] != "UP" || p["idle"] != int64(1) || p["down"] != int64(1) || p["total_nodes"] != int64(2) {
		t.Errorf("partition = %v", p)
	}
	if c.ran[0][0] != "/opt/slurm/bin/scontrol" {
		t.Errorf("ran %q", c.ran[0])
	}

	ret, _, err = a.RunStep(sdk.Call{With: map[string]any{"nodes": nil}})
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	n := res["nodes"].([]any)[1].(map[string]any)
	if res["count"] != 2 || n["state"] != "DOWN" || !reflect.DeepEqual(n["flags"], []any{"NOT_RESPONDING"}) {
		t.Errorf("RunStep() = %v", res)
	}
}

func TestRunStep_jobs(t *testing.T) {
	c := &cluster{answer: func([]string, []byte) runner.Output { return runner.Output{Stdout: queueJSON(3, "RUNNING", 0)} }}
	ret, _, err := newAction(t, c).RunStep(sdk.Call{With: map[string]any{"jobs": map[string]any{"user": "alice", "state": "running"}}})
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["count"] != 1 {
		t.Errorf("RunStep() = %v", res)
	}
	if want := []string{"squeue", "--json", "--user=alice", "--states=RUNNING"}; !reflect.DeepEqual(c.ran[0], want) {
		t.Errorf("ran %q", c.ran[0])
	}
}

func TestRunStep_guard(t *testing.T) {
	tests := []struct {
		name    string
		with    map[string]any
		guard   sdk.Guard
		refused string
	}{
		{"submit under read-only", map[string]any{"host": "login", "submit": map[string]any{"script": "x"}}, sdk.Guard{ReadOnly: true}, "the run is read-only"},
		{"cancel under read-only", map[string]any{"cancel": 1}, sdk.Guard{ReadOnly: true}, "the run is read-only"},
		{"a host not allowed", map[string]any{"host": "login", "job": 1}, sdk.Guard{AllowHosts: []string{"other"}}, "not one the run allows"},
		{"no host under allow-host", map[string]any{"job": 1}, sdk.Guard{AllowHosts: []string{"login"}}, "give the login node as host"},
		{"job under read-only", map[string]any{"job": 1}, sdk.Guard{ReadOnly: true}, ""},
		{"a host allowed", map[string]any{"host": "login", "port": 2222, "nodes": nil}, sdk.Guard{AllowHosts: []string{"login:2222"}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster{answer: func(argv []string, _ []byte) runner.Output {
				if argv[0] == "squeue" {
					return runner.Output{Stdout: queueJSON(1, "RUNNING", 0)}
				}
				return runner.Output{Stdout: []byte(`{"nodes": []}`)}
			}}
			_, _, err := newAction(t, c).RunStep(sdk.Call{With: tt.with, Guard: tt.guard})
			if tt.refused == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !sdk.IsRefused(err) || !strings.Contains(err.Error(), tt.refused) {
				t.Fatalf("RunStep() error = %v, want refusal %q", err, tt.refused)
			}
			if len(c.ran) != 0 {
				t.Errorf("ran %q", c.ran)
			}
		})
	}
}

func TestParseStep_errors(t *testing.T) {
	tests := []struct {
		name string
		with map[string]any
		want string
	}{
		{"nothing", map[string]any{}, "requires exactly one of with.submit"},
		{"two operations", map[string]any{"job": 1, "cancel": 1}, "not job and cancel together"},
		{"submit and another operation", map[string]any{"job": 1, "submit": map[string]any{"script": "x"}}, "not submit and job together"},
		{"defaults of submit alone", map[string]any{"submit": map[string]any{"partition": "debug"}}, "exactly one of script and file"},
		{"unknown key", map[string]any{"job": 1, "region": "x"}, "not region"},
		{"wait for another operation", map[string]any{"cancel": 1, "wait": "1m"}, "with.wait is for job, not cancel"},
		{"bad job id", map[string]any{"job": "abc"}, "with.job must be the ID of a job"},
		{"zero job id", map[string]any{"job": 0}, "not 0"},
		{"poll too short", map[string]any{"job": 1, "poll": "100ms"}, "with.poll must be 1s or longer"},
		{"timeout within wait", map[string]any{"job": 1, "wait": "1m", "timeout": "30s"}, "must be longer than with.wait"},
		{"ssh key without host", map[string]any{"job": 1, "key_file": "~/.ssh/id"}, "with.key_file is for logging in to with.host"},
		{"passphrase without key", map[string]any{"job": 1, "host": "login", "key_passphrase": "x"}, "with.key_passphrase is for with.key_file"},
		{"bad port", map[string]any{"job": 1, "host": "login", "port": 70000}, "from 1 to 65535"},
		{"relative bin_dir", map[string]any{"job": 1, "bin_dir": "bin"}, "absolute path"},
		{"submit not an object", map[string]any{"submit": "#!/bin/sh"}, "with.submit must be an object"},
		{"submit without script", map[string]any{"submit": map[string]any{"partition": "debug"}}, "exactly one of script and file"},
		{"submit with both", map[string]any{"submit": map[string]any{"script": "x", "file": "y"}}, "exactly one of script and file"},
		{"submit unknown key", map[string]any{"submit": map[string]any{"script": "x", "queue": "y"}}, "not queue"},
		{"submit bad type", map[string]any{"submit": map[string]any{"script": "x", "partition": 1}}, "with.submit.partition must be a string"},
		{"submit bad time", map[string]any{"submit": map[string]any{"script": "x", "time": "soon"}}, "with.submit.time must be a duration"},
		{"submit bad option", map[string]any{"submit": map[string]any{"script": "x", "options": []any{"exclusive"}}}, "each starting with -"},
		{"jobs unknown key", map[string]any{"jobs": map[string]any{"queue": "x"}}, "with.jobs takes"},
		{"nodes not a name", map[string]any{"nodes": 3}, "with.nodes must be a name"},
		{"submit bad mem", map[string]any{"submit": map[string]any{"script": "x", "mem": "lots"}}, "with.submit.mem: memory must be"},
		{"endpoint and host", map[string]any{"job": 1, "endpoint": "http://slurm:6820", "host": "login"}, "with.host is for running the commands of Slurm"},
		{"endpoint and bin_dir", map[string]any{"job": 1, "endpoint": "http://slurm:6820", "bin_dir": "/opt/slurm/bin"}, "with.bin_dir is for running the commands"},
		{"endpoint not a URL", map[string]any{"job": 1, "endpoint": "slurm:6820"}, "must be the URL of slurmrestd"},
		{"endpoint with a query", map[string]any{"job": 1, "endpoint": "http://slurm:6820/?a=b"}, "must be the URL of slurmrestd"},
		{"token without endpoint", map[string]any{"job": 1, "token": "x"}, "with.token is for with.endpoint"},
		{"output by slurmrestd", map[string]any{"job": 1, "endpoint": "http://slurm:6820", "output": true}, "which slurmrestd cannot"},
		{"options by slurmrestd", map[string]any{"endpoint": "http://slurm:6820", "submit": map[string]any{"script": "x", "chdir": "/", "options": []any{"--exclusive"}}}, "which slurmrestd does not take"},
		{"no chdir by slurmrestd", map[string]any{"endpoint": "http://slurm:6820", "submit": map[string]any{"script": "x"}}, "with.submit.chdir is required with endpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseStep(tt.with, func(string) string { return "alice" })
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("parseStep() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestParseTimeLimit(t *testing.T) {
	tests := []struct {
		in   any
		want int64
	}{
		{"90s", 2},
		{"25h", 25 * 60},
		{"1-12:00:00", 36 * 60},
		{"1-12", 36 * 60},
		{"1-0:30", 24*60 + 30},
		{"2:00:01", 121},
		{"5:30", 6},
		{"30", 30},
		{float64(45), 45},
	}
	for _, tt := range tests {
		got, err := parseTimeLimit(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("parseTimeLimit(%v) = %d, %v, want %d", tt.in, got, err, tt.want)
		}
	}
	for _, in := range []any{"0", "0:00", "soon", float64(-1)} {
		if got, err := parseTimeLimit(in); err == nil {
			t.Errorf("parseTimeLimit(%v) = %d, want an error", in, got)
		}
	}
}

func TestParseStep_submitDefaults(t *testing.T) {
	s, err := parseStep(map[string]any{"job": 1, "submit": map[string]any{"partition": "debug", "keep": true}}, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if s.op != opJob {
		t.Errorf("op = %s", s.op)
	}
}

func TestParseStep_timeout(t *testing.T) {
	s, err := parseStep(map[string]any{"job": 1, "wait": "10m"}, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if s.timeout != 10*time.Minute+DefaultTimeout || s.poll != DefaultPoll {
		t.Errorf("timeout = %s, poll = %s", s.timeout, s.poll)
	}
}

func TestNote_passphrase(t *testing.T) {
	a := newAction(t, &cluster{})
	a.init()
	conn := connection{Host: "login", Port: 22, User: "alice", KeyFile: "/k", KeyPassphrase: "secret"}
	if err := a.note(sdk.Call{Step: sdk.Step{RunID: "../../etc"}}, conn, 1); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(a.ledgerDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ledger dir: %v, %v", entries, err)
	}
	b, err := os.ReadFile(filepath.Join(a.ledgerDir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), `"key_file"`) {
		t.Errorf("noted %s", b)
	}
	if err := a.note(sdk.Call{}, conn, 1); err == nil {
		t.Error("note() without a run ID succeeded")
	}
}

func TestWithoutSecrets(t *testing.T) {
	with := map[string]any{"key_passphrase": "secret", "token": "jwt", "job": 1}
	got := withoutSecrets(with)
	if got["key_passphrase"] != "<redacted>" || got["token"] != "<redacted>" || with["key_passphrase"] != "secret" || with["token"] != "jwt" {
		t.Errorf("withoutSecrets() = %v", got)
	}
}

func TestParseConnection_endpoint(t *testing.T) {
	env := func(v string) func(string) string {
		return func(k string) string {
			if k == "SLURM_JWT" {
				return v
			}
			return "alice"
		}
	}
	c, err := parseConnection(map[string]any{"endpoint": "https://slurm.example.com/api/"}, env("SLURM_JWT=abc\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c != (connection{Endpoint: "https://slurm.example.com/api", Token: "abc"}) {
		t.Errorf("parseConnection() = %+v", c)
	}
	if c.endpointAddress() != "slurm.example.com:443" {
		t.Errorf("endpointAddress() = %s", c.endpointAddress())
	}
	c, err = parseConnection(map[string]any{"endpoint": "http://slurm:6820", "token": "given", "user": "bob"}, env("abc"))
	if err != nil || c.Token != "given" || c.User != "bob" || c.endpointAddress() != "slurm:6820" {
		t.Errorf("parseConnection() = %+v, %v", c, err)
	}
	if _, err := parseConnection(map[string]any{"endpoint": "http://slurm:6820"}, env("")); err == nil || !strings.Contains(err.Error(), "with.token is required with endpoint") {
		t.Errorf("parseConnection() without a token error = %v", err)
	}
}

// restd stands in for slurmrestd, answering "METHOD /path".
func restd(t *testing.T, answers map[string]string) (*httptest.Server, *[]*http.Request) {
	t.Helper()
	var mu sync.Mutex
	var requests []*http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r)
		mu.Unlock()
		body, ok := answers[r.Method+" "+r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, "Unable find requested URL.")
			return
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, &requests
}

func TestRunStep_endpoint(t *testing.T) {
	srv, requests := restd(t, map[string]string{
		"POST /slurm/v0.0.42/job/submit": `{"job_id": 77, "errors": []}`,
		"GET /slurm/v0.0.42/job/77":      string(queueJSON(77, "RUNNING", 0)),
		"DELETE /slurm/v0.0.42/job/77":   `{"status": [], "errors": []}`,
	})
	c := &cluster{}
	a := newAction(t, c)
	a.http = srv.Client()
	jwt := ""
	a.getenv = func(k string) string {
		if k == "SLURM_JWT" {
			return jwt
		}
		return ""
	}
	ret, _, err := a.RunStep(sdk.Call{
		With: map[string]any{
			"endpoint": srv.URL,
			"token":    "step-token",
			"submit":   map[string]any{"script": "#!/bin/sh\ntrue\n", "chdir": "/home/alice", "time": "1:30:00"},
		},
		Step: sdk.Step{RunID: "run-rest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, req := ret["res"].(map[string]any), ret["req"].(map[string]any)
	if res["job_id"] != int64(77) || ret["status"] != 0 {
		t.Errorf("RunStep() = %v", ret)
	}
	if req["endpoint"] != srv.URL || req["api_version"] != "v0.0.42" || req["command"] != nil {
		t.Errorf("req = %v", req)
	}
	if len(c.dialled) != 0 {
		t.Errorf("dialled %+v", c.dialled)
	}
	if got := (*requests)[0].Header.Get("X-SLURM-USER-TOKEN"); got != "step-token" {
		t.Errorf("sent the token %q", got)
	}
	entries, _ := os.ReadDir(a.ledgerDir)
	b, err := os.ReadFile(filepath.Join(a.ledgerDir, entries[0].Name()))
	if err != nil || strings.Contains(string(b), "step-token") || !strings.Contains(string(b), srv.URL) {
		t.Errorf("noted %s, %v", b, err)
	}

	// Without SLURM_JWT, the cleanup has no token to cancel the job with.
	if err := a.Cleanup(sdk.CleanupCall{RunID: "run-rest"}); err == nil || !strings.Contains(err.Error(), "jobs [77] at "+srv.URL+" are left") {
		t.Fatalf("Cleanup() without SLURM_JWT error = %v", err)
	}
	_, _, err = a.RunStep(sdk.Call{
		With: map[string]any{"endpoint": srv.URL, "token": "step-token", "submit": map[string]any{"script": "x", "chdir": "/"}},
		Step: sdk.Step{RunID: "run-rest-2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	jwt = "env-token"
	if err := a.Cleanup(sdk.CleanupCall{RunID: "run-rest-2"}); err != nil {
		t.Fatal(err)
	}
	last := (*requests)[len(*requests)-1]
	if last.Method != http.MethodDelete || last.URL.Path != "/slurm/v0.0.42/job/77" || last.Header.Get("X-SLURM-USER-TOKEN") != "env-token" {
		t.Errorf("cleanup sent %s %s with %q", last.Method, last.URL.Path, last.Header.Get("X-SLURM-USER-TOKEN"))
	}
}

func TestCheckGuard_endpoint(t *testing.T) {
	s := step{op: opJob, conn: connection{Endpoint: "https://slurm.example.com"}}
	if err := checkGuard(s, sdk.Guard{AllowHosts: []string{"slurm.example.com:443"}}); err != nil {
		t.Errorf("checkGuard() = %v", err)
	}
	if err := checkGuard(s, sdk.Guard{AllowHosts: []string{"login.example.com"}}); !sdk.IsRefused(err) {
		t.Errorf("checkGuard() = %v, want a refusal", err)
	}
	s.op = opSubmit
	if err := checkGuard(s, sdk.Guard{ReadOnly: true}); !sdk.IsRefused(err) {
		t.Errorf("checkGuard() = %v, want a refusal", err)
	}
}

// TestManifestsDeclare checks that action.yml, and the one the e2e workflow
// uses, declare the params, the guard, the fields of res and the cleanup the
// action has, so that probe check and the guard of a run take it as it is.
// probe manifest keeps what action.yml declares in the one of a release.
func TestManifestsDeclare(t *testing.T) {
	for _, name := range []string{"action.yml", "e2e/action/action.yml"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		m, err := sdk.ParseManifest(data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(m.Params, params) {
			t.Errorf("%s declares params %v, want %v", name, m.Params, params)
		}
		if !slices.Equal(m.Guard, keeps) {
			t.Errorf("%s declares guard %v, want %v", name, m.Guard, keeps)
		}
		if !slices.Equal(m.Res, resFields) {
			t.Errorf("%s declares res %v, want %v", name, m.Res, resFields)
		}
		if !m.Cleanup {
			t.Errorf("%s does not declare cleanup", name)
		}
	}
}

// TestRunStep_resDeclared checks that each operation returns only the
// fields of res that action.yml declares.
func TestRunStep_resDeclared(t *testing.T) {
	c := &cluster{answer: func(argv []string, _ []byte) runner.Output {
		switch argv[0] {
		case "sbatch":
			return runner.Output{Stdout: []byte("1\n")}
		case "squeue":
			return runner.Output{Stdout: queueJSON(1, "RUNNING", 0)}
		case "head":
			return runner.Output{Stdout: []byte("out")}
		case "scontrol":
			if argv[3] == "partition" {
				return runner.Output{Stdout: []byte(`{"partitions": [{"name": "debug"}]}`)}
			}
			return runner.Output{Stdout: []byte(`{"nodes": [{"name": "c1"}]}`)}
		}
		return runner.Output{}
	}}
	a := newAction(t, c)
	for _, with := range []map[string]any{
		{"submit": map[string]any{"script": "x", "keep": true}},
		{"job": 1, "output": true},
		{"jobs": nil},
		{"cancel": 1},
		{"partitions": nil},
		{"nodes": nil},
	} {
		ret, _, err := a.RunStep(sdk.Call{With: with})
		if err != nil {
			t.Fatal(err)
		}
		for k := range ret["res"].(map[string]any) {
			if !slices.Contains(resFields, k) {
				t.Errorf("%v returns res.%s, which action.yml does not declare", with, k)
			}
		}
	}
}
