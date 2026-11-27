package slurm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
)

type answer struct {
	status int
	body   string
}

func fixtureText(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// fakeRestd answers "METHOD /path" as slurmrestd would, and a path it does
// not know with the page slurmrestd answers one with.
type fakeRestd struct {
	answers  map[string]answer
	requests []*http.Request
	bodies   []string
}

func (f *fakeRestd) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	f.requests = append(f.requests, r)
	f.bodies = append(f.bodies, string(b))
	a, ok := f.answers[r.Method+" "+r.URL.Path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "Unable find requested URL. Please view /openapi/v3 for API reference.")
		return
	}
	w.WriteHeader(a.status)
	_, _ = io.WriteString(w, a.body)
}

func newREST(t *testing.T, answers map[string]answer) (*REST, *fakeRestd) {
	t.Helper()
	f := &fakeRestd{answers: answers}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return &REST{URL: srv.URL, Token: "tok", User: "alice", UserAgent: "probe-slurm/test", HTTP: srv.Client()}, f
}

func TestREST_Submit(t *testing.T) {
	r, f := newREST(t, map[string]answer{
		"POST /slurm/v0.0.40/job/submit": {200, fixtureText(t, "rest-submit-v0.0.42.json")},
	})
	id, cluster, err := r.Submit(context.Background(), JobSpec{
		Script: []byte("#!/bin/sh\ntrue\n"), Name: "probe", Partition: "debug", TimeLimit: 90,
		Nodes: "1-2", NTasks: 2, CPUsPerTask: 1, Mem: "1.5G", Gres: "gpu:1,gres/shard:2", Chdir: "/home/alice",
		Output: "o-%j", Environment: []string{"PATH=/usr/bin:/bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != 5 || cluster != "" || r.Version() != "v0.0.40" {
		t.Errorf("Submit() = %d, %q, version %s", id, cluster, r.Version())
	}
	// The newer versions were tried first, and are not served.
	if len(f.requests) != 3 {
		t.Fatalf("made %d requests, want 3", len(f.requests))
	}
	last := f.requests[2]
	if last.Header.Get("X-SLURM-USER-TOKEN") != "tok" || last.Header.Get("X-SLURM-USER-NAME") != "alice" ||
		last.Header.Get("User-Agent") != "probe-slurm/test" || last.Header.Get("Content-Type") != "application/json" {
		t.Errorf("headers = %v", last.Header)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(f.bodies[2]), &body); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"script": "#!/bin/sh\ntrue\n",
		"job": map[string]any{
			"name": "probe", "partition": "debug", "time_limit": float64(90), "nodes": "1-2", "tasks": float64(2),
			"cpus_per_task": float64(1), "memory_per_node": float64(1536), "tres_per_node": "gres/gpu:1,gres/shard:2",
			"current_working_directory": "/home/alice", "standard_output": "o-%j", "environment": []any{"PATH=/usr/bin:/bin"},
		},
	}
	if !reflect.DeepEqual(body, want) {
		t.Errorf("body = %v, want %v", body, want)
	}
}

func TestREST_SubmitAllMemory(t *testing.T) {
	r, f := newREST(t, map[string]answer{"POST /slurm/v0.0.42/job/submit": {200, `{"job_id": 9, "errors": []}`}})
	if _, _, err := r.Submit(context.Background(), JobSpec{Script: []byte("x"), Chdir: "/", Mem: "0"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.bodies[0], `"memory_per_node":0`) {
		t.Errorf("body = %s", f.bodies[0])
	}
}

func TestREST_NoRedirect(t *testing.T) {
	var reached []string
	other := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		reached = append(reached, r.Header.Get("X-SLURM-USER-TOKEN"))
	}))
	t.Cleanup(other.Close)
	r, _ := newREST(t, nil)
	srv := httptest.NewServer(http.RedirectHandler(other.URL+"/slurm/v0.0.42/job/submit", http.StatusFound))
	t.Cleanup(srv.Close)
	r.URL = srv.URL
	if _, _, err := r.Submit(context.Background(), JobSpec{Script: []byte("x"), Chdir: "/"}); err == nil || !strings.Contains(err.Error(), "slurmrestd answered 302") {
		t.Errorf("Submit() error = %v", err)
	}
	if len(reached) != 0 {
		t.Errorf("the redirect was followed, with the tokens %q", reached)
	}
}

func TestREST_JobForgottenOtherwiseThan404(t *testing.T) {
	r, _ := newREST(t, map[string]answer{
		"GET /slurm/v0.0.39/job/42":   {500, `{"errors": [{"description": "Unable to query JobId=42", "error_number": 2017, "error": "Invalid job id specified"}]}`},
		"GET /slurmdb/v0.0.39/job/42": {200, fixtureText(t, "sacct-v0.0.40.json")},
	})
	j, err := r.Job(context.Background(), 42)
	if err != nil || j.ID != 42 {
		t.Fatalf("Job() = %+v, %v", j, err)
	}
}

func TestREST_SubmitRefused(t *testing.T) {
	r, _ := newREST(t, map[string]answer{
		"POST /slurm/v0.0.42/job/submit": {500, fixtureText(t, "rest-submit-refused-v0.0.42.json")},
	})
	_, _, err := r.Submit(context.Background(), JobSpec{Script: []byte("#!/bin/sh\n"), Partition: "nope", Chdir: "/tmp"})
	var refused *Refusal
	if !errors.As(err, &refused) || refused.Message != "Batch job submission failed: Invalid partition name specified" {
		t.Fatalf("Submit() error = %v", err)
	}
}

func TestREST_SubmitWithoutWhatItNeeds(t *testing.T) {
	r, f := newREST(t, nil)
	if _, _, err := r.Submit(context.Background(), JobSpec{Script: []byte("x")}); err == nil || !strings.Contains(err.Error(), "working directory") {
		t.Errorf("Submit() without chdir error = %v", err)
	}
	if _, _, err := r.Submit(context.Background(), JobSpec{Script: []byte("x"), Chdir: "/", Options: []string{"--exclusive"}}); err == nil || !strings.Contains(err.Error(), "options of sbatch") {
		t.Errorf("Submit() with options error = %v", err)
	}
	if len(f.requests) != 0 {
		t.Errorf("made %d requests", len(f.requests))
	}
}

func TestREST_NoVersion(t *testing.T) {
	r, _ := newREST(t, nil)
	if _, err := r.Jobs(context.Background(), Filter{}); err == nil || !strings.Contains(err.Error(), "serves none of the versions") {
		t.Errorf("Jobs() error = %v", err)
	}
}

func TestREST_Unauthorized(t *testing.T) {
	r, _ := newREST(t, map[string]answer{"GET /slurm/v0.0.42/jobs": {401, "Authentication failure"}})
	if _, err := r.Jobs(context.Background(), Filter{}); err == nil || !strings.Contains(err.Error(), "refused the token: 401") {
		t.Errorf("Jobs() error = %v", err)
	}
}

func TestREST_Job(t *testing.T) {
	unknown := fixtureText(t, "rest-job-unknown-v0.0.42.json")
	tests := []struct {
		name        string
		answers     map[string]answer
		state       string
		errContains string
	}{
		{"from slurmctld", map[string]answer{"GET /slurm/v0.0.42/job/5": {200, fixtureText(t, "rest-job-v0.0.42.json")}}, "CANCELLED", ""},
		{"from slurmdbd", map[string]answer{
			"GET /slurm/v0.0.42/job/5":   {404, unknown},
			"GET /slurmdb/v0.0.42/job/5": {200, fixtureText(t, "sacct-v0.0.40.json")},
		}, "", "job 5: GET /slurm/v0.0.42/job/5"},
		{"without accounting", map[string]answer{"GET /slurm/v0.0.42/job/5": {404, unknown}}, "", "serves no accounting"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := newREST(t, tt.answers)
			j, err := r.Job(context.Background(), 5)
			if tt.errContains != "" {
				if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("Job() error = %v, want %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if j.ID != 5 || j.State != tt.state || j.Name != "sleeper" || j.StdoutPath != "/home/probe/slurm-5.out" {
				t.Errorf("Job() = %+v", j)
			}
		})
	}
}

func TestREST_JobFromAccounting(t *testing.T) {
	r, _ := newREST(t, map[string]answer{
		"GET /slurm/v0.0.42/job/42":   {404, fixtureText(t, "rest-job-unknown-v0.0.42.json")},
		"GET /slurmdb/v0.0.42/job/42": {200, fixtureText(t, "sacct-v0.0.40.json")},
	})
	j, err := r.Job(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if j.ID != 42 || !j.Done() {
		t.Errorf("Job() = %+v", j)
	}
}

func TestREST_Jobs(t *testing.T) {
	r, _ := newREST(t, map[string]answer{"GET /slurm/v0.0.42/jobs": {200, fixtureText(t, "squeue-v0.0.40.json")}})
	all, err := r.Jobs(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range all {
		if j.Done() {
			t.Errorf("Jobs() without a state has %d, which is over", j.ID)
		}
	}
	done, err := r.Jobs(context.Background(), Filter{State: "COMPLETED"})
	if err != nil {
		t.Fatal(err)
	}
	if len(done) == 0 {
		t.Error("Jobs() with state COMPLETED has none")
	}
	for _, j := range done {
		if j.State != "COMPLETED" {
			t.Errorf("Jobs() with state COMPLETED has %d, %s", j.ID, j.State)
		}
	}
}

func TestREST_Cancel(t *testing.T) {
	running := strings.Replace(fixtureText(t, "rest-job-v0.0.42.json"), `"CANCELLED"`, `"RUNNING"`, 1)
	tests := []struct {
		name      string
		delete    answer
		cancelled bool
		refusal   string
	}{
		{"cancelled", answer{200, fixtureText(t, "rest-cancel-v0.0.42.json")}, true, ""},
		{"over meanwhile", answer{200, fixtureText(t, "rest-cancel-completed-v0.0.42.json")}, false, ""},
		{"refused", answer{200, `{"status": [{"error": {"code": 1, "message": "Access/permission denied"}}], "errors": []}`}, false, "Access/permission denied"},
		{"refused before v0.0.42", answer{500, `{"errors": [{"description": "kill job error", "error": "Access/permission denied", "error_number": 1}]}`}, false, "kill job error: Access/permission denied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := newREST(t, map[string]answer{
				"GET /slurm/v0.0.42/job/5":    {200, running},
				"DELETE /slurm/v0.0.42/job/5": tt.delete,
			})
			j, cancelled, err := r.Cancel(context.Background(), 5)
			if tt.refusal != "" {
				var refused *Refusal
				if !errors.As(err, &refused) || refused.Message != tt.refusal {
					t.Fatalf("Cancel() error = %v, want refusal %q", err, tt.refusal)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if j.State != "RUNNING" || cancelled != tt.cancelled {
				t.Errorf("Cancel() = %s, %v", j.State, cancelled)
			}
		})
	}
}

func TestREST_CancelOver(t *testing.T) {
	r, f := newREST(t, map[string]answer{"GET /slurm/v0.0.42/job/5": {200, fixtureText(t, "rest-job-v0.0.42.json")}})
	j, cancelled, err := r.Cancel(context.Background(), 5)
	if err != nil || cancelled || j.State != "CANCELLED" {
		t.Fatalf("Cancel() = %s, %v, %v", j.State, cancelled, err)
	}
	if len(f.requests) != 1 {
		t.Errorf("made %d requests, want only the lookup", len(f.requests))
	}
}

func TestREST_CancelAll(t *testing.T) {
	r, f := newREST(t, map[string]answer{
		"DELETE /slurm/v0.0.42/job/5": {200, fixtureText(t, "rest-cancel-v0.0.42.json")},
		"DELETE /slurm/v0.0.42/job/6": {200, fixtureText(t, "rest-cancel-completed-v0.0.42.json")},
		"DELETE /slurm/v0.0.42/job/7": {200, fixtureText(t, "rest-cancel-unknown-v0.0.42.json")},
	})
	if err := r.CancelAll(context.Background(), []int64{5, 6, 7}); err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 3 {
		t.Errorf("made %d requests", len(f.requests))
	}

	r, _ = newREST(t, map[string]answer{
		"DELETE /slurm/v0.0.42/job/8": {200, `{"status": [{"error": {"code": 1, "message": "Access/permission denied"}}], "errors": []}`},
	})
	if err := r.CancelAll(context.Background(), []int64{8}); err == nil || !strings.Contains(err.Error(), "job 8: Access/permission denied") {
		t.Errorf("CancelAll() error = %v", err)
	}
}

func TestREST_NodesAndPartitions(t *testing.T) {
	r, _ := newREST(t, map[string]answer{
		"GET /slurm/v0.0.42/nodes":      {200, fixtureText(t, "nodes-v0.0.42.json")},
		"GET /slurm/v0.0.42/partitions": {200, fixtureText(t, "partitions-v0.0.42.json")},
	})
	// As scontrol, nodes the cluster does not have are left out.
	nodes, err := r.Nodes(context.Background(), "c[1-3],nope")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Name != "c1" {
		t.Errorf("Nodes() = %+v", nodes)
	}
	partitions, err := r.Partitions(context.Background(), "debug")
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 1 || partitions[0].Name != "debug" || partitions[0].Idle+partitions[0].Mixed+partitions[0].Allocated+partitions[0].Down+partitions[0].Drained+partitions[0].Other != 1 {
		t.Errorf("Partitions() = %+v", partitions)
	}
	if _, err := r.Partitions(context.Background(), "nope"); err == nil || !strings.Contains(err.Error(), "no partition nope") {
		t.Errorf("Partitions(nope) error = %v", err)
	}
}

func TestExpandHostlist(t *testing.T) {
	tests := []struct {
		list string
		want []string
		err  bool
	}{
		{"c1", []string{"c1"}, false},
		{"c[1-3,7],login", []string{"c1", "c2", "c3", "c7", "login"}, false},
		{"gpu[08-10]", []string{"gpu08", "gpu09", "gpu10"}, false},
		{"r[1-2]n[1-2]", []string{"r1n1", "r1n2", "r2n1", "r2n2"}, false},
		{"c[1-", nil, true},
		{"c[3-1]", nil, true},
		{"c[1-1000000]", nil, true},
	}
	for _, tt := range tests {
		got, err := ExpandHostlist(tt.list)
		if (err != nil) != tt.err || (!tt.err && !reflect.DeepEqual(got, tt.want)) {
			t.Errorf("ExpandHostlist(%q) = %q, %v", tt.list, got, err)
		}
	}
}

func TestParseMem(t *testing.T) {
	tests := []struct {
		in   string
		want int64
		err  bool
	}{
		{"512", 512, false}, {"512M", 512, false}, {"4G", 4096, false}, {"1.5g", 1536, false},
		{"1T", 1 << 20, false}, {"100K", 1, false}, {"0", 0, false}, {"lots", 0, true}, {"-1G", 0, true},
		{"inf", 0, true}, {"NaN", 0, true}, {"1e30G", 0, true},
	}
	for _, tt := range tests {
		got, err := ParseMem(tt.in)
		if (err != nil) != tt.err || got != tt.want {
			t.Errorf("ParseMem(%q) = %d, %v", tt.in, got, err)
		}
	}
}
