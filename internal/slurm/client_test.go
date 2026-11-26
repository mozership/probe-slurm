package slurm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mozership/probe-slurm/internal/runner"
)

// fakeRunner answers each command by its name, and records what it ran.
type fakeRunner struct {
	answers map[string]runner.Output
	ran     [][]string
	stdin   [][]byte
}

func (f *fakeRunner) Run(_ context.Context, argv []string, stdin []byte) (runner.Output, error) {
	f.ran = append(f.ran, argv)
	f.stdin = append(f.stdin, stdin)
	out, ok := f.answers[argv[0]]
	if !ok {
		return runner.Output{}, errors.New("no such command: " + argv[0])
	}
	return out, nil
}

func (f *fakeRunner) Close() error { return nil }

func TestClient_Submit(t *testing.T) {
	tests := []struct {
		name        string
		out         runner.Output
		id          int64
		cluster     string
		refusal     string
		errContains string
	}{
		{"id", runner.Output{Stdout: []byte("123\n")}, 123, "", "", ""},
		{"id and cluster after a warning", runner.Output{Stdout: []byte("sbatch: warning: x\n124;hpc\n")}, 124, "hpc", "", ""},
		{"refused", runner.Output{Code: 1, Stderr: []byte("sbatch: error: invalid partition specified: nope\n")}, 0, "", "sbatch: error: invalid partition specified: nope", ""},
		{"no id", runner.Output{Stdout: []byte("Submitted\n")}, 0, "", "", "printed no job ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeRunner{answers: map[string]runner.Output{"/opt/slurm/bin/sbatch": tt.out}}
			c := Client{Runner: f, BinDir: "/opt/slurm/bin"}
			id, cluster, err := c.Submit(context.Background(), []string{"--partition=debug"}, []byte("#!/bin/sh\n"))
			var refused *Refusal
			switch {
			case tt.refusal != "":
				if !errors.As(err, &refused) || refused.Message != tt.refusal {
					t.Fatalf("Submit() error = %v, want refusal %q", err, tt.refusal)
				}
			case tt.errContains != "":
				if err == nil || !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("Submit() error = %v, want %q", err, tt.errContains)
				}
			case err != nil:
				t.Fatal(err)
			}
			if id != tt.id || cluster != tt.cluster {
				t.Errorf("Submit() = %d, %q, want %d, %q", id, cluster, tt.id, tt.cluster)
			}
			want := []string{"/opt/slurm/bin/sbatch", "--parsable", "--partition=debug"}
			if !reflect.DeepEqual(f.ran[0], want) || string(f.stdin[0]) != "#!/bin/sh\n" {
				t.Errorf("ran %q with %q", f.ran[0], f.stdin[0])
			}
		})
	}
}

func TestClient_Job(t *testing.T) {
	queue := runner.Output{Stdout: fixture(t, "squeue-v0.0.40.json")}
	gone := runner.Output{Code: 1, Stdout: fixture(t, "squeue-invalid.json")}
	acct := runner.Output{Stdout: fixture(t, "sacct-v0.0.40.json")}
	noAcct := runner.Output{Code: 1, Stderr: []byte("sacct: error: Slurm accounting storage is disabled\n")}

	tests := []struct {
		name        string
		squeue      runner.Output
		sacct       runner.Output
		id          int64
		state       string
		ran         int
		errContains string
	}{
		{"in the queue", queue, acct, 43, "PENDING", 1, ""},
		{"no longer in the queue", gone, acct, 42, "COMPLETED", 2, ""},
		// An squeue that prints every job with --json, the one asked for
		// not among them.
		{"not among those squeue prints", queue, acct, 44, "PENDING", 2, ""},
		{"no accounting", gone, noAcct, 42, "", 2, "accounting storage is disabled"},
		{"known to neither", queue, acct, 99, "", 2, "neither squeue nor sacct knows job 99"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeRunner{answers: map[string]runner.Output{"squeue": tt.squeue, "sacct": tt.sacct}}
			j, err := Client{Runner: f}.Job(context.Background(), tt.id)
			if tt.errContains != "" {
				if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("Job() error = %v, want %q", err, tt.errContains)
				}
			} else if err != nil {
				t.Fatal(err)
			} else if j.ID != tt.id || j.State != tt.state {
				t.Errorf("Job() = %+v", j)
			}
			if len(f.ran) != tt.ran {
				t.Errorf("ran %q", f.ran)
			}
		})
	}
}

func TestClient_Jobs(t *testing.T) {
	f := &fakeRunner{answers: map[string]runner.Output{"squeue": {Stdout: fixture(t, "squeue-v0.0.40.json")}}}
	jobs, err := Client{Runner: f}.Jobs(context.Background(), Filter{User: "probe", State: "pending"})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != 43 {
		t.Errorf("Jobs() = %+v", jobs)
	}
	want := []string{"squeue", "--json", "--user=probe", "--states=pending"}
	if !reflect.DeepEqual(f.ran[0], want) {
		t.Errorf("ran %q, want %q", f.ran[0], want)
	}
}

func TestClient_Cancel(t *testing.T) {
	running := runner.Output{Stdout: fixture(t, "squeue-v0.0.40.json")}
	tests := []struct {
		name      string
		id        int64
		scancel   runner.Output
		cancelled bool
		refused   bool
		ran       int
	}{
		{"cancelled", 43, runner.Output{}, true, false, 2},
		// The job is over, so scancel is not run.
		{"already over", 42, runner.Output{}, false, false, 1},
		{"over by the time scancel ran", 43, runner.Output{Code: 1, Stderr: []byte("scancel: error: Kill job error on job id 43: Job/step already completing or completed\n")}, false, false, 2},
		{"refused", 43, runner.Output{Code: 1, Stderr: []byte("scancel: error: Kill job error on job id 43: Access/permission denied\n")}, false, true, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeRunner{answers: map[string]runner.Output{"squeue": running, "scancel": tt.scancel}}
			j, cancelled, err := Client{Runner: f}.Cancel(context.Background(), tt.id)
			var refusal *Refusal
			if errors.As(err, &refusal) != tt.refused || (err != nil && !tt.refused) {
				t.Fatalf("Cancel() error = %v", err)
			}
			if cancelled != tt.cancelled || j.ID != tt.id || len(f.ran) != tt.ran {
				t.Errorf("Cancel() = %+v, %v; ran %q", j, cancelled, f.ran)
			}
		})
	}

	f := &fakeRunner{answers: map[string]runner.Output{"squeue": running, "sacct": {Code: 1, Stderr: []byte("Slurm accounting storage is disabled")}}}
	if _, _, err := (Client{Runner: f}).Cancel(context.Background(), 99); !errors.Is(err, ErrNotFound) {
		t.Errorf("Cancel() of an unknown job: %v", err)
	}
}

func TestClient_CancelAll(t *testing.T) {
	f := &fakeRunner{answers: map[string]runner.Output{"scancel": {Code: 1, Stderr: []byte(
		"scancel: error: Kill job error on job id 5: Job/step already completing or completed\n" +
			"scancel: error: Kill job error on job id 6: Invalid job id specified\n")}}}
	if err := (Client{Runner: f}).CancelAll(context.Background(), []int64{5, 6, 7}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"scancel", "5", "6", "7"}; !reflect.DeepEqual(f.ran[0], want) {
		t.Errorf("ran %q", f.ran[0])
	}

	f.answers["scancel"] = runner.Output{Code: 1, Stderr: []byte("scancel: error: Access/permission denied\n")}
	if err := (Client{Runner: f}).CancelAll(context.Background(), []int64{8}); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("CancelAll() error = %v", err)
	}
}

func TestClient_Partitions(t *testing.T) {
	r := &byArgs{answers: map[string][]byte{"partition": fixture(t, "partitions-v0.0.40.json"), "node": fixture(t, "nodes-v0.0.40.json")}}
	got, err := Client{Runner: r}.Partitions(context.Background(), "long")
	if err != nil {
		t.Fatal(err)
	}
	want := []Partition{{Name: "long", State: "DRAIN", TotalNodes: 2, Idle: 1, Allocated: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Partitions() = %+v, want %+v", got, want)
	}

	if _, err := (Client{Runner: r}).Partitions(context.Background(), "nope"); err == nil || !strings.Contains(err.Error(), "no partition nope") {
		t.Errorf("Partitions() error = %v", err)
	}
}

// byArgs answers scontrol show by what it shows.
type byArgs struct {
	answers map[string][]byte
}

func (b *byArgs) Run(_ context.Context, argv []string, _ []byte) (runner.Output, error) {
	return runner.Output{Stdout: b.answers[argv[3]]}, nil
}

func (b *byArgs) Close() error { return nil }

func TestClient_ReadFile(t *testing.T) {
	f := &fakeRunner{answers: map[string]runner.Output{"head": {Stdout: []byte("12345")}}}
	out, truncated, err := Client{Runner: f, BinDir: "/opt/slurm/bin"}.ReadFile(context.Background(), "/home/a b.out", 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "1234" || !truncated {
		t.Errorf("ReadFile() = %q, %v", out, truncated)
	}
	if want := []string{"head", "-c", "5", "--", "/home/a b.out"}; !reflect.DeepEqual(f.ran[0], want) {
		t.Errorf("ran %q", f.ran[0])
	}
}
