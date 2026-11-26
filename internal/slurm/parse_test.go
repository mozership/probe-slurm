package slurm

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func unix(sec int64) time.Time { return time.Unix(sec, 0).UTC() }

func TestParseQueue(t *testing.T) {
	tests := []struct {
		file string
		want []Job
	}{
		{"squeue-v0.0.40.json", []Job{
			{
				ID: 42, Name: "probe", User: "probe", Account: "lab", Partition: "debug",
				State: "COMPLETED", Nodes: "c1",
				Submit: unix(1764000000), Start: unix(1764000005), End: unix(1764000015),
				StdoutPath: "/home/probe/slurm-42.out",
			},
			{
				// The start and the end slurmctld expects are not taken
				// for when the job started and ended.
				ID: 43, Name: "waits", User: "probe", Account: "lab", Partition: "debug",
				State: "PENDING", Reason: "Resources", Submit: unix(1764000100),
				StdoutPath: "/scratch/probe/waits-40_3.out",
			},
		}},
		{"squeue-v0.0.39.json", []Job{{
			ID: 7, Name: "old", User: "probe", Account: "lab", Partition: "debug",
			State: "FAILED", Reason: "NonZeroExitCode", Nodes: "c2", ExitCode: 3,
			Submit: unix(1764000000), Start: unix(1764000010), End: unix(1764000070),
			StdoutPath: "/home/probe/out.txt",
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			got, err := ParseQueue(fixture(t, tt.file))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseQueue()\n got %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestParseQueue_errors(t *testing.T) {
	_, err := ParseQueue(fixture(t, "squeue-invalid.json"))
	if err == nil || err.Error() != "Invalid job id specified: Invalid job id specified" {
		t.Errorf("ParseQueue() error = %v", err)
	}
}

func TestParseAccounting(t *testing.T) {
	got, err := ParseAccounting(fixture(t, "sacct-v0.0.40.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := []Job{
		{
			// The job ran twice, and the second time is kept.
			ID: 42, Name: "probe", User: "probe", Account: "lab", Partition: "debug",
			State: "COMPLETED", Nodes: "c2",
			Submit: unix(1764000000), Start: unix(1764000020), End: unix(1764000030),
			StdoutPath: "/home/probe/slurm-42.out",
		},
		{
			ID: 44, Name: "queued", User: "probe", Account: "lab", Partition: "debug",
			State: "PENDING", Reason: "Priority", Submit: unix(1764000000), Comment: "note",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ParseAccounting()\n got %+v\nwant %+v", got, want)
	}
}

func TestParseAccounting_signal(t *testing.T) {
	got, err := ParseAccounting([]byte(`{"jobs": [{"job_id": 1, "state": {"current": ["CANCELLED", "SIGNALED"]}, "exit_code": {"return_code": {"set": true, "number": 0}, "signal": {"id": {"set": true, "number": 15}}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got[0].State != "CANCELLED" || got[0].Signal != 15 {
		t.Errorf("ParseAccounting() = %+v", got[0])
	}
}

func TestJob_times(t *testing.T) {
	now := unix(1000)
	tests := []struct {
		name          string
		job           Job
		wait, elapsed time.Duration
	}{
		{"pending", Job{State: "PENDING", Submit: unix(900)}, 100 * time.Second, 0},
		{"running", Job{State: "RUNNING", Submit: unix(900), Start: unix(950)}, 50 * time.Second, 50 * time.Second},
		{"completed", Job{State: "COMPLETED", Submit: unix(900), Start: unix(950), End: unix(970)}, 50 * time.Second, 20 * time.Second},
		{"cancelled before it started", Job{State: "CANCELLED", Submit: unix(900)}, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.job.Wait(now); got != tt.wait {
				t.Errorf("Wait() = %s, want %s", got, tt.wait)
			}
			if got := tt.job.Elapsed(now); got != tt.elapsed {
				t.Errorf("Elapsed() = %s, want %s", got, tt.elapsed)
			}
		})
	}
}

func TestParseNodes(t *testing.T) {
	got, err := ParseNodes(fixture(t, "nodes-v0.0.39.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := []Node{{Name: "c1", State: "IDLE", Flags: []string{"DRAIN"}, Partitions: []string{"debug", "long"}, Reason: "maintenance", CPUs: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ParseNodes()\n got %+v\nwant %+v", got, want)
	}
}

func TestParsePartitions(t *testing.T) {
	tests := []struct {
		file string
		want []Partition
	}{
		{"partitions-v0.0.40.json", []Partition{{Name: "debug", State: "UP", TotalNodes: 4}, {Name: "long", State: "DRAIN", TotalNodes: 2}}},
		{"partitions-v0.0.39.json", []Partition{{Name: "debug", State: "UP", TotalNodes: 4}}},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			got, err := ParsePartitions(fixture(t, tt.file))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParsePartitions()\n got %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestPartition_Count(t *testing.T) {
	nodes, err := ParseNodes(fixture(t, "nodes-v0.0.40.json"))
	if err != nil {
		t.Fatal(err)
	}
	p := Partition{Name: "debug"}
	for _, n := range nodes {
		p.Count(n)
	}
	want := Partition{Name: "debug", Idle: 1, Mixed: 1, Allocated: 1, Down: 1, Drained: 1}
	if p != want {
		t.Errorf("Count() = %+v, want %+v", p, want)
	}
}

func TestExpandPath(t *testing.T) {
	j := Job{ID: 9, Name: "name", User: "alice"}
	tests := []struct {
		path, workDir, want string
	}{
		{"", "/home", ""},
		{"out.txt", "/home/alice", "/home/alice/out.txt"},
		{"/abs/%j.out", "/home", "/abs/9.out"},
		{"%x-%u-%%-%N.out", "/w", "/w/name-alice-%-%N.out"},
		{"trailing%", "/w", "/w/trailing%"},
	}
	for _, tt := range tests {
		if got := expandPath(tt.path, tt.workDir, j, 0, number{}); got != tt.want {
			t.Errorf("expandPath(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

// The files of v0.0.42 are what Slurm 24.11 printed in the container of the
// e2e tests.
func TestParse_v0042(t *testing.T) {
	jobs, err := ParseQueue(fixture(t, "squeue-v0.0.42.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := Job{
		ID: 1, Name: "sbatch", User: "probe", Partition: "debug", State: "COMPLETED", Nodes: "c1",
		Submit: unix(1795708185), Start: unix(1795708186), End: unix(1795708186),
		StdoutPath: "/home/probe/slurm-1.out",
	}
	if len(jobs) != 1 || !reflect.DeepEqual(jobs[0], want) {
		t.Errorf("ParseQueue()\n got %+v\nwant %+v", jobs, want)
	}

	nodes, err := ParseNodes(fixture(t, "nodes-v0.0.42.json"))
	if err != nil {
		t.Fatal(err)
	}
	wantNodes := []Node{{Name: "c1", State: "IDLE", Flags: []string{}, Partitions: []string{"debug", "closed"}, CPUs: 2}}
	if !reflect.DeepEqual(nodes, wantNodes) {
		t.Errorf("ParseNodes()\n got %+v\nwant %+v", nodes, wantNodes)
	}

	partitions, err := ParsePartitions(fixture(t, "partitions-v0.0.42.json"))
	if err != nil {
		t.Fatal(err)
	}
	wantPartitions := []Partition{{Name: "debug", State: "UP", TotalNodes: 1}, {Name: "closed", State: "DOWN", TotalNodes: 1}}
	if !reflect.DeepEqual(partitions, wantPartitions) {
		t.Errorf("ParsePartitions()\n got %+v\nwant %+v", partitions, wantPartitions)
	}
}
