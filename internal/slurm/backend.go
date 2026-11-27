package slurm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Backend is how the action asks Slurm: by its commands, or by slurmrestd.
// Both answer with the same jobs, nodes and partitions.
type Backend interface {
	// Submit submits the job, and returns its ID and the cluster it went
	// to, when Slurm names one. Slurm refusing the job is a *Refusal.
	Submit(ctx context.Context, spec JobSpec) (int64, string, error)
	// Job returns the job, from slurmctld, or from the accounting once
	// slurmctld has forgotten it. A job neither knows is ErrNotFound.
	Job(ctx context.Context, id int64) (Job, error)
	// Jobs returns the jobs in the queue the filter names.
	Jobs(ctx context.Context, f Filter) ([]Job, error)
	// Cancel cancels the job, and returns it as it was before: a job
	// already over is not cancelled. Slurm refusing is a *Refusal.
	Cancel(ctx context.Context, id int64) (Job, bool, error)
	// CancelAll cancels the jobs, those already over or forgotten aside.
	CancelAll(ctx context.Context, ids []int64) error
	// Nodes returns the nodes, as a host list such as c[1-4] names them,
	// or every node.
	Nodes(ctx context.Context, names string) ([]Node, error)
	// Partitions returns the partition, or every partition, with the
	// states of their nodes counted.
	Partitions(ctx context.Context, name string) ([]Partition, error)
}

// ErrNotFound is the error of a job neither slurmctld nor the accounting
// knows.
var ErrNotFound = errors.New("job not found")

// Refusal is what Slurm said when it refused what it was asked, such as a
// job for a partition that does not exist.
type Refusal struct {
	Message string
}

func (r *Refusal) Error() string { return r.Message }

// JobSpec is a job to submit. An empty field leaves it to Slurm.
type JobSpec struct {
	Script    []byte
	Name      string
	Partition string
	Account   string
	QOS       string
	// TimeLimit is in minutes.
	TimeLimit int64
	// Nodes is a count, or a range of counts as 1-4.
	Nodes       string
	NTasks      int64
	CPUsPerTask int64
	// Mem is the memory per node as sbatch takes it, such as 4G.
	Mem string
	// Gres is as sbatch takes it, such as gpu:1.
	Gres   string
	Chdir  string
	Output string
	Error  string
	// Options are other options of sbatch, which only the commands take.
	Options []string
	// Environment is the environment of the job, as NAME=value, which
	// only slurmrestd takes; sbatch exports its own.
	Environment []string
}

// FormatMinutes writes a time limit in minutes as sbatch takes it.
func FormatMinutes(m int64) string {
	days, m := m/(24*60), m%(24*60)
	if days > 0 {
		return fmt.Sprintf("%d-%02d:%02d:00", days, m/60, m%60)
	}
	return fmt.Sprintf("%02d:%02d:00", m/60, m%60)
}

// ParseMem reads memory as sbatch takes it, a number with K, M, G or T, or
// megabytes without, into megabytes, rounded up.
func ParseMem(s string) (int64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	scale := map[byte]float64{'K': 1.0 / 1024, 'M': 1, 'G': 1024, 'T': 1024 * 1024}
	factor := 1.0
	if t != "" {
		if f, ok := scale[t[len(t)-1]]; ok {
			factor, t = f, t[:len(t)-1]
		}
	}
	n, err := strconv.ParseFloat(t, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("memory must be a number with K, M, G or T, such as 4G, not %q", s)
	}
	mb := n * factor
	whole := int64(mb)
	if float64(whole) < mb {
		whole++
	}
	return whole, nil
}

// selectPartition keeps the partition named, or every one when no name is
// given.
func selectPartition(partitions []Partition, name string) ([]Partition, error) {
	if name == "" {
		return partitions, nil
	}
	partitions = slices.DeleteFunc(partitions, func(p Partition) bool { return p.Name != name })
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partition %s", name)
	}
	return partitions, nil
}

// countNodes counts the nodes of each partition by their state.
func countNodes(partitions []Partition, nodes []Node) {
	for i := range partitions {
		for _, n := range nodes {
			if slices.Contains(n.Partitions, partitions[i].Name) {
				partitions[i].Count(n)
			}
		}
	}
}
