// Package slurm runs the commands of Slurm and reads what they print with
// --json, the same whichever version of the data_parser printed it.
package slurm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/mozership/probe-slurm/internal/runner"
)

// Client runs the commands of Slurm by a runner.
type Client struct {
	Runner runner.Runner
	// BinDir is the directory of the commands, or empty for the PATH to
	// find them.
	BinDir string
}

// ErrNotFound is the error of a job neither squeue nor sacct knows.
var ErrNotFound = errors.New("job not found")

// Command returns the command line of argv as the client runs it.
func (c Client) Command(argv []string) []string {
	out := slices.Clone(argv)
	if c.BinDir != "" {
		out[0] = path.Join(c.BinDir, out[0])
	}
	return out
}

func (c Client) run(ctx context.Context, argv []string, stdin []byte) (runner.Output, error) {
	return c.Runner.Run(ctx, c.Command(argv), stdin)
}

// Refusal is what Slurm said when it refused what it was asked, such as a
// job for a partition that does not exist.
type Refusal struct {
	Message string
}

func (r *Refusal) Error() string { return r.Message }

func message(out runner.Output) string {
	msg := strings.TrimSpace(string(out.Stderr))
	if msg == "" {
		msg = strings.TrimSpace(string(out.Stdout))
	}
	if msg == "" {
		msg = fmt.Sprintf("exited with %d", out.Code)
	}
	return msg
}

// SubmitArgs are the arguments of sbatch for a job; the script is its
// standard input.
func SubmitArgs(options []string) []string {
	return append([]string{"sbatch", "--parsable"}, options...)
}

// Submit submits the script with the options of sbatch, and returns the ID
// of the job and the cluster it went to, when sbatch names one. sbatch
// refusing the job is a *Refusal.
func (c Client) Submit(ctx context.Context, options []string, script []byte) (int64, string, error) {
	out, err := c.run(ctx, SubmitArgs(options), script)
	if err != nil {
		return 0, "", err
	}
	if out.Code != 0 {
		return 0, "", &Refusal{Message: message(out)}
	}
	// --parsable prints jobid or jobid;cluster, after any warning.
	lines := strings.Split(strings.TrimSpace(string(out.Stdout)), "\n")
	last := strings.TrimSpace(lines[len(lines)-1])
	idText, cluster, _ := strings.Cut(last, ";")
	id, err := strconv.ParseInt(idText, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("sbatch printed no job ID: %q", strings.TrimSpace(string(out.Stdout)))
	}
	return id, cluster, nil
}

// QueueArgs are the arguments of squeue for the jobs the filter names.
func QueueArgs(f Filter) []string {
	argv := []string{"squeue", "--json"}
	if f.JobID != 0 {
		argv = append(argv, "--states=all", "--jobs="+strconv.FormatInt(f.JobID, 10))
	}
	if f.User != "" {
		argv = append(argv, "--user="+f.User)
	}
	if f.Partition != "" {
		argv = append(argv, "--partition="+f.Partition)
	}
	if f.State != "" {
		argv = append(argv, "--states="+f.State)
	}
	if f.Name != "" {
		argv = append(argv, "--name="+f.Name)
	}
	if f.Account != "" {
		argv = append(argv, "--account="+f.Account)
	}
	return argv
}

// Filter names jobs by what they have; an empty field names any.
type Filter struct {
	JobID     int64
	User      string
	Partition string
	State     string
	Name      string
	Account   string
}

// Matches tells whether the job is one the filter names. The filter is
// applied to what squeue prints as well, as a version of squeue may print
// every job with --json, whatever it was asked.
func (f Filter) Matches(j Job) bool {
	return (f.JobID == 0 || j.ID == f.JobID) &&
		(f.User == "" || j.User == f.User) &&
		(f.Partition == "" || slices.Contains(strings.Split(j.Partition, ","), f.Partition)) &&
		(f.State == "" || strings.EqualFold(j.State, f.State)) &&
		(f.Name == "" || j.Name == f.Name) &&
		(f.Account == "" || j.Account == f.Account)
}

// Jobs returns the jobs in the queue the filter names.
func (c Client) Jobs(ctx context.Context, f Filter) ([]Job, error) {
	out, err := c.run(ctx, QueueArgs(f), nil)
	if err != nil {
		return nil, err
	}
	if out.Code != 0 && len(bytes.TrimSpace(out.Stdout)) == 0 {
		return nil, fmt.Errorf("squeue: %s", message(out))
	}
	jobs, err := ParseQueue(out.Stdout)
	if err != nil {
		return nil, fmt.Errorf("squeue: %w", err)
	}
	return slices.DeleteFunc(jobs, func(j Job) bool { return !f.Matches(j) }), nil
}

// Job returns the job from squeue, or from sacct once slurmctld has
// forgotten it. A job neither knows is ErrNotFound.
func (c Client) Job(ctx context.Context, id int64) (Job, error) {
	f := Filter{JobID: id}
	out, err := c.run(ctx, QueueArgs(f), nil)
	if err != nil {
		return Job{}, err
	}
	var queueErr error
	if out.Code == 0 || len(bytes.TrimSpace(out.Stdout)) > 0 {
		jobs, err := ParseQueue(out.Stdout)
		if err == nil {
			for _, j := range jobs {
				if f.Matches(j) {
					return j, nil
				}
			}
		}
		queueErr = err
	}
	if queueErr == nil && out.Code != 0 {
		queueErr = errors.New(message(out))
	}

	argv := []string{"sacct", "--json", "--jobs=" + strconv.FormatInt(id, 10)}
	acct, err := c.run(ctx, argv, nil)
	if err != nil {
		return Job{}, err
	}
	if acct.Code != 0 && len(bytes.TrimSpace(acct.Stdout)) == 0 {
		return Job{}, fmt.Errorf("%w: squeue no longer lists job %d, and sacct cannot tell of it: %s", ErrNotFound, id, message(acct))
	}
	jobs, err := ParseAccounting(acct.Stdout)
	if err != nil {
		return Job{}, fmt.Errorf("%w: squeue no longer lists job %d, and sacct cannot tell of it: %w", ErrNotFound, id, err)
	}
	for _, j := range jobs {
		if f.Matches(j) {
			return j, nil
		}
	}
	if queueErr != nil {
		return Job{}, fmt.Errorf("%w: job %d: %w", ErrNotFound, id, queueErr)
	}
	return Job{}, fmt.Errorf("%w: neither squeue nor sacct knows job %d", ErrNotFound, id)
}

// CancelArgs are the arguments of scancel for the jobs.
func CancelArgs(ids ...int64) []string {
	argv := []string{"scancel"}
	for _, id := range ids {
		argv = append(argv, strconv.FormatInt(id, 10))
	}
	return argv
}

// Cancel cancels the job, and returns it as it was before: a job already
// over is not cancelled. The job is looked up first, as scancel exits with
// 0 for a job it does not know, or one already over. scancel refusing, as
// for the job of another user, is a *Refusal.
func (c Client) Cancel(ctx context.Context, id int64) (Job, bool, error) {
	j, err := c.Job(ctx, id)
	if err != nil {
		return Job{}, false, err
	}
	if j.Done() {
		return j, false, nil
	}
	out, err := c.run(ctx, CancelArgs(id), nil)
	if err != nil {
		return j, false, err
	}
	msg := string(out.Stderr)
	if strings.Contains(msg, "already completing or completed") {
		return j, false, nil
	}
	if out.Code != 0 || strings.Contains(msg, "error:") {
		return j, false, &Refusal{Message: message(out)}
	}
	return j, true, nil
}

// CancelAll cancels the jobs, those already over aside.
func (c Client) CancelAll(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	out, err := c.run(ctx, CancelArgs(ids...), nil)
	if err != nil {
		return err
	}
	var errs []string
	for line := range strings.SplitSeq(string(out.Stderr), "\n") {
		if strings.Contains(line, "error:") && !strings.Contains(line, "already completing or completed") && !strings.Contains(line, "Invalid job id") {
			errs = append(errs, strings.TrimSpace(line))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// NodesArgs are the arguments of scontrol for the nodes, or every node.
func NodesArgs(names string) []string {
	argv := []string{"scontrol", "--json", "show", "node"}
	if names != "" {
		argv = append(argv, names)
	}
	return argv
}

// Nodes returns the nodes, as a host list such as c[1-4] names them, or
// every node.
func (c Client) Nodes(ctx context.Context, names string) ([]Node, error) {
	out, err := c.run(ctx, NodesArgs(names), nil)
	if err != nil {
		return nil, err
	}
	if out.Code != 0 && len(bytes.TrimSpace(out.Stdout)) == 0 {
		return nil, fmt.Errorf("scontrol show node: %s", message(out))
	}
	nodes, err := ParseNodes(out.Stdout)
	if err != nil {
		return nil, fmt.Errorf("scontrol show node: %w", err)
	}
	return nodes, nil
}

// PartitionsArgs are the arguments of scontrol for the partition, or every
// partition.
func PartitionsArgs(name string) []string {
	argv := []string{"scontrol", "--json", "show", "partition"}
	if name != "" {
		argv = append(argv, name)
	}
	return argv
}

// Partitions returns the partition, or every partition, with the states of
// their nodes counted.
func (c Client) Partitions(ctx context.Context, name string) ([]Partition, error) {
	out, err := c.run(ctx, PartitionsArgs(name), nil)
	if err != nil {
		return nil, err
	}
	if out.Code != 0 && len(bytes.TrimSpace(out.Stdout)) == 0 {
		return nil, fmt.Errorf("scontrol show partition: %s", message(out))
	}
	partitions, err := ParsePartitions(out.Stdout)
	if err != nil {
		return nil, fmt.Errorf("scontrol show partition: %w", err)
	}
	if name != "" {
		partitions = slices.DeleteFunc(partitions, func(p Partition) bool { return p.Name != name })
		if len(partitions) == 0 {
			return nil, fmt.Errorf("scontrol show partition: no partition %s", name)
		}
	}
	nodes, err := c.Nodes(ctx, "")
	if err != nil {
		return nil, err
	}
	for i := range partitions {
		for _, n := range nodes {
			if slices.Contains(n.Partitions, partitions[i].Name) {
				partitions[i].Count(n)
			}
		}
	}
	return partitions, nil
}

// ReadArgs are the arguments of head for the first limit bytes of the file,
// and one more to tell whether there is more.
func ReadArgs(file string, limit int) []string {
	return []string{"head", "-c", strconv.Itoa(limit + 1), "--", file}
}

// ReadFile returns the first limit bytes of the file, as the host the
// commands run on reads it, and whether it has more.
func (c Client) ReadFile(ctx context.Context, file string, limit int) ([]byte, bool, error) {
	out, err := c.Runner.Run(ctx, ReadArgs(file, limit), nil)
	if err != nil {
		return nil, false, err
	}
	if out.Code != 0 {
		return nil, false, fmt.Errorf("failed to read %s: %s", file, message(out))
	}
	if len(out.Stdout) > limit {
		return out.Stdout[:limit], true, nil
	}
	return out.Stdout, false, nil
}
