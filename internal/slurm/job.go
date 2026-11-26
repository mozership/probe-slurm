package slurm

import (
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Job is a job as squeue or sacct tells it, the same whichever told it.
type Job struct {
	ID        int64
	Name      string
	User      string
	Account   string
	Partition string
	// State is the base state, such as PENDING or COMPLETED, without the
	// flags that follow it.
	State    string
	Reason   string
	Nodes    string
	ExitCode int64
	Signal   int64
	Submit   time.Time
	// Start is when the job started, and End when it ended; zero until it
	// does, though Slurm tells when it expects them.
	Start      time.Time
	End        time.Time
	StdoutPath string
	Comment    string
}

// terminal are the base states of a job that is over.
var terminal = []string{
	"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY",
	"NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE",
}

// Done tells whether the job is over.
func (j Job) Done() bool {
	return slices.Contains(terminal, j.State)
}

// Wait is how long the job waited in the queue, or has waited so far.
func (j Job) Wait(now time.Time) time.Duration {
	if j.Submit.IsZero() {
		return 0
	}
	if !j.Start.IsZero() {
		return j.Start.Sub(j.Submit)
	}
	if j.State == "PENDING" {
		return now.Sub(j.Submit)
	}
	return 0
}

// Elapsed is how long the job ran, or has run so far.
func (j Job) Elapsed(now time.Time) time.Duration {
	if j.Start.IsZero() {
		return 0
	}
	if !j.End.IsZero() {
		return j.End.Sub(j.Start)
	}
	if j.Done() {
		return 0
	}
	return now.Sub(j.Start)
}

// ctldJob is a job as slurmctld tells it to squeue and scontrol.
type ctldJob struct {
	JobID       int64    `json:"job_id"`
	Name        string   `json:"name"`
	UserName    string   `json:"user_name"`
	Account     string   `json:"account"`
	Partition   string   `json:"partition"`
	JobState    states   `json:"job_state"`
	StateReason string   `json:"state_reason"`
	Nodes       string   `json:"nodes"`
	Comment     comment  `json:"comment"`
	SubmitTime  number   `json:"submit_time"`
	StartTime   number   `json:"start_time"`
	EndTime     number   `json:"end_time"`
	ExitCode    exitCode `json:"exit_code"`
	StandardOut string   `json:"standard_output"`
	WorkDir     string   `json:"current_working_directory"`
	ArrayJobID  number   `json:"array_job_id"`
	ArrayTaskID number   `json:"array_task_id"`
}

func (c ctldJob) job() Job {
	j := Job{
		ID:        c.JobID,
		Name:      c.Name,
		User:      c.UserName,
		Account:   c.Account,
		Partition: c.Partition,
		State:     c.JobState.base(),
		Reason:    c.StateReason,
		Nodes:     c.Nodes,
		ExitCode:  c.ExitCode.code,
		Signal:    c.ExitCode.signal,
		Submit:    c.SubmitTime.time(),
		Comment:   string(c.Comment),
	}
	// slurmctld tells when it expects a pending job to start, and a job
	// not yet over to end.
	if j.State != "PENDING" {
		j.Start = c.StartTime.time()
	}
	if j.Done() {
		j.End = c.EndTime.time()
	}
	if j.Reason == "None" {
		j.Reason = ""
	}
	j.StdoutPath = expandPath(c.StandardOut, c.WorkDir, j, c.ArrayJobID.value(), c.ArrayTaskID)
	return j
}

// dbJob is a job as slurmdbd tells it to sacct.
type dbJob struct {
	JobID     int64  `json:"job_id"`
	Name      string `json:"name"`
	User      string `json:"user"`
	Account   string `json:"account"`
	Partition string `json:"partition"`
	Nodes     string `json:"nodes"`
	State     struct {
		Current states `json:"current"`
		Reason  string `json:"reason"`
	} `json:"state"`
	ExitCode exitCode `json:"exit_code"`
	Time     struct {
		Submission number `json:"submission"`
		Start      number `json:"start"`
		End        number `json:"end"`
	} `json:"time"`
	Comment        comment `json:"comment"`
	Stdout         string  `json:"stdout"`
	StdoutExpanded string  `json:"stdout_expanded"`
	WorkDir        string  `json:"working_directory"`
	Array          struct {
		JobID number `json:"job_id"`
		Task  number `json:"task_id"`
	} `json:"array"`
}

func (d dbJob) job() Job {
	j := Job{
		ID:        d.JobID,
		Name:      d.Name,
		User:      d.User,
		Account:   d.Account,
		Partition: d.Partition,
		State:     d.State.Current.base(),
		Reason:    d.State.Reason,
		Nodes:     d.Nodes,
		ExitCode:  d.ExitCode.code,
		Signal:    d.ExitCode.signal,
		Submit:    d.Time.Submission.time(),
		Start:     d.Time.Start.time(),
		End:       d.Time.End.time(),
		Comment:   string(d.Comment),
	}
	if j.Reason == "None" {
		j.Reason = ""
	}
	if j.Nodes == "None assigned" {
		j.Nodes = ""
	}
	if !j.Done() {
		j.End = time.Time{}
	}
	if d.StdoutExpanded != "" {
		j.StdoutPath = d.StdoutExpanded
	} else {
		j.StdoutPath = expandPath(d.Stdout, d.WorkDir, j, d.Array.JobID.value(), d.Array.Task)
	}
	return j
}

// ParseQueue reads what squeue --json or scontrol show job --json printed.
func ParseQueue(b []byte) ([]Job, error) {
	var out struct {
		reply
		Jobs []ctldJob `json:"jobs"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("failed to read the jobs: %w", err)
	}
	if err := out.err(); err != nil {
		return nil, err
	}
	jobs := make([]Job, 0, len(out.Jobs))
	for _, c := range out.Jobs {
		jobs = append(jobs, c.job())
	}
	return jobs, nil
}

// ParseAccounting reads what sacct --json printed. A job requeued is told
// once for each time it ran, and only the last is kept.
func ParseAccounting(b []byte) ([]Job, error) {
	var out struct {
		reply
		Jobs []dbJob `json:"jobs"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("failed to read the jobs: %w", err)
	}
	if err := out.err(); err != nil {
		return nil, err
	}
	jobs := make([]Job, 0, len(out.Jobs))
	seen := map[int64]int{}
	for _, d := range out.Jobs {
		j := d.job()
		if i, ok := seen[j.ID]; ok {
			jobs[i] = j
			continue
		}
		seen[j.ID] = len(jobs)
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// expandPath expands the patterns of a filename sbatch takes, such as
// slurm-%j.out, which slurmctld tells as they were given, and makes it
// absolute by the working directory of the job.
func expandPath(p, workDir string, j Job, arrayJobID int64, arrayTask number) string {
	if p == "" {
		return ""
	}
	if strings.Contains(p, "%") {
		var b strings.Builder
		for i := 0; i < len(p); i++ {
			if p[i] != '%' || i+1 == len(p) {
				b.WriteByte(p[i])
				continue
			}
			i++
			switch p[i] {
			case '%':
				b.WriteByte('%')
			case 'j', 'J':
				b.WriteString(strconv.FormatInt(j.ID, 10))
			case 'A':
				if arrayJobID > 0 {
					b.WriteString(strconv.FormatInt(arrayJobID, 10))
				} else {
					b.WriteString(strconv.FormatInt(j.ID, 10))
				}
			case 'a':
				if arrayTask.set && !arrayTask.infinite {
					b.WriteString(strconv.FormatInt(arrayTask.n, 10))
				} else {
					b.WriteString("4294967294")
				}
			case 'x':
				b.WriteString(j.Name)
			case 'u':
				b.WriteString(j.User)
			default:
				// Those that need what the job does not tell, such as %N
				// and %t, and padded ones, such as %4j, are left as given.
				b.WriteByte('%')
				b.WriteByte(p[i])
			}
		}
		p = b.String()
	}
	if !path.IsAbs(p) && workDir != "" {
		p = path.Join(workDir, p)
	}
	return p
}
