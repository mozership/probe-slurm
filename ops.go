package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mozership/probe-slurm/internal/runner"
	"github.com/mozership/probe-slurm/internal/slurm"
	"github.com/mozership/probe/sdk"
)

// resFields are the fields of res the operations return, as action.yml
// declares them.
var resFields = []string{
	"job_id", "cluster", "error", "name", "user", "account", "partition", "state", "reason", "nodes",
	"exit_code", "signal", "submit_time", "start_time", "end_time", "wait_sec", "elapsed_sec",
	"stdout_path", "done", "stdout", "stdout_truncated", "jobs", "count", "cancelled", "partitions",
}

// OutputLimit is the most of the output of a job a step reads.
const OutputLimit = 1 << 20

// req is what the step did, as the result tells it: never the passphrase.
func (s step) req() map[string]any {
	req := map[string]any{"operation": s.op}
	if s.conn.Host != "" {
		req["host"] = s.conn.Host
		req["port"] = s.conn.Port
		req["user"] = s.conn.User
	}
	if s.conn.BinDir != "" {
		req["bin_dir"] = s.conn.BinDir
	}
	switch s.op {
	case opSubmit:
		if s.submit.file != "" {
			req["file"] = s.submit.file
		} else {
			req["script"] = s.submit.script
		}
		req["keep"] = s.submit.keep
	case opJob:
		req["job_id"] = s.jobID
		req["wait"] = s.wait.String()
		req["output"] = s.output
	case opCancel:
		req["job_id"] = s.jobID
	case opJobs:
		req["user"], req["partition"], req["state"], req["name"], req["account"] = s.filter.User, s.filter.Partition, s.filter.State, s.filter.Name, s.filter.Account
	case opPartitions, opNodes:
		req["name"] = s.name
	}
	return req
}

func command(c slurm.Client, argv []string) string {
	return runner.Quote(c.Command(argv))
}

func (a *Action) submit(ctx context.Context, c slurm.Client, s step, call sdk.Call, req map[string]any) (map[string]any, bool, error) {
	script := []byte(s.submit.script)
	if s.submit.file != "" {
		var err error
		if script, err = os.ReadFile(s.submit.file); err != nil {
			return nil, false, fmt.Errorf("failed to read with.submit.file: %w", err)
		}
	}
	req["command"] = command(c, slurm.SubmitArgs(s.submit.options))

	id, cluster, err := c.Submit(ctx, s.submit.options, script)
	if refused, ok := errors.AsType[*slurm.Refusal](err); ok {
		return map[string]any{"job_id": 0, "cluster": "", "error": refused.Message}, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !s.submit.keep {
		if err := a.note(call, s.conn, id); err != nil {
			a.log.Warn("the job is left in Slurm once the run is over, as it could not be noted for the cleanup", "job_id", id, "error", err)
		}
	}
	return map[string]any{"job_id": id, "cluster": cluster, "error": ""}, true, nil
}

func (a *Action) job(ctx context.Context, c slurm.Client, s step, req map[string]any) (map[string]any, bool, error) {
	req["command"] = command(c, slurm.QueueArgs(slurm.Filter{JobID: s.jobID}))
	deadline := a.now().Add(s.wait)
	var j slurm.Job
	for {
		var err error
		if j, err = c.Job(ctx, s.jobID); err != nil {
			return nil, false, err
		}
		left := deadline.Sub(a.now())
		if j.Done() || left <= 0 {
			break
		}
		a.log.Trace("waiting for the job", "job_id", s.jobID, "state", j.State)
		timer := time.NewTimer(min(s.poll, left))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-timer.C:
		}
	}

	res := jobFields(j, a.now())
	if s.output {
		res["stdout"], res["stdout_truncated"] = "", false
		if !j.Start.IsZero() {
			if j.StdoutPath == "" {
				return nil, false, fmt.Errorf("slurm does not tell where job %d writes its output", j.ID)
			}
			out, truncated, err := c.ReadFile(ctx, j.StdoutPath, OutputLimit)
			if err != nil {
				return nil, false, err
			}
			res["stdout"], res["stdout_truncated"] = string(out), truncated
		}
	}
	return res, j.State == "COMPLETED" && j.ExitCode == 0 && j.Signal == 0, nil
}

func (a *Action) jobs(ctx context.Context, c slurm.Client, s step, req map[string]any) (map[string]any, bool, error) {
	req["command"] = command(c, slurm.QueueArgs(s.filter))
	jobs, err := c.Jobs(ctx, s.filter)
	if err != nil {
		return nil, false, err
	}
	now := a.now()
	list := make([]any, 0, len(jobs))
	for _, j := range jobs {
		list = append(list, jobFields(j, now))
	}
	return map[string]any{"jobs": list, "count": len(list)}, true, nil
}

func (a *Action) cancel(ctx context.Context, c slurm.Client, s step, req map[string]any) (map[string]any, bool, error) {
	req["command"] = command(c, slurm.CancelArgs(s.jobID))
	j, cancelled, err := c.Cancel(ctx, s.jobID)
	if refused, ok := errors.AsType[*slurm.Refusal](err); ok {
		return map[string]any{"job_id": s.jobID, "state": j.State, "cancelled": false, "error": refused.Message}, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return map[string]any{"job_id": s.jobID, "state": j.State, "cancelled": cancelled, "error": ""}, true, nil
}

func (a *Action) partitions(ctx context.Context, c slurm.Client, s step, req map[string]any) (map[string]any, bool, error) {
	req["command"] = command(c, slurm.PartitionsArgs(s.name))
	partitions, err := c.Partitions(ctx, s.name)
	if err != nil {
		return nil, false, err
	}
	list := make([]any, 0, len(partitions))
	for _, p := range partitions {
		list = append(list, map[string]any{
			"name": p.Name, "state": p.State, "total_nodes": p.TotalNodes,
			"idle": p.Idle, "mixed": p.Mixed, "allocated": p.Allocated,
			"down": p.Down, "drained": p.Drained, "other": p.Other,
		})
	}
	return map[string]any{"partitions": list, "count": len(list)}, true, nil
}

func (a *Action) nodes(ctx context.Context, c slurm.Client, s step, req map[string]any) (map[string]any, bool, error) {
	req["command"] = command(c, slurm.NodesArgs(s.name))
	nodes, err := c.Nodes(ctx, s.name)
	if err != nil {
		return nil, false, err
	}
	list := make([]any, 0, len(nodes))
	for _, n := range nodes {
		list = append(list, map[string]any{
			"name": n.Name, "state": n.State, "flags": toAny(n.Flags), "partitions": toAny(n.Partitions),
			"reason": n.Reason, "cpus": n.CPUs, "alloc_cpus": n.AllocCPUs,
		})
	}
	return map[string]any{"nodes": list, "count": len(list)}, true, nil
}

func jobFields(j slurm.Job, now time.Time) map[string]any {
	return map[string]any{
		"job_id":      j.ID,
		"name":        j.Name,
		"user":        j.User,
		"account":     j.Account,
		"partition":   j.Partition,
		"state":       j.State,
		"reason":      j.Reason,
		"nodes":       j.Nodes,
		"exit_code":   j.ExitCode,
		"signal":      j.Signal,
		"submit_time": formatTime(j.Submit),
		"start_time":  formatTime(j.Start),
		"end_time":    formatTime(j.End),
		"wait_sec":    int64(j.Wait(now).Seconds()),
		"elapsed_sec": int64(j.Elapsed(now).Seconds()),
		"stdout_path": j.StdoutPath,
		"done":        j.Done(),
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func toAny(list []string) []any {
	out := make([]any, len(list))
	for i, v := range list {
		out[i] = v
	}
	return out
}
