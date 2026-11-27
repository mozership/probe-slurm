package main

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mozership/probe-slurm/internal/slurm"
	"github.com/mozership/probe/sdk"
)

// step is what with asks of a step.
type step struct {
	op      string
	conn    connection
	timeout time.Duration

	jobID  int64
	wait   time.Duration
	poll   time.Duration
	output bool

	submit submitSpec
	filter slurm.Filter
	// name is the partition, or the nodes as a host list, of partitions
	// and nodes; empty for every one.
	name string
}

// DefaultPoll is how often a job is looked at while a step waits for it.
const DefaultPoll = 10 * time.Second

// MinPoll keeps a step from asking slurmctld more often than it should be.
const MinPoll = time.Second

func parseStep(with map[string]any, getenv func(string) string) (step, error) {
	var s step
	for k := range with {
		if !slices.Contains(params, k) {
			return s, fmt.Errorf("slurm action takes %s, not %s", strings.Join(params, ", "), k)
		}
	}
	var named []string
	for _, op := range operations {
		if _, ok := with[op]; ok {
			named = append(named, op)
		}
	}
	if len(named) > 1 && submitDefaultsOnly(with[opSubmit]) {
		named = slices.DeleteFunc(named, func(op string) bool { return op == opSubmit })
	}
	switch len(named) {
	case 0:
		return s, fmt.Errorf("slurm action requires exactly one of with.%s", strings.Join(operations, ", with."))
	case 1:
	default:
		return s, fmt.Errorf("slurm action takes exactly one of %s, not %s together", strings.Join(operations, ", "), strings.Join(named, " and "))
	}
	s.op = named[0]
	if s.op != opJob {
		for _, k := range jobParams {
			if _, ok := with[k]; ok {
				return s, fmt.Errorf("with.%s is for job, not %s", k, s.op)
			}
		}
	}

	var err error
	if s.conn, err = parseConnection(with, getenv); err != nil {
		return s, err
	}
	if s.conn.Endpoint != "" {
		if err := restCanDo(with, s.op); err != nil {
			return s, err
		}
	}

	v := with[s.op]
	switch s.op {
	case opSubmit:
		s.submit, err = parseSubmit(v)
	case opJob, opCancel:
		s.jobID, err = parseJobID(v, "with."+s.op)
	case opJobs:
		s.filter, err = parseFilter(v)
	case opPartitions, opNodes:
		s.name, err = optionalName(v, "with."+s.op)
	}
	if err != nil {
		return s, err
	}

	if s.op == opJob {
		if s.wait, err = optionalDuration(with, "with.", "wait"); err != nil {
			return s, err
		}
		if s.poll, err = optionalDuration(with, "with.", "poll"); err != nil {
			return s, err
		}
		if s.poll == 0 {
			s.poll = DefaultPoll
		}
		if s.poll < MinPoll {
			return s, fmt.Errorf("with.poll must be %s or longer, not %s", MinPoll, s.poll)
		}
		if s.output, err = optionalBool(with, "with.", "output"); err != nil {
			return s, err
		}
	}

	s.timeout = DefaultTimeout + s.wait
	if _, ok := with["timeout"]; ok {
		if s.timeout, err = sdk.ParseTimeout(with["timeout"]); err != nil {
			return s, fmt.Errorf("with.timeout: %w", err)
		}
		if s.timeout <= s.wait {
			return s, fmt.Errorf("with.timeout (%s) must be longer than with.wait (%s), for the step to look at the job once it has waited", s.timeout, s.wait)
		}
	}
	return s, nil
}

// restCanDo refuses what slurmrestd cannot do, before anything is sent:
// it reads no file of a job, takes no option of sbatch, and does not know
// the home directory of the user to run a job in.
func restCanDo(with map[string]any, op string) error {
	if op == opJob {
		if output, _ := optionalBool(with, "with.", "output"); output {
			return errors.New("with.output reads the file of the job on the login node, which slurmrestd cannot; give host rather than endpoint to read it")
		}
	}
	if op != opSubmit {
		return nil
	}
	given, _ := with[opSubmit].(map[string]any)
	if _, ok := given["options"]; ok {
		return errors.New("with.submit.options are options of sbatch, which slurmrestd does not take")
	}
	if chdir, _ := optionalString(given, "with.submit.", "chdir"); chdir == "" {
		return errors.New("with.submit.chdir is required with endpoint, as slurmrestd runs a job in no directory of its own; give a directory on the cluster, such as the home directory")
	}
	return nil
}

func parseConnection(with map[string]any, getenv func(string) string) (connection, error) {
	var c connection
	var err error
	if c.Endpoint, err = optionalString(with, "with.", "endpoint"); err != nil {
		return c, err
	}
	if c.Endpoint != "" {
		return parseEndpoint(with, c, getenv)
	}
	if _, ok := with["token"]; ok {
		return c, fmt.Errorf("with.token is for with.endpoint, which is not given")
	}
	if c.Host, err = optionalString(with, "with.", "host"); err != nil {
		return c, err
	}
	if c.BinDir, err = optionalString(with, "with.", "bin_dir"); err != nil {
		return c, err
	}
	if c.BinDir != "" && !strings.HasPrefix(c.BinDir, "/") {
		return c, fmt.Errorf("with.bin_dir must be an absolute path, not %q", c.BinDir)
	}
	sshOnly := []string{"port", "user", "key_file", "key_passphrase", "known_hosts"}
	if c.Host == "" {
		for _, k := range sshOnly {
			if _, ok := with[k]; ok {
				return c, fmt.Errorf("with.%s is for logging in to with.host, which is not given", k)
			}
		}
		return c, nil
	}
	if strings.ContainsAny(c.Host, " /@") {
		return c, fmt.Errorf("with.host must be the name or the address of the login node, not %q", c.Host)
	}
	c.Port = 22
	if v, ok := with["port"]; ok {
		port, err := toInt(v, "with.port")
		if err != nil {
			return c, err
		}
		if port < 1 || port > 65535 {
			return c, fmt.Errorf("with.port must be from 1 to 65535, not %d", port)
		}
		c.Port = int(port)
	}
	if c.User, err = optionalString(with, "with.", "user"); err != nil {
		return c, err
	}
	if c.User == "" {
		c.User = getenv("USER")
	}
	if c.User == "" {
		return c, fmt.Errorf("with.user is required, as USER does not name one")
	}
	if c.KeyFile, err = optionalString(with, "with.", "key_file"); err != nil {
		return c, err
	}
	if c.KeyPassphrase, err = optionalString(with, "with.", "key_passphrase"); err != nil {
		return c, err
	}
	if c.KeyPassphrase != "" && c.KeyFile == "" {
		return c, fmt.Errorf("with.key_passphrase is for with.key_file, which is not given")
	}
	if c.KnownHosts, err = optionalString(with, "with.", "known_hosts"); err != nil {
		return c, err
	}
	return c, nil
}

// parseEndpoint reads where slurmrestd is, and the token to give it.
func parseEndpoint(with map[string]any, c connection, getenv func(string) string) (connection, error) {
	for _, k := range []string{"host", "port", "key_file", "key_passphrase", "known_hosts", "bin_dir"} {
		if _, ok := with[k]; ok {
			return c, fmt.Errorf("with.%s is for running the commands of Slurm, not for slurmrestd at with.endpoint", k)
		}
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return c, fmt.Errorf("with.endpoint must be the URL of slurmrestd, such as http://slurm.example.com:6820, not %q", c.Endpoint)
	}
	c.Endpoint = strings.TrimRight(u.String(), "/")
	if c.User, err = optionalString(with, "with.", "user"); err != nil {
		return c, err
	}
	if c.Token, err = optionalString(with, "with.", "token"); err != nil {
		return c, err
	}
	if c.Token == "" {
		c.Token = getenv("SLURM_JWT")
	}
	// scontrol token prints SLURM_JWT=<token>, which is taken as it is.
	c.Token = strings.TrimPrefix(strings.TrimSpace(c.Token), "SLURM_JWT=")
	if c.Token == "" {
		return c, errors.New("with.token is required with endpoint, as SLURM_JWT does not give one; scontrol token makes one")
	}
	return c, nil
}

// submitDefaultsOnly tells whether v is a submit without a script, as the
// defaults of a job give the steps that do something else.
func submitDefaultsOnly(v any) bool {
	given, ok := v.(map[string]any)
	if !ok {
		return false
	}
	_, script := given["script"]
	_, file := given["file"]
	return !script && !file
}

// submitKeys are the keys of with.submit.
var submitKeys = []string{
	"script", "file", "name", "partition", "account", "qos", "time", "nodes", "ntasks",
	"cpus_per_task", "mem", "gres", "chdir", "output", "error", "options", "keep",
}

// submitSpec is the job a step submits: its script is written in the
// step, or read from file when the step runs.
type submitSpec struct {
	job  slurm.JobSpec
	file string
	keep bool
}

// DefaultJobName is the name of a job whose step names none.
const DefaultJobName = "probe"

// slurmTime is a time limit as sbatch takes it: minutes, MM:SS, HH:MM:SS,
// D-HH, D-HH:MM or D-HH:MM:SS.
var slurmTime = regexp.MustCompile(`^(?:(\d+)-)?(\d+)(?::(\d+))?(?::(\d+))?$`)

func parseSubmit(v any) (submitSpec, error) {
	sub := submitSpec{}
	given, ok := v.(map[string]any)
	if !ok {
		return sub, fmt.Errorf("with.submit must be an object of the job, such as {script: ..., partition: debug}, not %T", v)
	}
	for k := range given {
		if !slices.Contains(submitKeys, k) {
			return sub, fmt.Errorf("with.submit takes %s, not %s", strings.Join(submitKeys, ", "), k)
		}
	}
	job := &sub.job
	script, err := optionalString(given, "with.submit.", "script")
	if err != nil {
		return sub, err
	}
	job.Script = []byte(script)
	if sub.file, err = optionalString(given, "with.submit.", "file"); err != nil {
		return sub, err
	}
	if (script == "") == (sub.file == "") {
		return sub, fmt.Errorf("with.submit requires exactly one of script and file")
	}

	for _, k := range []struct {
		key string
		dst *string
	}{
		{"name", &job.Name}, {"partition", &job.Partition}, {"account", &job.Account}, {"qos", &job.QOS},
		{"mem", &job.Mem}, {"gres", &job.Gres}, {"chdir", &job.Chdir}, {"output", &job.Output}, {"error", &job.Error},
	} {
		if *k.dst, err = optionalString(given, "with.submit.", k.key); err != nil {
			return sub, err
		}
	}
	if job.Name == "" {
		job.Name = DefaultJobName
	}
	if job.Mem != "" {
		if _, err := slurm.ParseMem(job.Mem); err != nil {
			return sub, fmt.Errorf("with.submit.mem: %w", err)
		}
	}

	if v, ok := given["time"]; ok {
		if job.TimeLimit, err = parseTimeLimit(v); err != nil {
			return sub, err
		}
	}
	if v, ok := given["nodes"]; ok {
		switch n := v.(type) {
		case string:
			job.Nodes = n
		default:
			i, err := toInt(v, "with.submit.nodes")
			if err != nil {
				return sub, err
			}
			job.Nodes = strconv.FormatInt(i, 10)
		}
	}
	for _, k := range []struct {
		key string
		dst *int64
	}{{"ntasks", &job.NTasks}, {"cpus_per_task", &job.CPUsPerTask}} {
		v, ok := given[k.key]
		if !ok {
			continue
		}
		i, err := toInt(v, "with.submit."+k.key)
		if err != nil {
			return sub, err
		}
		if i < 1 {
			return sub, fmt.Errorf("with.submit.%s must be 1 or more, not %d", k.key, i)
		}
		*k.dst = i
	}
	if v, ok := given["options"]; ok {
		list, ok := v.([]any)
		if !ok {
			return sub, fmt.Errorf("with.submit.options must be a list of options of sbatch, such as [--exclusive], not %T", v)
		}
		for _, o := range list {
			s, ok := o.(string)
			if !ok || !strings.HasPrefix(s, "-") {
				return sub, fmt.Errorf("with.submit.options must be options of sbatch, each starting with -, not %v", o)
			}
			job.Options = append(job.Options, s)
		}
	}
	if sub.keep, err = optionalBool(given, "with.submit.", "keep"); err != nil {
		return sub, err
	}
	return sub, nil
}

// parseTimeLimit reads a time limit into minutes: a duration such as
// 1h30m, a time as sbatch takes it such as 1-12:00:00, or a number of
// minutes, as sbatch reads one. Seconds are rounded up to a minute.
func parseTimeLimit(v any) (int64, error) {
	if s, ok := v.(string); ok {
		if d, err := time.ParseDuration(s); err == nil {
			if d <= 0 {
				return 0, fmt.Errorf("with.submit.time must be longer than 0, not %s", s)
			}
			return int64(math.Ceil(d.Minutes())), nil
		}
		if m := slurmTime.FindStringSubmatch(s); m != nil {
			minutes := slurmTimeMinutes(m)
			if minutes <= 0 {
				return 0, fmt.Errorf("with.submit.time must be longer than 0, not %s", s)
			}
			return minutes, nil
		}
		return 0, fmt.Errorf("with.submit.time must be a duration such as 1h30m, or a time as sbatch takes it such as 1:30:00, not %q", s)
	}
	minutes, err := toInt(v, "with.submit.time")
	if err != nil {
		return 0, err
	}
	if minutes <= 0 {
		return 0, fmt.Errorf("with.submit.time must be longer than 0, not %d", minutes)
	}
	return minutes, nil
}

// slurmTimeMinutes reads the parts of a time sbatch takes as slurmTime
// matched them: with days, they are D-HH[:MM[:SS]], and without,
// MM[:SS] or HH:MM:SS.
func slurmTimeMinutes(m []string) int64 {
	n := func(s string) int64 {
		i, _ := strconv.ParseInt(s, 10, 64)
		return i
	}
	var seconds int64
	switch {
	case m[1] != "":
		seconds = n(m[1])*86400 + n(m[2])*3600 + n(m[3])*60 + n(m[4])
	case m[4] != "":
		seconds = n(m[2])*3600 + n(m[3])*60 + n(m[4])
	default:
		seconds = n(m[2])*60 + n(m[3])
	}
	return (seconds + 59) / 60
}

// filterKeys are the keys of with.jobs.
var filterKeys = []string{"user", "partition", "state", "name", "account"}

func parseFilter(v any) (slurm.Filter, error) {
	var f slurm.Filter
	if v == nil {
		return f, nil
	}
	given, ok := v.(map[string]any)
	if !ok {
		return f, fmt.Errorf("with.jobs must be an object of what the jobs have, such as {user: alice}, or nothing for every job, not %T", v)
	}
	for k := range given {
		if !slices.Contains(filterKeys, k) {
			return f, fmt.Errorf("with.jobs takes %s, not %s", strings.Join(filterKeys, ", "), k)
		}
	}
	fields := map[string]*string{"user": &f.User, "partition": &f.Partition, "state": &f.State, "name": &f.Name, "account": &f.Account}
	for k, dst := range fields {
		s, err := optionalString(given, "with.jobs.", k)
		if err != nil {
			return f, err
		}
		*dst = s
	}
	f.State = strings.ToUpper(f.State)
	return f, nil
}

func parseJobID(v any, name string) (int64, error) {
	id, err := toInt(v, name)
	if err != nil {
		return 0, fmt.Errorf("%s must be the ID of a job, such as 123: %w", name, err)
	}
	if id <= 0 {
		return 0, fmt.Errorf("%s must be the ID of a job, such as 123, not %d", name, id)
	}
	return id, nil
}

func optionalName(v any, name string) (string, error) {
	if v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a name, or nothing for every one, not %T", name, v)
	}
	return strings.TrimSpace(s), nil
}

func optionalString(m map[string]any, scope, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s%s must be a string, not %T", scope, key, v)
	}
	return s, nil
}

func optionalBool(m map[string]any, scope, key string) (bool, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return false, nil
	}
	switch b := v.(type) {
	case bool:
		return b, nil
	case string:
		if parsed, err := strconv.ParseBool(b); err == nil {
			return parsed, nil
		}
	}
	return false, fmt.Errorf("%s%s must be true or false, not %v", scope, key, v)
}

func optionalDuration(m map[string]any, scope, key string) (time.Duration, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, nil
	}
	d, err := sdk.ParseTimeout(v)
	if err != nil {
		return 0, fmt.Errorf("%s%s: %w", scope, key, err)
	}
	return d, nil
}

// toInt reads a whole number, given as a number or as a string of one, as
// a template such as {{outputs.submit.job_id}} gives it.
func toInt(v any, name string) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		if n == math.Trunc(n) && math.Abs(n) < 1<<53 {
			return int64(n), nil
		}
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			return i, nil
		}
	}
	return 0, fmt.Errorf("%s must be a whole number, not %v", name, v)
}
