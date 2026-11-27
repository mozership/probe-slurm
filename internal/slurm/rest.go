package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// Versions are the versions of the API of slurmrestd the action speaks,
// newest first: v0.0.39 of Slurm 23.02 to v0.0.42 of Slurm 24.11.
var Versions = []string{"v0.0.42", "v0.0.41", "v0.0.40", "v0.0.39"}

// The codes of slurm_errno the action tells apart in what slurmrestd says.
const (
	errInvalidJobID      = 2017
	errAlreadyCompleting = 2021
)

// MaxResponse bounds what the action reads of a response of slurmrestd.
const MaxResponse = 64 << 20

// REST asks slurmrestd, by the newest version of its API both speak.
type REST struct {
	// URL is where slurmrestd is, as http://slurm.example.com:6820.
	URL string
	// Token is the JSON Web Token of the user, as scontrol token makes it.
	Token string
	// User is the user to act as, for a token of SlurmUser or root, or
	// empty for the user of the token.
	User      string
	UserAgent string
	HTTP      *http.Client
	// version is the version of the API, found by the first request.
	version string
}

// Version is the version of the API the requests were made by, or empty
// before the first.
func (r *REST) Version() string { return r.version }

// errNoPath is a path slurmrestd does not serve, as the accounting of one
// without slurmdbd.
var errNoPath = errors.New("slurmrestd does not serve the path")

type response struct {
	status int
	body   []byte
}

// json tells whether the response is one of the API, rather than a page of
// the server's own, as for a path it does not serve.
func (r response) json() bool {
	b := bytes.TrimSpace(r.body)
	return len(b) > 0 && b[0] == '{'
}

// do sends the request to the path under /slurm/ or /slurmdb/ of the
// version of the API. Before the version is known, the newest first is
// tried, and the next for a path the server does not serve.
func (r *REST) do(ctx context.Context, method, api, path string, body any) (response, error) {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return response{}, err
		}
	}
	versions := Versions
	if r.version != "" {
		versions = []string{r.version}
	}
	for _, v := range versions {
		res, err := r.send(ctx, method, "/"+api+"/"+v+path, payload)
		if err != nil {
			return response{}, err
		}
		if res.status == http.StatusNotFound && !res.json() {
			continue
		}
		r.version = v
		return res, nil
	}
	if r.version != "" {
		return response{}, fmt.Errorf("%w: %s %s/%s/%s%s", errNoPath, method, r.URL, api, r.version, path)
	}
	return response{}, fmt.Errorf("slurmrestd at %s serves none of the versions %s of its API", r.URL, strings.Join(Versions, ", "))
}

func (r *REST) send(ctx context.Context, method, path string, payload []byte) (response, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.URL+path, body)
	if err != nil {
		return response{}, err
	}
	req.Header.Set("X-SLURM-USER-TOKEN", r.Token)
	if r.User != "" {
		req.Header.Set("X-SLURM-USER-NAME", r.User)
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if r.UserAgent != "" {
		req.Header.Set("User-Agent", r.UserAgent)
	}
	res, err := r.client().Do(req)
	if err != nil {
		return response{}, fmt.Errorf("failed to reach slurmrestd: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(res.Body, MaxResponse+1))
	if err != nil {
		return response{}, fmt.Errorf("failed to read the response of slurmrestd: %w", err)
	}
	if len(b) > MaxResponse {
		return response{}, fmt.Errorf("the response of slurmrestd to %s %s is larger than %d bytes", method, path, MaxResponse)
	}
	out := response{status: res.StatusCode, body: b}
	if res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden {
		return out, fmt.Errorf("slurmrestd refused the token: %s %s", res.Status, snippet(b))
	}
	return out, nil
}

// client returns the HTTP client, made not to follow redirects: a redirect
// would carry the token, a header of its own, to a host the guard did not
// check, and turn a POST into a GET.
func (r *REST) client() *http.Client {
	c := http.Client{}
	if r.HTTP != nil {
		c = *r.HTTP
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &c
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// get returns the body of a request slurmrestd answered with 200.
func (r *REST) get(ctx context.Context, api, path string) ([]byte, error) {
	res, err := r.do(ctx, http.MethodGet, api, path, nil)
	if err != nil {
		return nil, err
	}
	if res.status != http.StatusOK {
		return nil, r.failure(http.MethodGet, api, path, res)
	}
	return res.body, nil
}

func (r *REST) failure(method, api, path string, res response) error {
	if res.json() {
		var rep reply
		if json.Unmarshal(res.body, &rep) == nil {
			if err := rep.err(); err != nil {
				return fmt.Errorf("%s /%s/%s%s: %w", method, api, r.version, path, err)
			}
		}
	}
	return fmt.Errorf("%s /%s/%s%s: slurmrestd answered %d: %s", method, api, r.version, path, res.status, snippet(res.body))
}

// jobDesc is a job as slurmrestd takes it. The numbers are plain, which
// every version reads as set.
type jobDesc struct {
	Name                    string   `json:"name,omitempty"`
	Partition               string   `json:"partition,omitempty"`
	Account                 string   `json:"account,omitempty"`
	QOS                     string   `json:"qos,omitempty"`
	TimeLimit               int64    `json:"time_limit,omitempty"`
	Nodes                   string   `json:"nodes,omitempty"`
	Tasks                   int64    `json:"tasks,omitempty"`
	CPUsPerTask             int64    `json:"cpus_per_task,omitempty"`
	MemoryPerNode           *int64   `json:"memory_per_node,omitempty"`
	TRESPerNode             string   `json:"tres_per_node,omitempty"`
	CurrentWorkingDirectory string   `json:"current_working_directory"`
	StandardOutput          string   `json:"standard_output,omitempty"`
	StandardError           string   `json:"standard_error,omitempty"`
	Environment             []string `json:"environment"`
}

// tres writes the generic resources as sbatch takes them, as gpu:1, as
// slurmrestd takes them, as gres/gpu:1.
func tres(gres string) string {
	var out []string
	for g := range strings.SplitSeq(gres, ",") {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		if !strings.Contains(g, "/") {
			g = "gres/" + g
		}
		out = append(out, g)
	}
	return strings.Join(out, ",")
}

// Submit posts the job to slurmrestd, which reads none of the #SBATCH lines
// of its script, and takes none of the options of sbatch.
func (r *REST) Submit(ctx context.Context, spec JobSpec) (int64, string, error) {
	if len(spec.Options) > 0 {
		return 0, "", errors.New("slurmrestd takes none of the options of sbatch")
	}
	if spec.Chdir == "" {
		return 0, "", errors.New("slurmrestd requires the working directory of the job")
	}
	desc := jobDesc{
		Name: spec.Name, Partition: spec.Partition, Account: spec.Account, QOS: spec.QOS,
		TimeLimit: spec.TimeLimit, Nodes: spec.Nodes, Tasks: spec.NTasks, CPUsPerTask: spec.CPUsPerTask,
		TRESPerNode: tres(spec.Gres), CurrentWorkingDirectory: spec.Chdir,
		StandardOutput: spec.Output, StandardError: spec.Error, Environment: spec.Environment,
	}
	if desc.Environment == nil {
		desc.Environment = []string{}
	}
	if spec.Mem != "" {
		mb, err := ParseMem(spec.Mem)
		if err != nil {
			return 0, "", err
		}
		// 0 is all the memory of the node, as sbatch --mem=0 takes it.
		desc.MemoryPerNode = &mb
	}
	body := map[string]any{"script": string(spec.Script), "job": desc}
	res, err := r.do(ctx, http.MethodPost, "slurm", "/job/submit", body)
	if err != nil {
		return 0, "", err
	}
	var out struct {
		reply
		JobID number `json:"job_id"`
	}
	if !res.json() || json.Unmarshal(res.body, &out) != nil {
		return 0, "", r.failure(http.MethodPost, "slurm", "/job/submit", res)
	}
	if err := out.err(); err != nil {
		return 0, "", &Refusal{Message: err.Error()}
	}
	if res.status != http.StatusOK || out.JobID.value() <= 0 {
		return 0, "", r.failure(http.MethodPost, "slurm", "/job/submit", res)
	}
	return out.JobID.value(), "", nil
}

// hasError tells whether slurmrestd answered with the error of the code.
func hasError(res response, code int) bool {
	var rep reply
	if !res.json() || json.Unmarshal(res.body, &rep) != nil {
		return false
	}
	return slices.ContainsFunc(rep.Errors, func(e replyError) bool { return e.ErrorNumber == code })
}

func jobPath(id int64) string {
	return "/job/" + strconv.FormatInt(id, 10)
}

// Job returns the job from slurmctld, or from slurmdbd once slurmctld has
// forgotten it, where slurmrestd serves its accounting.
func (r *REST) Job(ctx context.Context, id int64) (Job, error) {
	f := Filter{JobID: id}
	res, err := r.do(ctx, http.MethodGet, "slurm", jobPath(id), nil)
	if err != nil {
		return Job{}, err
	}
	var ctldErr error
	switch res.status {
	case http.StatusOK:
		jobs, err := ParseQueue(res.body)
		if err != nil {
			return Job{}, fmt.Errorf("GET /slurm/%s%s: %w", r.version, jobPath(id), err)
		}
		for _, j := range jobs {
			if f.Matches(j) {
				return j, nil
			}
		}
	case http.StatusNotFound:
		ctldErr = r.failure(http.MethodGet, "slurm", jobPath(id), res)
	default:
		// An older version may answer a job it has forgotten otherwise than
		// with 404, telling it as an invalid job ID all the same.
		if hasError(res, errInvalidJobID) {
			ctldErr = r.failure(http.MethodGet, "slurm", jobPath(id), res)
			break
		}
		return Job{}, r.failure(http.MethodGet, "slurm", jobPath(id), res)
	}

	b, err := r.get(ctx, "slurmdb", jobPath(id))
	if errors.Is(err, errNoPath) {
		return Job{}, fmt.Errorf("%w: slurmctld no longer knows job %d, and slurmrestd serves no accounting to tell of it", ErrNotFound, id)
	}
	if err != nil {
		return Job{}, fmt.Errorf("%w: slurmctld no longer knows job %d, and the accounting cannot tell of it: %w", ErrNotFound, id, err)
	}
	jobs, err := ParseAccounting(b)
	if err != nil {
		return Job{}, fmt.Errorf("%w: slurmctld no longer knows job %d, and the accounting cannot tell of it: %w", ErrNotFound, id, err)
	}
	for _, j := range jobs {
		if f.Matches(j) {
			return j, nil
		}
	}
	if ctldErr != nil {
		return Job{}, fmt.Errorf("%w: job %d: %w", ErrNotFound, id, ctldErr)
	}
	return Job{}, fmt.Errorf("%w: neither slurmctld nor the accounting knows job %d", ErrNotFound, id)
}

// Jobs returns the jobs slurmctld knows the filter names. Without a state,
// those over are left out, as squeue leaves them.
func (r *REST) Jobs(ctx context.Context, f Filter) ([]Job, error) {
	b, err := r.get(ctx, "slurm", "/jobs")
	if err != nil {
		return nil, err
	}
	jobs, err := ParseQueue(b)
	if err != nil {
		return nil, fmt.Errorf("GET /slurm/%s/jobs: %w", r.version, err)
	}
	return slices.DeleteFunc(jobs, func(j Job) bool {
		return !f.Matches(j) || (f.State == "" && j.Done())
	}), nil
}

// cancelReply is what slurmrestd answers a DELETE of a job with: the
// errors of the request, and from v0.0.42 on, the error of each job.
type cancelReply struct {
	reply
	Status []struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	} `json:"status"`
}

// cancelError returns the error of cancelling, and whether the job was
// already over, which is no error.
func (r *REST) cancelError(id int64, res response) (over bool, err error) {
	var out cancelReply
	if !res.json() || json.Unmarshal(res.body, &out) != nil {
		return false, r.failure(http.MethodDelete, "slurm", jobPath(id), res)
	}
	var msgs []string
	for _, e := range out.Errors {
		switch e.ErrorNumber {
		case errAlreadyCompleting:
			over = true
		case errInvalidJobID:
			return false, fmt.Errorf("%w: slurmctld does not know job %d", ErrNotFound, id)
		default:
			msgs = append(msgs, e.message())
		}
	}
	for _, s := range out.Status {
		switch s.Error.Code {
		case 0:
		case errAlreadyCompleting:
			over = true
		case errInvalidJobID:
			return false, fmt.Errorf("%w: slurmctld does not know job %d", ErrNotFound, id)
		default:
			msgs = append(msgs, s.Error.Message)
		}
	}
	if len(msgs) > 0 {
		return over, &Refusal{Message: strings.Join(msgs, "; ")}
	}
	if res.status != http.StatusOK && !over {
		return false, r.failure(http.MethodDelete, "slurm", jobPath(id), res)
	}
	return over, nil
}

// Cancel cancels the job. The job is looked up first, as slurmrestd before
// v0.0.42 tells no error for one already over.
func (r *REST) Cancel(ctx context.Context, id int64) (Job, bool, error) {
	j, err := r.Job(ctx, id)
	if err != nil {
		return Job{}, false, err
	}
	if j.Done() {
		return j, false, nil
	}
	res, err := r.do(ctx, http.MethodDelete, "slurm", jobPath(id), nil)
	if err != nil {
		return j, false, err
	}
	over, err := r.cancelError(id, res)
	if err != nil {
		return j, false, err
	}
	return j, !over, nil
}

// CancelAll cancels the jobs one by one.
func (r *REST) CancelAll(ctx context.Context, ids []int64) error {
	var errs []error
	for _, id := range ids {
		res, err := r.do(ctx, http.MethodDelete, "slurm", jobPath(id), nil)
		if err == nil {
			_, err = r.cancelError(id, res)
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			errs = append(errs, fmt.Errorf("job %d: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// Nodes returns the nodes slurmctld knows, those of the host list alone
// when one is given, as slurmrestd takes the name of one node only. A name
// it does not know is left out, as scontrol leaves it.
func (r *REST) Nodes(ctx context.Context, names string) ([]Node, error) {
	var want []string
	if names != "" {
		var err error
		if want, err = ExpandHostlist(names); err != nil {
			return nil, err
		}
	}
	b, err := r.get(ctx, "slurm", "/nodes")
	if err != nil {
		return nil, err
	}
	nodes, err := ParseNodes(b)
	if err != nil {
		return nil, fmt.Errorf("GET /slurm/%s/nodes: %w", r.version, err)
	}
	if want != nil {
		nodes = slices.DeleteFunc(nodes, func(n Node) bool { return !slices.Contains(want, n.Name) })
	}
	return nodes, nil
}

// Partitions returns the partitions slurmctld knows.
func (r *REST) Partitions(ctx context.Context, name string) ([]Partition, error) {
	b, err := r.get(ctx, "slurm", "/partitions")
	if err != nil {
		return nil, err
	}
	partitions, err := ParsePartitions(b)
	if err != nil {
		return nil, fmt.Errorf("GET /slurm/%s/partitions: %w", r.version, err)
	}
	if partitions, err = selectPartition(partitions, name); err != nil {
		return nil, err
	}
	nodes, err := r.Nodes(ctx, "")
	if err != nil {
		return nil, err
	}
	countNodes(partitions, nodes)
	return partitions, nil
}
