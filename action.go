package main

import (
	"context"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe-slurm/internal/runner"
	"github.com/mozership/probe-slurm/internal/slurm"
	"github.com/mozership/probe/sdk"
)

// DefaultTimeout bounds a step that does not set with.timeout, besides
// what it waits for a job.
const DefaultTimeout = 30 * time.Second

// The operations a step can do, each by a key of with that holds what it
// does it to. A step has exactly one of them.
const (
	opSubmit     = "submit"
	opJob        = "job"
	opJobs       = "jobs"
	opCancel     = "cancel"
	opPartitions = "partitions"
	opNodes      = "nodes"
)

var operations = []string{opSubmit, opJob, opJobs, opCancel, opPartitions, opNodes}

// writes are the operations that change what Slurm holds.
var writes = []string{opSubmit, opCancel}

// jobParams are the keys of with that only job reads.
var jobParams = []string{"wait", "poll", "output"}

var commonParams = []string{"host", "port", "user", "key_file", "key_passphrase", "known_hosts", "bin_dir", "endpoint", "token", "timeout"}

// params are the keys the action takes in with, as action.yml declares
// them.
var params = slices.Concat(operations, jobParams, commonParams)

// keeps are the kinds of guard the action keeps to, as action.yml declares
// them.
var keeps = []string{sdk.KindReadOnly, sdk.KindAllowHost}

// Action asks Slurm for a step: by its commands, on a login node over SSH
// or on the host Probe runs on, or by slurmrestd.
type Action struct {
	log hclog.Logger
	// getenv reads the environment. It defaults to os.Getenv.
	getenv func(string) string
	// now tells the time. It defaults to time.Now.
	now func() time.Time
	// dial returns the runner for a connection. It defaults to a local
	// runner without a host, and an SSH one with.
	dial func(ctx context.Context, c connection, getenv func(string) string) (runner.Runner, error)
	// http sends the requests to slurmrestd. It defaults to a client of
	// its own.
	http *http.Client
	// ledgerDir is where the jobs a run submitted are noted for its
	// cleanup. It defaults to probe-slurm under os.TempDir.
	ledgerDir string
}

func (a *Action) init() {
	if a.log == nil {
		a.log = hclog.NewNullLogger()
	}
	if a.getenv == nil {
		a.getenv = os.Getenv
	}
	if a.now == nil {
		a.now = time.Now
	}
	if a.dial == nil {
		a.dial = dial
	}
	if a.http == nil {
		a.http = &http.Client{}
	}
	if a.ledgerDir == "" {
		a.ledgerDir = defaultLedgerDir()
	}
}

// connection says how Slurm is asked: by slurmrestd at Endpoint, or by its
// commands on a login node, or on this host when Host is empty too. It is
// noted for the cleanup of a run without the passphrase of the key or the
// token.
type connection struct {
	Endpoint      string `json:"endpoint,omitempty"`
	Token         string `json:"-"`
	Host          string `json:"host,omitempty"`
	Port          int    `json:"port,omitempty"`
	User          string `json:"user,omitempty"`
	KeyFile       string `json:"key_file,omitempty"`
	KnownHosts    string `json:"known_hosts,omitempty"`
	BinDir        string `json:"bin_dir,omitempty"`
	KeyPassphrase string `json:"-"`
}

func (c connection) ssh() runner.SSHConfig {
	return runner.SSHConfig{
		Host: c.Host, Port: c.Port, User: c.User,
		KeyFile: c.KeyFile, KeyPassphrase: c.KeyPassphrase, KnownHosts: c.KnownHosts,
		ClientVersion: "SSH-2.0-probe-slurm_" + version,
	}
}

// where is what a message tells the connection by.
func (c connection) where() string {
	switch {
	case c.Endpoint != "":
		return c.Endpoint
	case c.Host != "":
		return c.ssh().Address()
	}
	return "this host"
}

// endpointAddress is the host and port of the endpoint, as the guard takes
// them.
func (c connection) endpointAddress() string {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return c.Endpoint
	}
	if u.Port() != "" {
		return u.Host
	}
	port := "80"
	if u.Scheme == "https" {
		port = "443"
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// backend returns how Slurm is asked over the connection, and what closes
// it.
func (a *Action) backend(ctx context.Context, c connection) (slurm.Backend, func(), error) {
	if c.Endpoint != "" {
		return &slurm.REST{URL: c.Endpoint, Token: c.Token, User: c.User, UserAgent: "probe-slurm/" + version, HTTP: a.http}, func() {}, nil
	}
	r, err := a.dial(ctx, c, a.getenv)
	if err != nil {
		return nil, nil, err
	}
	return slurm.CLI{Runner: r, BinDir: c.BinDir}, func() { _ = r.Close() }, nil
}

func dial(ctx context.Context, c connection, getenv func(string) string) (runner.Runner, error) {
	if c.Host == "" {
		return runner.Local{}, nil
	}
	return runner.DialSSH(ctx, c.ssh(), getenv)
}

// Run runs the step in with, under no guard.
func (a *Action) Run(with map[string]any) (map[string]any, error) {
	ret, _, err := a.RunStep(sdk.Call{With: with})
	return ret, err
}

// RunStep runs the step in call.With under the guard of the run. What Slurm
// answers is a result, a job it refuses included; only a command that
// cannot run, a job it cannot find, or a step the guard refuses is an
// error.
func (a *Action) RunStep(call sdk.Call) (map[string]any, map[string]any, error) {
	a.init()
	sdk.LogParams(a.log, "received slurm parameters", withoutSecrets(call.With))

	s, err := parseStep(call.With, a.getenv)
	if err != nil {
		return nil, nil, err
	}
	if err := checkGuard(s, call.Guard); err != nil {
		return nil, nil, err
	}

	start := a.now()
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	client, closeBackend, err := a.backend(ctx, s.conn)
	if err != nil {
		return nil, nil, err
	}
	defer closeBackend()

	req := s.req()
	var res map[string]any
	var ok bool
	switch s.op {
	case opSubmit:
		res, ok, err = a.submit(ctx, client, s, call, req)
	case opJob:
		res, ok, err = a.job(ctx, client, s, req)
	case opJobs:
		res, ok, err = a.jobs(ctx, client, s, req)
	case opCancel:
		res, ok, err = a.cancel(ctx, client, s, req)
	case opPartitions:
		res, ok, err = a.partitions(ctx, client, s, req)
	case opNodes:
		res, ok, err = a.nodes(ctx, client, s, req)
	}
	describe(client, req)
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("the step did not finish within its timeout of %s: %w", s.timeout, err)
	}
	ret := map[string]any{"req": req, "res": res, "rt": a.now().Sub(start), "status": 1}
	if ok {
		ret["status"] = 0
	}
	sdk.LogOutcome(a.log, "slurm "+s.op, ret, err)
	if err != nil {
		return nil, nil, err
	}
	return ret, nil, nil
}

// secretParams are the keys of with that are never logged or told.
var secretParams = []string{"key_passphrase", "token"}

// withoutSecrets returns with as it can be logged, the passphrase of the
// key and the token replaced.
func withoutSecrets(with map[string]any) map[string]any {
	out := maps.Clone(with)
	for _, k := range secretParams {
		if _, ok := out[k]; ok {
			out[k] = "<redacted>"
		}
	}
	return out
}

func checkGuard(s step, g sdk.Guard) error {
	if g.ReadOnly && slices.Contains(writes, s.op) {
		return sdk.Refuse("%s changes what Slurm holds, and the run is read-only; only %s are run", s.op, strings.Join([]string{opJob, opJobs, opPartitions, opNodes}, ", "))
	}
	if len(g.AllowHosts) == 0 {
		return nil
	}
	if s.conn.Endpoint != "" {
		return g.CheckHost(s.conn.endpointAddress())
	}
	if s.conn.Host == "" {
		return sdk.Refuse("without host, the commands of Slurm reach whatever slurmctld this host is set up for, which --allow-host cannot check; give the login node as host, or slurmrestd as endpoint")
	}
	return g.CheckHost(s.conn.ssh().Address())
}
