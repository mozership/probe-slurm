package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mozership/probe/sdk"
)

// CleanupTimeout keeps a cleanup within the 30 seconds Probe gives it.
const CleanupTimeout = 25 * time.Second

// entry notes a job a run submitted, and where it was submitted, for the
// cleanup of the run to cancel it.
type entry struct {
	connection
	JobID int64 `json:"job_id"`
}

func defaultLedgerDir() string {
	return filepath.Join(os.TempDir(), "probe-slurm")
}

var plainRunID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func (a *Action) ledger(runID string) string {
	name := runID
	if !plainRunID.MatchString(runID) || runID == "." || runID == ".." {
		sum := sha256.Sum256([]byte(runID))
		name = hex.EncodeToString(sum[:])
	}
	return filepath.Join(a.ledgerDir, name+".jsonl")
}

// note notes the job for the cleanup of the run. The passphrase of the key
// is not noted, so a key that needs one is left to the agent or the
// default keys then, nor is the token, which is read from SLURM_JWT then.
func (a *Action) note(call sdk.Call, c connection, id int64) error {
	if call.Step.RunID == "" {
		return errors.New("the run has no ID, as a Probe from before cleanups gives none")
	}
	if c.KeyPassphrase != "" {
		c.KeyFile = ""
	}
	line, err := json.Marshal(entry{connection: c, JobID: id})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(a.ledgerDir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(a.ledger(call.Step.RunID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	// One write of a line under O_APPEND keeps the lines of steps running at
	// once apart.
	_, err = f.Write(append(line, '\n'))
	return errors.Join(err, f.Close())
}

// Cleanup cancels the jobs the steps of the run submitted, those already
// over aside, where each was submitted.
func (a *Action) Cleanup(call sdk.CleanupCall) error {
	a.init()
	path := a.ledger(call.RunID)
	entries, err := readLedger(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), CleanupTimeout)
	defer cancel()
	var conns []connection
	jobs := map[connection][]int64{}
	for _, e := range entries {
		if _, ok := jobs[e.connection]; !ok {
			conns = append(conns, e.connection)
		}
		if !slices.Contains(jobs[e.connection], e.JobID) {
			jobs[e.connection] = append(jobs[e.connection], e.JobID)
		}
	}
	var errs []error
	for _, c := range conns {
		if err := a.cancelAll(ctx, c, jobs[c]); err != nil {
			errs = append(errs, err)
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (a *Action) cancelAll(ctx context.Context, c connection, ids []int64) error {
	where := c.where()
	if c.Endpoint != "" {
		c.Token = strings.TrimPrefix(strings.TrimSpace(a.getenv("SLURM_JWT")), "SLURM_JWT=")
		if c.Token == "" {
			return fmt.Errorf("jobs %v at %s are left in Slurm: the token is not noted, and SLURM_JWT gives none to cancel them with", ids, where)
		}
	}
	client, closeBackend, err := a.backend(ctx, c)
	if err != nil {
		return fmt.Errorf("jobs %v on %s are left in Slurm: %w", ids, where, err)
	}
	defer closeBackend()
	if err := client.CancelAll(ctx, ids); err != nil {
		return fmt.Errorf("jobs %v on %s may be left in Slurm: %w", ids, where, err)
	}
	a.log.Trace("cancelled the jobs the run submitted", "jobs", ids, "where", where)
	return nil
}

func readLedger(path string) ([]entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var entries []entry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var e entry
		// A line a step stopped halfway through writing is skipped.
		if json.Unmarshal(scanner.Bytes(), &e) == nil && e.JobID > 0 {
			entries = append(entries, e)
		}
	}
	return entries, scanner.Err()
}
