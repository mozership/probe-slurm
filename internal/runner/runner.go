// Package runner runs the commands of Slurm on the host Probe runs on, or on
// a login node over SSH, alike.
package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

// Output is what a command printed and how it exited.
type Output struct {
	Stdout []byte
	Stderr []byte
	Code   int
}

// Runner runs a command, given as its arguments, with stdin as its input. A
// command that runs and exits with any code is an Output; only one that
// cannot be run, or is stopped by ctx, is an error.
type Runner interface {
	Run(ctx context.Context, argv []string, stdin []byte) (Output, error)
	Close() error
}

// Local runs the commands on the host Probe runs on.
type Local struct{}

func (Local) Run(ctx context.Context, argv []string, stdin []byte) (Output, error) {
	if len(argv) == 0 {
		return Output{}, errors.New("no command to run")
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	err := cmd.Run()
	out := Output{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if ctx.Err() != nil {
		return out, fmt.Errorf("%s: %w", argv[0], ctx.Err())
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		out.Code = exit.ExitCode()
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("%s: %w", argv[0], err)
	}
	return out, nil
}

func (Local) Close() error { return nil }

var safeArg = regexp.MustCompile(`^[A-Za-z0-9_@%+=:,./-]+$`)

// Quote returns argv as one command line a POSIX shell, or csh, splits back
// into the same arguments.
func Quote(argv []string) string {
	quoted := make([]string, len(argv))
	for i, a := range argv {
		if safeArg.MatchString(a) {
			quoted[i] = a
			continue
		}
		quoted[i] = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
	}
	return strings.Join(quoted, " ")
}
