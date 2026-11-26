// Command probe-slurm serves a Probe action that submits, watches and
// cancels jobs of Slurm, and reads the state of its partitions and nodes,
// on a login node over SSH or on the host Probe runs on. Probe starts it for
// a step that uses github.com/mozership/probe-slurm@<commit>.
package main

import (
	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe/sdk"
)

// version is set at build time.
var version = "dev"

func main() {
	sdk.Serve(func(log hclog.Logger) sdk.Action {
		return &Action{log: log}
	})
}
