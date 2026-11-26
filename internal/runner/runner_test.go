package runner

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestQuote(t *testing.T) {
	argv := []string{"sbatch", "--job-name=a b", "--comment=it's", "", "/opt/x-1.0/bin:%j"}
	got := Quote(argv)
	want := `sbatch '--job-name=a b' '--comment=it'\''s' '' /opt/x-1.0/bin:%j`
	if got != want {
		t.Fatalf("Quote() = %s, want %s", got, want)
	}
	// The shell splits the line back into the arguments.
	split, err := exec.Command("sh", "-c", "printargs() { for a in \"$@\"; do printf '[%s]' \"$a\"; done; }; printargs "+strings.TrimPrefix(got, "sbatch ")).Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(split) != "[--job-name=a b][--comment=it's][][/opt/x-1.0/bin:%j]" {
		t.Errorf("the shell split %q into %s", got, split)
	}
}

func TestLocal_Run(t *testing.T) {
	out, err := Local{}.Run(context.Background(), []string{"sh", "-c", "cat; echo err >&2; exit 3"}, []byte("in"))
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Stdout) != "in" || string(out.Stderr) != "err\n" || out.Code != 3 {
		t.Errorf("Run() = %+v", out)
	}

	if _, err := (Local{}).Run(context.Background(), []string{"probe-slurm-no-such-command"}, nil); err == nil {
		t.Error("Run() of a missing command succeeded")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := (Local{}).Run(ctx, []string{"sleep", "5"}, nil); err == nil {
		t.Error("Run() past its deadline succeeded")
	}
}
