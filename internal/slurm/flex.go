package slurm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// number is a number as the data_parser of each version of Slurm writes it:
// as itself up to v0.0.39, and as {"set", "infinite", "number"} from
// v0.0.40 on.
type number struct {
	set      bool
	infinite bool
	n        int64
}

func (n *number) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if bytes.Equal(b, []byte("null")) {
		*n = number{}
		return nil
	}
	if len(b) > 0 && b[0] == '{' {
		var o struct {
			Set      *bool       `json:"set"`
			Infinite bool        `json:"infinite"`
			Number   json.Number `json:"number"`
		}
		if err := json.Unmarshal(b, &o); err != nil {
			return err
		}
		v, err := toInt(o.Number)
		if err != nil {
			return err
		}
		*n = number{set: o.Set == nil || *o.Set, infinite: o.Infinite, n: v}
		return nil
	}
	var v json.Number
	if err := json.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("not a number: %s", b)
	}
	i, err := toInt(v)
	if err != nil {
		return err
	}
	*n = number{set: true, n: i}
	return nil
}

func toInt(v json.Number) (int64, error) {
	if v == "" {
		return 0, nil
	}
	if i, err := v.Int64(); err == nil {
		return i, nil
	}
	f, err := v.Float64()
	if err != nil {
		return 0, fmt.Errorf("not a number: %s", v)
	}
	return int64(f), nil
}

// value is the number, or 0 when it is not set or infinite.
func (n number) value() int64 {
	if !n.set || n.infinite {
		return 0
	}
	return n.n
}

// time is the number as a Unix time, or the zero time when it is 0, not
// set, or infinite.
func (n number) time() time.Time {
	if v := n.value(); v > 0 {
		return time.Unix(v, 0).UTC()
	}
	return time.Time{}
}

// states is a state and its flags, as ["RUNNING", "COMPLETING"] from v0.0.40
// on, or as "RUNNING" or "idle+drain" before.
type states []string

func (s *states) UnmarshalJSON(b []byte) error {
	var list []string
	if err := json.Unmarshal(b, &list); err != nil {
		var one string
		if err := json.Unmarshal(b, &one); err != nil {
			return fmt.Errorf("not a state: %s", b)
		}
		list = strings.FieldsFunc(one, func(r rune) bool { return r == '+' || r == ',' })
	}
	out := make(states, 0, len(list))
	for _, v := range list {
		// sinfo marks a node not responding with *, and others with ~ # ! % $ @ ^ -.
		v = strings.ToUpper(strings.TrimRight(strings.TrimSpace(v), "*~#!%$@^-"))
		if v != "" {
			out = append(out, v)
		}
	}
	*s = out
	return nil
}

func (s states) base() string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

func (s states) flags() []string {
	if len(s) < 2 {
		return []string{}
	}
	return s[1:]
}

// stringOrList is a list Slurm writes as a list, or as one string of its
// items separated by commas.
type stringOrList []string

func (s *stringOrList) UnmarshalJSON(b []byte) error {
	var list []string
	if err := json.Unmarshal(b, &list); err == nil {
		*s = list
		return nil
	}
	var one string
	if err := json.Unmarshal(b, &one); err != nil {
		return fmt.Errorf("not a list: %s", b)
	}
	*s = strings.FieldsFunc(one, func(r rune) bool { return r == ',' })
	return nil
}

// exitCode is how a job exited: a number up to v0.0.38, and from v0.0.39 on
// {"status", "return_code", "signal": {"id" or "signal_id", "name"}}.
type exitCode struct {
	code   int64
	signal int64
}

func (e *exitCode) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if bytes.Equal(b, []byte("null")) {
		*e = exitCode{}
		return nil
	}
	if len(b) > 0 && b[0] == '{' {
		var o struct {
			ReturnCode number `json:"return_code"`
			Signal     struct {
				ID       number `json:"id"`
				SignalID number `json:"signal_id"`
			} `json:"signal"`
		}
		if err := json.Unmarshal(b, &o); err != nil {
			return err
		}
		sig := o.Signal.ID.value()
		if sig == 0 {
			sig = o.Signal.SignalID.value()
		}
		*e = exitCode{code: o.ReturnCode.value(), signal: sig}
		return nil
	}
	var n number
	if err := n.UnmarshalJSON(b); err != nil {
		return err
	}
	*e = exitCode{code: n.value()}
	return nil
}

// comment is the comment of a job: a string from slurmctld, and
// {"job", "administrator", "system"} from slurmdbd.
type comment string

func (c *comment) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*c = comment(s)
		return nil
	}
	var o struct {
		Job string `json:"job"`
	}
	if err := json.Unmarshal(b, &o); err != nil {
		return fmt.Errorf("not a comment: %s", b)
	}
	*c = comment(o.Job)
	return nil
}

// reply is what every command answers with --json, and slurmrestd with
// each response, besides its data: the errors that made it fail.
type reply struct {
	Errors []replyError `json:"errors"`
}

type replyError struct {
	Description string `json:"description"`
	Error       string `json:"error"`
	ErrorNumber int    `json:"error_number"`
}

func (e replyError) message() string {
	switch {
	case e.Description == "":
		return e.Error
	case e.Error == "":
		return e.Description
	}
	return e.Description + ": " + e.Error
}

func (r reply) err() error {
	var msgs []string
	for _, e := range r.Errors {
		msgs = append(msgs, e.message())
	}
	if len(msgs) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(msgs, "; "))
}
