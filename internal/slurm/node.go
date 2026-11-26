package slurm

import (
	"encoding/json"
	"fmt"
	"slices"
)

// Node is a node as scontrol show node tells it.
type Node struct {
	Name string
	// State is the base state, such as IDLE, MIXED or DOWN, and Flags those
	// that follow it, such as DRAIN or NOT_RESPONDING.
	State      string
	Flags      []string
	Partitions []string
	Reason     string
	CPUs       int64
	AllocCPUs  int64
}

// Drained tells whether the node takes no new job, being drained or being
// drained once its jobs end.
func (n Node) Drained() bool {
	return n.State == "DRAINED" || n.State == "DRAINING" || slices.Contains(n.Flags, "DRAIN")
}

// Partition is a partition as scontrol show partition tells it, with the
// states of its nodes counted.
type Partition struct {
	Name       string
	State      string
	TotalNodes int64
	Idle       int64
	Mixed      int64
	Allocated  int64
	Down       int64
	Drained    int64
	Other      int64
}

// Count adds the node to the counts of the partition. A node down counts as
// down whether drained or not, and a drained one as drained whatever it runs.
func (p *Partition) Count(n Node) {
	switch {
	case n.State == "DOWN":
		p.Down++
	case n.Drained():
		p.Drained++
	case n.State == "IDLE":
		p.Idle++
	case n.State == "MIXED":
		p.Mixed++
	case n.State == "ALLOCATED":
		p.Allocated++
	default:
		p.Other++
	}
}

type ctldNode struct {
	Name       string       `json:"name"`
	State      states       `json:"state"`
	Partitions stringOrList `json:"partitions"`
	Reason     string       `json:"reason"`
	CPUs       number       `json:"cpus"`
	AllocCPUs  number       `json:"alloc_cpus"`
}

// ParseNodes reads what scontrol show node --json printed.
func ParseNodes(b []byte) ([]Node, error) {
	var out struct {
		reply
		Nodes []ctldNode `json:"nodes"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("failed to read the nodes: %w", err)
	}
	if err := out.err(); err != nil {
		return nil, err
	}
	nodes := make([]Node, 0, len(out.Nodes))
	for _, c := range out.Nodes {
		partitions := []string(c.Partitions)
		if partitions == nil {
			partitions = []string{}
		}
		nodes = append(nodes, Node{
			Name:       c.Name,
			State:      c.State.base(),
			Flags:      c.State.flags(),
			Partitions: partitions,
			Reason:     c.Reason,
			CPUs:       c.CPUs.value(),
			AllocCPUs:  c.AllocCPUs.value(),
		})
	}
	return nodes, nil
}

// ctldPartition is a partition as slurmctld tells it: its state under
// "partition" from v0.0.40 on and at the top before, and its nodes as an
// object from v0.0.40 on and as a string before.
type ctldPartition struct {
	Name      string `json:"name"`
	State     states `json:"state"`
	Partition *struct {
		State states `json:"state"`
	} `json:"partition"`
	Nodes      json.RawMessage `json:"nodes"`
	TotalNodes number          `json:"total_nodes"`
}

// ParsePartitions reads what scontrol show partition --json printed.
func ParsePartitions(b []byte) ([]Partition, error) {
	var out struct {
		reply
		Partitions []ctldPartition `json:"partitions"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("failed to read the partitions: %w", err)
	}
	if err := out.err(); err != nil {
		return nil, err
	}
	partitions := make([]Partition, 0, len(out.Partitions))
	for _, c := range out.Partitions {
		p := Partition{Name: c.Name, State: c.State.base(), TotalNodes: c.TotalNodes.value()}
		if c.Partition != nil && len(c.Partition.State) > 0 {
			p.State = c.Partition.State.base()
		}
		var nodes struct {
			Total number `json:"total"`
		}
		if len(c.Nodes) > 0 && c.Nodes[0] == '{' {
			if err := json.Unmarshal(c.Nodes, &nodes); err != nil {
				return nil, fmt.Errorf("nodes of partition %s: %w", c.Name, err)
			}
			p.TotalNodes = nodes.Total.value()
		}
		partitions = append(partitions, p)
	}
	return partitions, nil
}
