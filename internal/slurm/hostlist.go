package slurm

import (
	"fmt"
	"strconv"
	"strings"
)

// MaxHosts bounds how many names a host list expands to.
const MaxHosts = 100000

// ExpandHostlist expands a host list as Slurm writes one, such as
// c[1-4,7],gpu[01-02],login, into its names, keeping the zeros a range is
// padded with.
func ExpandHostlist(list string) ([]string, error) {
	var names []string
	for _, item := range splitTop(list) {
		if item == "" {
			continue
		}
		expanded, err := expandItem(item)
		if err != nil {
			return nil, err
		}
		names = append(names, expanded...)
		if len(names) > MaxHosts {
			return nil, fmt.Errorf("host list %s names more than %d hosts", list, MaxHosts)
		}
	}
	return names, nil
}

// splitTop splits the list at the commas outside brackets.
func splitTop(list string) []string {
	var items []string
	depth, start := 0, 0
	for i, r := range list {
		switch r {
		case '[':
			depth++
		case ']':
			depth--
		case ',':
			if depth == 0 {
				items = append(items, strings.TrimSpace(list[start:i]))
				start = i + 1
			}
		}
	}
	return append(items, strings.TrimSpace(list[start:]))
}

func expandItem(item string) ([]string, error) {
	open := strings.IndexByte(item, '[')
	if open < 0 {
		if strings.ContainsRune(item, ']') {
			return nil, fmt.Errorf("host list item %s has ] without [", item)
		}
		return []string{item}, nil
	}
	end := strings.IndexByte(item[open:], ']')
	if end < 0 {
		return nil, fmt.Errorf("host list item %s has [ without ]", item)
	}
	end += open
	prefix, ranges, rest := item[:open], item[open+1:end], item[end+1:]
	tails, err := expandItem(rest)
	if err != nil {
		return nil, err
	}
	var names []string
	for r := range strings.SplitSeq(ranges, ",") {
		lo, hi, found := strings.Cut(r, "-")
		if !found {
			hi = lo
		}
		from, err1 := strconv.Atoi(lo)
		to, err2 := strconv.Atoi(hi)
		if err1 != nil || err2 != nil || from > to || from < 0 {
			return nil, fmt.Errorf("host list item %s has a range %s that is not one", item, r)
		}
		if to-from >= MaxHosts {
			return nil, fmt.Errorf("host list item %s names more than %d hosts", item, MaxHosts)
		}
		width := 0
		if len(lo) > 1 && lo[0] == '0' {
			width = len(lo)
		}
		for n := from; n <= to; n++ {
			for _, tail := range tails {
				names = append(names, fmt.Sprintf("%s%0*d%s", prefix, width, n, tail))
			}
			if len(names) > MaxHosts {
				return nil, fmt.Errorf("host list item %s names more than %d hosts", item, MaxHosts)
			}
		}
	}
	return names, nil
}
