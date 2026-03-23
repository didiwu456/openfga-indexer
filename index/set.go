package index

import "sort"

// sortedSet is an ordered, deduplicated list of strings stored as a sorted
// slice. This enables O(m+n) set intersection and O(log n) membership tests.
type sortedSet []string

func newSortedSet(items ...string) sortedSet {
	s := sortedSet(append([]string(nil), items...))
	sort.Strings(s)
	return dedupe(s)
}

func dedupe(s sortedSet) sortedSet {
	if len(s) <= 1 {
		return s
	}
	out := s[:1]
	for _, v := range s[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

// add inserts item maintaining sort order. Returns the new set.
func (s sortedSet) add(item string) sortedSet {
	i := sort.SearchStrings(s, item)
	if i < len(s) && s[i] == item {
		return s // already present
	}
	s = append(s, "")
	copy(s[i+1:], s[i:])
	s[i] = item
	return s
}

// remove deletes item. Returns the new set.
func (s sortedSet) remove(item string) sortedSet {
	i := sort.SearchStrings(s, item)
	if i >= len(s) || s[i] != item {
		return s // not present
	}
	return append(s[:i:i], s[i+1:]...)
}

// contains reports whether item is in the set (O(log n)).
func (s sortedSet) contains(item string) bool {
	i := sort.SearchStrings(s, item)
	return i < len(s) && s[i] == item
}

// intersects returns true if sets a and b share at least one element.
// Both sets must be sorted. Runs in O(m+n).
func intersects(a, b sortedSet) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			return true
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return false
}

// clone returns a copy of s.
func (s sortedSet) clone() sortedSet {
	return append(sortedSet(nil), s...)
}
