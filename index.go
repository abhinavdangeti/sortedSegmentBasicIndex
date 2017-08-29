package main

import (
	"bytes"
)

// SegmentKeysIndex is a minimal in-memory index (typically for a
// moss segment).
type SegmentKeysIndex struct {
	// Number of keys that can be indexed
	numIndexableKeys int

	// Keys that have been added so far
	numKeys int

	// Size in bytes of all the indexed keys
	numKeyBytes int

	// In-memory byte array of keys
	data []byte

	// Start offsets of keys in the data array
	offsets []uint32

	// Number of keys skipped in between 2 adjacent keys in the data array
	hop int

	// Total number of keys in the source segment
	srcKeyCount int
}

// NewSegmentKeysIndex creates a segmentKindBasicIndex per
// the specifications, and returns it to the caller.
func NewSegmentKeysIndex(quota int, keyCount int,
	keyAvgSize int) *SegmentKeysIndex {

	numIndexableKeys := quota / (keyAvgSize + 4 /* 4 for the offset */)
	hop := keyCount / numIndexableKeys

	data := make([]byte, numIndexableKeys*keyAvgSize)
	offsets := make([]uint32, numIndexableKeys)

	return &SegmentKeysIndex{
		numIndexableKeys: numIndexableKeys,
		numKeys:          0,
		numKeyBytes:      0,
		data:             data,
		offsets:          offsets,
		hop:              hop,
		srcKeyCount:      keyCount,
	}
}

// Add adds a qualified entry to the index. Returns true if space
// still available, false otherwise.
func (s *SegmentKeysIndex) Add(keyIdx int, key []byte) bool {
	if s.numKeys >= s.numIndexableKeys {
		// All keys that can be indexed already have been,
		// return false indicating that there's no room for
		// anymore.
		return false
	}

	if len(key) > (len(s.data) - s.numKeyBytes) {
		// No room for any more keys
		return false
	}

	if keyIdx%(s.hop+1) != 0 {
		// Key does not satisfy the hop condition.
		return true
	}

	s.offsets[s.numKeys] = uint32(s.numKeyBytes)
	copy(s.data[s.numKeyBytes:], key[:])
	s.numKeys++
	s.numKeyBytes += len(key)

	return true
}

// Lookup fetches the range of offsets between which the key
// exists, if present at all. The returned leftPos and rightPos
// can directly be used as the left and right extreme cursors
// while binary searching over the source segment.
func (s *SegmentKeysIndex) Lookup(key []byte) (leftPos int, rightPos int) {
	i, j := 0, s.numKeys

	leftPos = 0
	rightPos = 0

	if i == j {
		// The index wasn't used.
		rightPos = s.srcKeyCount
		return
	}

	// If key smaller than the first key, return early.
	keyStart := s.offsets[0]
	keyEnd := s.offsets[1]
	cmp := bytes.Compare(key, s.data[keyStart:keyEnd])
	if cmp < 0 {
		// ENOENT.
		return
	}

	// If key larger than last key, return early.
	keyStart = s.offsets[s.numKeys-1]
	keyEnd = uint32(s.numKeyBytes)
	cmp = bytes.Compare(s.data[keyStart:keyEnd], key)
	if cmp < 0 {
		leftPos = (s.numKeys - 1) * (s.hop + 1)
		rightPos = s.srcKeyCount
		return
	}

	for i < j {
		h := i + (j-i)/2

		keyStart = s.offsets[h]
		if h < s.numKeys-1 {
			keyEnd = s.offsets[h+1]
		} else {
			keyEnd = uint32(s.numKeyBytes)
		}

		cmp = bytes.Compare(s.data[keyStart:keyEnd], key)
		if cmp == 0 {
			// Direct hit.
			leftPos = h * (s.hop + 1)
			rightPos = leftPos + 1
			return
		} else if cmp < 0 {
			if i == h {
				break
			}
			i = h
		} else {
			if j == h {
				break
			}
			j = h
		}
	}

	leftPos = i * (s.hop + 1)
	rightPos = j * (s.hop + 1)

	return
}
