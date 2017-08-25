package main

import (
	"bytes"
)

// SegmentKindBasicIndex is a minimal in-memory index (typically for a
// moss segment).
type SegmentKindBasicIndex struct {
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
}

// NewSegmentKindBasicIndex creates a segmentKindBasicIndex per
// the specifications, and returns it to the caller.
func NewSegmentKindBasicIndex(quota int, keyCount int,
	keyAvgSize int) *SegmentKindBasicIndex {

	indexKeyCount := quota / (keyAvgSize + 4 /* 4 for the offset */)
	hop := keyCount / indexKeyCount

	data := make([]byte, indexKeyCount*keyAvgSize)
	offsets := make([]uint32, indexKeyCount)

	return &SegmentKindBasicIndex{
		numIndexableKeys: indexKeyCount,
		numKeys:          0,
		numKeyBytes:      0,
		data:             data,
		offsets:          offsets,
		hop:              hop,
	}
}

// Add adds a qualified entry to the index. Returns true if space
// still available, false otherwise.
func (s *SegmentKindBasicIndex) Add(keyIdx int, key []byte) bool {
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

// Lookup fetches the range of offsets between which the key exists,
// if present at all.
// - If in case of direct hit: found: true, leftPos: position
// - If key is before the first entry: ENOENT, leftPos = rightPos = 0
// - rightPos will need to be updated to the segment length in the
//   following cases:
//     * If index doesn't contain any keys: rightPos = -1
//     * If key lies after the last key in the index: rightPos = -1
// - In all other cases: found: false, valid leftPos & rightPos
func (s *SegmentKindBasicIndex) Lookup(key []byte) (found bool,
	leftPos int, rightPos int) {

	i, j := 0, s.numKeys

	found = false
	leftPos = 0
	rightPos = 0

	if i == j {
		// The index wasn't used
		rightPos = -1
		return
	}

	// If key smaller than the first key, return early.
	keyStart := s.offsets[0]
	keyEnd := s.offsets[1]
	cmp := bytes.Compare(key, s.data[keyStart:keyEnd])
	if cmp < 0 {
		// ENOENT
		return
	}

	// If key larger than last key, return early.
	keyStart = s.offsets[s.numKeys-1]
	keyEnd = uint32(s.numKeyBytes)
	cmp = bytes.Compare(s.data[keyStart:keyEnd], key)
	if cmp < 0 {
		leftPos = (s.numKeys - 1) * (s.hop + 1)
		rightPos = -1
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
			// Direct hit
			found = true
			leftPos = h * (s.hop + 1)
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
