//  Copyright (c) 2017 Couchbase, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"bytes"
)

type SegmentKindBasicIndex struct {
	// Number of keys that can be indexed
	numIndexableKeys int

	// Size in bytes of all the indexed keys
	numKeyBytes int

	// In-memory byte array of keys
	data []byte

	// Start offsets of keys in the data array
	offsets []uint32

	// Number of keys skipped in between 2 adjacent keys in the data array
	hop int
}

func NewSegmentKindBasicIndex(quota int, keyCount int,
	keyAvgSize int) *SegmentKindBasicIndex {

	indexKeyCount := quota / (keyAvgSize + 4 /* 4 for the offset */)
	hop := keyCount / indexKeyCount

	data := make([]byte, indexKeyCount*keyAvgSize)

	return &SegmentKindBasicIndex{
		numIndexableKeys: indexKeyCount,
		numKeyBytes:      0,
		data:             data,
		offsets:          []uint32{},
		hop:              hop,
	}
}

// Returns true if space still available, false otherwise
func (s *SegmentKindBasicIndex) Add(keyIdx int, key []byte) bool {
	if keyIdx%(s.hop+1) != 0 {
		// Key does not satisfy the hop condition.
		if len(s.offsets) >= s.numIndexableKeys {
			// All keys that can be indexed already have been,
			// return false indicating that there's no room for
			// anymore.
			return false
		}
		return true
	}

	if len(key) > (len(s.data) - s.numKeyBytes) {
		// No room for any more keys
		return false
	}

	s.offsets = append(s.offsets, uint32(s.numKeyBytes))
	copy(s.data[s.numKeyBytes:], key[:])
	s.numKeyBytes += len(key)

	if len(s.offsets) == s.numIndexableKeys {
		// All keys that are to be indexed have already been
		// indexed.
		return false
	}

	return true
}

func (s *SegmentKindBasicIndex) Lookup(key []byte) (found bool,
	leftPos int, rightPos int) {

	// Get the starting offsets of the first and last key
	// in the data array
	i, j := 0, len(s.offsets)

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
	keyStart = s.offsets[len(s.offsets)-1]
	keyEnd = uint32(s.numKeyBytes)
	cmp = bytes.Compare(s.data[keyStart:keyEnd], key)
	if cmp < 0 {
		leftPos = (len(s.offsets) - 1) * (s.hop + 1) // TODO: * 2
		rightPos = -1
		return
	}

	for i < j {
		h := i + (j-i)/2

		keyStart = s.offsets[h]
		if h < len(s.offsets)-1 {
			keyEnd = s.offsets[h+1]
		} else {
			keyEnd = uint32(s.numKeyBytes)
		}

		cmp = bytes.Compare(s.data[keyStart:keyEnd], key)
		if cmp == 0 {
			// Direct hit
			found = true
			leftPos = h * (s.hop + 1) // TODO: * 2
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

	leftPos = i * (s.hop + 1)  // TODO: * 2
	rightPos = j * (s.hop + 1) // TODO: * 2

	return
}
