package main

import (
	"bytes"
	"testing"
)

func TestSegmentKindBasicIndex(t *testing.T) {
	// Consider a moss segment with this data:
	// Buf: |key1|val1|key10|val10|key100|val100|key1000|val1000
	//      |key250|val250|key4000|val4000|key500|val500
	// Kvs: |4|4|0    |5|5|8      |6|6|18       |7|7|30
	//      |7|7|44       |7|7|56         |6|6|70

	keys := []string{
		"key1",
		"key10",
		"key100",
		"key1000",
		"key250",
		"key4000",
		"key500",
	}

	s := newSegmentKindBasicIndex(
		30, // quota
		7,  // number of keys
		6,  // average key size
	)

	for i, k := range keys {
		ret := s.Add(i, []byte(k))
		if !ret {
			t.Errorf("Unexpected Add failure!")
		}
	}

	ret := s.Add(0, []byte("key"))
	if ret {
		t.Errorf("Space shouldn't have been available!")
	}

	if s.numIndexableKeys != 3 {
		t.Errorf("Unexpected number of keys (%v) indexed!", s.numIndexableKeys)
	}

	data := []byte("key1key1000key500")

	if !bytes.Contains(s.data, data) {
		t.Errorf("Unexpected data in index array: %v", string(s.data))
	}

	if len(s.offsets) != 3 ||
		s.offsets[0] != 0 || s.offsets[1] != 4 || s.offsets[2] != 11 {
		t.Errorf("Unexpected content in offsets array!")
	}

	if s.hop != 2 {
		t.Errorf("Unexpected hop: %v!", s.hop)
	}

	var found bool
	var left, right int

	found, left, _ = s.Lookup([]byte("key1000"))
	if !found || left != 3 {
		t.Errorf("Unexpected results for key1000")
	}

	found, left, _ = s.Lookup([]byte("key1"))
	if !found || left != 0 {
		t.Errorf("Unexpected results for key1")
	}

	found, left, _ = s.Lookup([]byte("key500"))
	if !found || left != 6 {
		t.Errorf("Unexpected results for key500")
	}

	found, left, right = s.Lookup([]byte("key400"))
	if found || left != 3 || right != 6 {
		t.Errorf("Unexpected results for key4000")
	}

	found, left, right = s.Lookup([]byte("key100"))
	if found || left != 0 || right != 3 {
		t.Errorf("Unexpected results for key100")
	}

	found, left, right = s.Lookup([]byte("key0"))
	if found || left != 0 || right != 0 {
		t.Errorf("Unexpected results for key0")
	}

	found, left, right = s.Lookup([]byte("key6"))
	if found || left != 6 || right != -1 {
		t.Errorf("Unexpected results for key6")
	}
}
