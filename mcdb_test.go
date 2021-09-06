// Copyright 2021 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package mcdb_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/UNO-SOFT/mcdb"
)

func TestReadWrite(t *testing.T) {
	dn, err := os.MkdirTemp("", "mcdb-test.cdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dn)
	cw, err := mcdb.NewWriter(dn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer cw.Close()

	keys := make([][]byte, 256)
	for i := range keys {
		keys[i] = []byte{byte(i)}
		if err = cw.Put(keys[i], keys[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err = cw.Close(); err != nil {
		t.Error(err)
	}

	rw, err := mcdb.NewReader(dn)
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Close()
	for _, want := range keys {
		got, err := rw.Get(want)
		if err != nil {
			t.Errorf("%q: %w", want, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got %q, wanted %q", got, want)
		}
	}

	it := rw.Iter()
	for it.Next() {
		if err = it.Err(); err != nil {
			t.Fatal(err)
		}
		key, val := it.Key(), it.Value()
		if len(key) != 1 {
			t.Errorf("key=%q too long", key)
		}
		if !bytes.Equal(key, val) {
			t.Errorf("key=%q val=%q", key, val)
		}
		keys[int(key[0])] = nil
	}
	if err = it.Err(); err != nil {
		t.Error(err)
	}

	for i, k := range keys {
		if k != nil {
			t.Errorf("%d left out from iteration", i)
		}
	}

	if err = rw.Dump(os.Stdout); err != nil {
		t.Error(err)
	}

	if err = rw.Close(); err != nil {
		t.Error(err)
	}
}
