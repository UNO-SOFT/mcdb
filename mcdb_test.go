// Copyright 2021 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package mcdb_test

import (
	"bytes"
	"os"
	"os/exec"
	"testing"

	"github.com/UNO-SOFT/mcdb"
)

func TestReadWrite(t *testing.T) {
	dn, err := os.MkdirTemp("", "mcdb-test.cdb")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(dn)
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
	t.Logf("Put %d keys into %q", len(keys), dn)
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
	t.Logf("Got %d keys from %q", len(keys), dn)

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
	t.Logf("Iterated through %d keys", len(keys))

	for i, k := range keys {
		if k != nil {
			t.Errorf("%d left out from iteration", i)
		}
	}
	t.Logf("Checked %d keys", len(keys))

	var buf bytes.Buffer
	if err = rw.Dump(&buf); err != nil {
		t.Error(err)
	}
	t.Logf("Dumped %d bytes", buf.Len())

	if err = rw.Close(); err != nil {
		t.Error(err)
	}

	removeAll(dn)
	if cw, err = mcdb.NewWriter(dn, 2); err != nil {
		t.Fatal(err)
	}
	defer cw.Close()

	if err = cw.Load(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	t.Logf("Loaded %d bytes", buf.Len())
	if err = cw.Close(); err != nil {
		t.Fatal(err)
	}
	if rw, err = mcdb.NewReader(dn); err != nil {
		t.Fatal(err)
	}
	defer rw.Close()
	it = rw.Iter()
	var n int
	for it.Next() {
		if err = it.Err(); err != nil {
			t.Fatal(err)
		}
		n++
	}
	t.Logf("Iterated %d keys", n)
	if n != len(keys) {
		t.Errorf("got %d keys after dump-load, instead of %d", n, len(keys))
	}
}

func removeAll(dir string) {
	_ = exec.Command("chmod", "u+w", "-R", dir).Run()
	_ = os.RemoveAll(dir)
}

func BenchmarkGet(t *testing.B) {
	dn, err := os.MkdirTemp("", "mcdb-test.cdb")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(dn)

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
	t.Logf("Put %d keys into %q", len(keys), dn)
	if err = cw.Close(); err != nil {
		t.Error(err)
	}

	rw, err := mcdb.NewReader(dn)
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Close()

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		_, _ = rw.Get([]byte{0})
		t.SetBytes(1)
	}
}
