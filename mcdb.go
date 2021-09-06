// Copyright 2021 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

// Package mcdb contains multiple Constant Databases (DJB's cdb),
// as one cdb can only contain 4GiB maximum.
//
// This is a thin wrapper over github.com/colinmarc/cdb.
package mcdb

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/colinmarc/cdb"
	"golang.org/x/exp/mmap"
)

var hashesPool = sync.Pool{New: func() interface{} { return fnv.New32() }}

// Hash is the used hash. FNV-1 at the moment.
func Hash(p []byte) uint32 {
	hsh := hashesPool.Get().(hash.Hash32)
	_, _ = hsh.Write(p)
	res := hsh.Sum32()
	hsh.Reset()
	hashesPool.Put(hsh)
	return res
}

// bucket returns the specific bucket a key must reside in.
func bucket(key []byte, expC int) int {
	return int(Hash(key) >> expC)
}

// Writer is the writer. It needs the number of tables beforehand.
type Writer struct {
	ws   []*cdb.Writer
	expC int
}

// NewWriter returns a new Writer.
//
// The next power-of-two number of tables are created, so for example
// with n=3, 4 tables are created, containing maximum 16GiB of data.
func NewWriter(dir string, n int) (*Writer, error) {
	var n2 int
	expC := 32
	for n2 = 1; n2 < n; n2 <<= 1 {
		expC--
	}
	m := Writer{expC: expC, ws: make([]*cdb.Writer, n2)}
	//log.Println("n:", n, "expC:", expC)
	base := filepath.Join(dir, FileName)
	_ = os.MkdirAll(dir, 0750)
	for i := range m.ws {
		fh, err := os.Create(fmt.Sprintf(base, n2, i))
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		if m.ws[i], err = cdb.NewWriter(fh, Hash); err != nil {
			_ = m.Close()
			return nil, err
		}
	}
	return &m, nil
}

// Close the underlying writers.
func (m *Writer) Close() error {
	if m == nil || len(m.ws) == 0 {
		return nil
	}
	ws := m.ws
	m.ws = nil
	for _, w := range ws {
		if w != nil {
			_ = w.Close()
		}
	}
	return nil
}

// Put the key into one of the underlying writers.
func (m *Writer) Put(key, val []byte) error {
	return m.ws[bucket(key, m.expC)].Put(key, val)
}

// Reader is a reader for multiple CDB files.
type Reader struct {
	rs   []*cdb.CDB
	expC int
}

// NewReader opens the multiple CDB files for reading.
func NewReader(dir string) (*Reader, error) {
	des, err := os.ReadDir(dir)
	if err != nil && len(des) == 0 {
		return nil, err
	}
	m := Reader{expC: 32}
	for _, de := range des {
		nm := de.Name()
		if !(strings.HasPrefix(nm, "mcdb-") && strings.HasSuffix(nm, ".cdb")) {
			continue
		}
		var u1, u2 uint32
		if _, err := fmt.Sscanf(de.Name(), FileName, &u1, &u2); err != nil {
			return nil, fmt.Errorf("%s: %w", de.Name(), err)
		}
		if m.rs == nil {
			m.rs = make([]*cdb.CDB, int(u1))
			for i := 1; i < len(m.rs); i <<= 1 {
				m.expC--
			}
		}
		if u1 != uint32(len(m.rs)) {
			_ = m.Close()
			return nil, fmt.Errorf("%s: first number should be the same for all files", de.Name())
		}
		if u1 < u2 {
			_ = m.Close()
			return nil, fmt.Errorf("%s: second number should not be bigger than the second", de.Name())
		}
		fh, err := mmap.Open(filepath.Join(dir, de.Name()))
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		if m.rs[int(u2)], err = cdb.New(fh, Hash); err != nil {
			_ = m.Close()
			return nil, err
		}
	}
	if len(m.rs) == 0 {
		return nil, errors.New("no " + FileName + " files found")
	}
	for i, r := range m.rs {
		if r == nil {
			_ = m.Close()
			return nil, fmt.Errorf(FileName+" not found", len(m.rs), i)
		}
	}
	//log.Println("rs:", len(m.rs), "expC:", m.expC)
	return &m, nil
}

const FileName = "mcdb-%d,%b.cdb"

// Close the underlying readers.
func (m *Reader) Close() error {
	rs := m.rs
	m.rs = nil
	if rs == nil {
		return nil
	}
	for _, r := range rs {
		if r != nil {
			_ = r.Close()
		}
	}
	return nil
}

// Get the key from the reader.
func (m *Reader) Get(key []byte) ([]byte, error) {
	return m.rs[bucket(key, m.expC)].Get(key)
}

// Iter returns an iterator.
func (m *Reader) Iter() *Iterator {
	return &Iterator{m: m, it: m.rs[0].Iter()}
}

// Iterator iterates through all keys of all CDB files.
type Iterator struct {
	m  *Reader
	it *cdb.Iterator
	i  int
}

// Err returns the last error.
func (m *Iterator) Err() error { return m.it.Err() }

// Key returns the current key (after Next).
func (m *Iterator) Key() []byte { return m.it.Key() }

// Value returns the current value (after Next).
func (m *Iterator) Value() []byte { return m.it.Value() }

// Next advances the iterator, if possible.
func (m *Iterator) Next() bool {
	if m.it.Next() {
		return true
	}
	for m.i < len(m.m.rs)-1 {
		m.i++
		m.it = m.m.rs[m.i].Iter()
		if m.it.Next() {
			return true
		}
	}
	return false
}
