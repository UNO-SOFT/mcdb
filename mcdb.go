// Copyright 2021 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

// Package mcdb contains multiple Constant Databases (DJB's cdb),
// as one cdb can only contain 4GiB maximum.
//
// This is a thin wrapper over github.com/colinmarc/cdb.
package mcdb

import (
	"bufio"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
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
	if expC == 32 {
		return 0
	}
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
		if err = os.Chmod(fh.Name(), 0440); err != nil {
			return nil, err
		}
		if m.ws[i], err = cdb.NewWriter(fh, Hash); err != nil {
			_ = m.Close()
			return nil, err
		}
	}
	if err := os.Chmod(dir, 0550); err != nil {
		_ = m.Close()
		return nil, err
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
	m := Reader{expC: 32}
	des, err := os.ReadDir(dir)
	if err != nil && len(des) == 0 {
		// Hack for one-file "multicdb"
		if fh, err := mmap.Open(dir); err == nil {
			if rs, err := cdb.New(fh, Hash); err == nil {
				m.rs = append(m.rs[:0], rs)
				return &m, nil
			}
		}
		return nil, err
	}
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
	if m == nil || len(m.rs) == 0 {
		return nil
	}
	return &Iterator{m: m, it: m.rs[0].Iter()}
}

// Dump all the underlying data in cdbmake format ("+%d,%d:%s->%s\n", len(key), len(value), key, value)
func (m *Reader) Dump(w io.Writer) error {
	bw := bufio.NewWriter(w)
	it := m.Iter()
	for it.Next() {
		if err := dump(bw, it.Key(), it.Value()); err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	return bw.Flush()
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

// Dump the current Iterator position data in cdbmake format ("+%d,%d:%s->%s\n", len(key), len(value), key, value).
func (m *Iterator) Dump(w io.Writer) error {
	if err := m.Err(); err != nil {
		return err
	}
	key, val := m.Key(), m.Value()
	bw := bufio.NewWriterSize(w, len("+65536,65536:->\n")+len(key)+len(val))
	if err := dump(bw, key, val); err != nil {
		return err
	}
	return bw.Flush()
}
func dump(bw *bufio.Writer, key, val []byte) error {
	_, err := fmt.Fprintf(bw, "+%d,%d:", len(key), len(val))
	if err != nil {
		return err
	}
	if _, err = bw.Write(key); err != nil {
		return err
	}
	if _, err = bw.WriteString("->"); err != nil {
		return err
	}
	if _, err = bw.Write(val); err != nil {
		return err
	}
	if err = bw.WriteByte('\n'); err != nil {
		return err
	}
	return nil
}

// Load the Writer from cdbmake format ("+%d,%d:%s->%s\n", len(key), len(value), key, value).
func (m *Writer) Load(r io.Reader) error {
	br := bufio.NewReaderSize(r, 1<<20)
	var key, val []byte
	for {
		var err error
		if key, val, err = load(br, key, val); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if err = m.Put(key, val); err != nil {
			return err
		}
	}
	return nil
}

func load(br *bufio.Reader, key, val []byte) ([]byte, []byte, error) {
	var keyLen, valLen uint32
	_, err := fmt.Fscanf(br, "+%d,%d:", &keyLen, &valLen)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = io.EOF
		}
		return key, val, err
	}
	R := func(key []byte, keyLen int) ([]byte, error) {
		if cap(key) < keyLen {
			key = make([]byte, keyLen)
		} else {
			key = key[:keyLen]
		}
		_, err := io.ReadFull(br, key)
		return key, err
	}
	if key, err = R(key, int(keyLen)); err != nil {
		return key, val, err
	}
	var a [2]byte
	if _, err = io.ReadFull(br, a[:]); err != nil {
		return key, val, err
	} else if !(a[0] == '-' && a[1] == '>') {
		return key, val, fmt.Errorf("wanted ->, got %q", a[:])
	}
	if val, err = R(val, int(valLen)); err != nil {
		return key, val, err
	}
	if b, err := br.ReadByte(); err != nil {
		return key, val, err
	} else if b != '\n' {
		return key, val, fmt.Errorf("wanted \\n, got %q", []byte{b})
	}

	return key, val, nil
}
