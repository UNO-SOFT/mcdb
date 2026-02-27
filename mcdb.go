// Copyright 2021, 2022 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

// Package mcdb contains multiple Constant Databases (DJB's cdb),
// as one cdb can only contain 4GiB maximum.
//
// This is a thin wrapper over github.com/colinmarc/cdb.
package mcdb

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	//"log"
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

type HashFunc func([]byte) uint32

var fnvHashesPool = sync.Pool{New: func() interface{} { return fnv.New32() }}

func fnvHash(p []byte) uint32 {
	hsh := fnvHashesPool.Get().(hash.Hash32)
	_, _ = hsh.Write(p)
	res := hsh.Sum32()
	hsh.Reset()
	fnvHashesPool.Put(hsh)
	return res
}

// bucket returns the specific bucket a key must reside in.
func bucket(h HashFunc, key []byte, expC int) int {
	//log.Printf("bucket(%p, %s, %d): %d", h, key, expC, h(key)>>expC)
	if expC == 32 {
		return 0
	}
	return int(h(key) >> expC)
}

// Writer is the writer. It needs the number of tables beforehand.
type Writer struct {
	Config
	ws         []cdbWriter
	path       string
	bucketHash HashFunc
	expC       int
	canGrow    bool
}

type cdbWriter struct {
	*cdb.Writer
	fileName string
}

// NewWriter returns a new Writer.
//
// The next power-of-two number of tables are created, so for example
// with n=3, 4 tables are created, containing maximum 16GiB of data.
//
// If n<=0 then automatic growing is enabled, with the starting number of tables being abs(n)
func NewWriter(dir string, n int) (*Writer, error) {
	canGrow := n <= 0
	if n == 0 {
		n = 2
	} else if n < 0 {
		n = -n
	}
	var n2 int
	expC := 32
	for n2 = 1; n2 < n; n2 <<= 1 {
		expC--
	}
	m := Writer{
		expC: expC, canGrow: canGrow, path: dir,
		bucketHash: fnvHash,
		ws:         make([]cdbWriter, n2),
	}
	//log.Println("n:", n, "expC:", expC)

	pat := FileName
	if i := strings.Index(pat, ",%b."); i >= 0 {
		pat = pat[:i+2] + "0" + strconv.Itoa(32-m.expC+1) + pat[i+2:]
	}
	base := filepath.Join(dir, pat)
	_ = os.MkdirAll(dir, 0750)
	_ = os.Chmod(dir, 0750)
	for i := range m.ws {
		fh, err := os.Create(fmt.Sprintf(base, DefaultVersion, n2, i))
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		w, err := cdb.NewWriter(fh, nil)
		if err != nil {
			_ = fh.Close()
			_ = m.Close()
			return nil, err
		}
		m.ws[i] = cdbWriter{Writer: w, fileName: fh.Name()}
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
		if w.Writer != nil {
			_ = w.Close()
		}
	}
	if fi, err := os.Stat(m.path); err != nil {
		return err
	} else if fi.Mode().IsDir() {
		des, _ := os.ReadDir(m.path)
		for _, de := range des {
			nm := de.Name()
			if strings.HasPrefix(nm, "mcdb-") && strings.HasSuffix(nm, ".cdb") {
				_ = os.Chmod(filepath.Join(m.path, nm), 0440)
			}
		}
		_ = os.Chmod(m.path, 0550)
	} else {
		_ = os.Chmod(m.path, 0440)
	}
	return nil
}

// Put the key into one of the underlying writers.
//
// If the amount of data written would exceed the limit, and the auto-growing is disabled,
// then Put returns ErrTooMuchData.
//
// When growing, the number of tables doubles, and all data is copied.
func (m *Writer) Put(key, val []byte) error {
	err := m.ws[bucket(m.bucketHash, key, m.expC)].Put(key, val)
	if err == nil || !m.canGrow || !errors.Is(err, cdb.ErrTooMuchData) {
		return err
	}

	// Grow by copying the old to 2x tables.
	for _, w := range m.ws {
		if err := w.Close(); err != nil {
			return err
		}
	}
	r, err := NewReader(m.path)
	if err != nil {
		return fmt.Errorf("read %q: %w", m.path, err)
	}
	defer r.Close()
	m2, err := NewWriter(m.path, 2*len(m.ws))
	if err != nil {
		return fmt.Errorf("create (%d) %q: %w", 2*len(m.ws), m.path, err)
	}
	m2.canGrow = true
	m2.Config = m.Config

	if err := m2.Put(key, val); err != nil {
		return fmt.Errorf("put %q: %w", key, err)
	}
	it := r.Iter()
	for it.Next() {
		if err := m2.Put(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	if err = it.Err(); err != nil {
		return fmt.Errorf("iterate: %w", err)
	}
	for _, w := range m.ws {
		_ = os.Remove(w.fileName)
	}
	m.Close()
	*m = *m2
	return nil
}

// Reader is a reader for multiple CDB files.
type Reader struct {
	Config
	rs                  []*cdb.CDB
	bucketHash, cdbHash HashFunc
	expC                int
}

type Version uint8

func (v Version) String() string { return fmt.Sprintf("v%d", v) }

const DefaultVersion = Version(1)

// NewReader opens the multiple CDB files for reading.
func NewReader(dir string) (*Reader, error) {
	m := Reader{expC: 32}
	des, err := os.ReadDir(dir)
	if err != nil && len(des) == 0 {
		// Hack for one-file "multicdb"
		if fh, err := mmap.Open(dir); err == nil {
			if rs, err := cdb.New(fh, nil); err == nil {
				m.rs = append(m.rs[:0], rs)
				return &m, nil
			}
		}
		return nil, err
	}
	var version Version
	for _, de := range des {
		nm := de.Name()
		if !(strings.HasPrefix(nm, "mcdb-") && strings.HasSuffix(nm, ".cdb")) {
			continue
		}
		var u1, u2 uint32
		var v Version
		var err error
		if strings.HasPrefix(nm, "mcdb-v") {
			_, err = fmt.Sscanf(de.Name(), FileName, &v, &u1, &u2)
		} else {
			_, err = fmt.Sscanf(de.Name(), FileNameV0, &u1, &u2)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", de.Name(), err)
		}
		if m.rs == nil {
			m.rs = make([]*cdb.CDB, int(u1))
			for i := 1; i < len(m.rs); i <<= 1 {
				m.expC--
			}
			version = v
			switch version {
			case 0:
				m.bucketHash, m.cdbHash = fnvHash, fnvHash
			case 1:
				m.bucketHash, m.cdbHash = fnvHash, nil
			default:
				return nil, fmt.Errorf("unknown version %d", version)
			}
		} else if version != v {
			return nil, fmt.Errorf("Version mismatch: was %d, now %d (%q)", version, v, de.Name())
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
		if m.rs[int(u2)], err = cdb.New(fh, m.cdbHash); err != nil {
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
			return nil, fmt.Errorf(FileName+" not found", version, len(m.rs), i)
		}
	}
	//log.Println("rs:", len(m.rs), "expC:", m.expC, "version:", version, "bucketHash:", m.bucketHash, "cdbHash:", m.cdbHash)
	return &m, nil
}

const (
	FileName   = "mcdb-v%d-%d,%b.cdb"
	FileNameV0 = "mcdb-%d,%b.cdb"
)

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

// Get returns the value for a given key, or nil if it can't be found.
func (m *Reader) Get(key []byte) ([]byte, error) {
	j := bucket(m.bucketHash, key, m.expC)
	b, err := m.rs[j].Get(key)
	if err == nil || !m.TryAll {
		return b, err
	}
	for i, r := range m.rs {
		if i == j {
			continue
		}
		if b, err2 := r.Get(key); err2 == nil {
			return b, nil
		}
	}
	return b, err
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
	return m.DumpContext(context.Background(), w)
}

// DumpContext dumps all the underlying data in cdbmake format ("+%d,%d:%s->%s\n", len(key), len(value), key, value)
func (m *Reader) DumpContext(ctx context.Context, w io.Writer) error {
	bw := bufio.NewWriter(w)
	defer func() { _ = bw.Flush() }()
	dump := m.Config.dumpFunc()
	it := m.Iter()
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := dump(bw, it); err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	return bw.Flush()
}

type kver interface {
	Key() []byte
	Value() []byte
}

func (cfg Config) dumpFunc() func(bw *bufio.Writer, kv kver) error {
	if cfg.Simple {
		if cfg.OnlyKeys {
			return func(bw *bufio.Writer, kv kver) error {
				_, _ = bw.Write(kv.Key())
				return bw.WriteByte('\n')
			}
		}
		return func(bw *bufio.Writer, kv kver) error {
			_, _ = bw.Write(kv.Key())
			_ = bw.WriteByte('\t')
			_, _ = bw.Write(kv.Value())
			return bw.WriteByte('\n')
		}
	}
	if cfg.OnlyKeys {
		return func(bw *bufio.Writer, kv kver) error {
			return DumpKey(bw, kv.Key())
		}
	}
	return func(bw *bufio.Writer, kv kver) error {
		return Dump(bw, kv.Key(), kv.Value())
	}
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
//
// It reads the next key/value pair and advances the iterator one record.
// It returns false when the scan stops, either by reaching the end of the database or an error.
// After Next returns false, the Err method will return any error that occurred while iterating.
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
	dump := m.m.dumpFunc()
	if err := dump(bw, m); err != nil {
		return err
	}
	return bw.Flush()
}

// DumpKey the given data key in cdbmake format ("+%d:%s\n", len(key), key)
func DumpKey(bw *bufio.Writer, key []byte) error {
	_, err := fmt.Fprintf(bw, "+%d:", len(key))
	if err != nil {
		return err
	}
	if _, err = bw.Write(key); err != nil {
		return err
	}
	return bw.WriteByte('\n')
}

// Dump the given data key, value pair in cdbmake format ("+%d,%d:%s->%s\n", len(key), len(value), key, value)
func Dump(bw *bufio.Writer, key, val []byte) error {
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
	return bw.WriteByte('\n')
}

// Load the Writer from cdbmake format ("+%d,%d:%s->%s\n", len(key), len(value), key, value).
func (m *Writer) Load(r io.Reader) error {
	return m.LoadContext(context.Background(), r)
}

// LoadContext the Writer from cdbmake format ("+%d,%d:%s->%s\n", len(key), len(value), key, value).
func (m *Writer) LoadContext(ctx context.Context, r io.Reader) error {
	br := bufio.NewReaderSize(r, 1<<20)
	parse := m.Config.parseFunc()
	var key, val []byte
	for {
		err := ctx.Err()
		if err != nil {
			return err
		}
		if key, val, err = parse(br, key, val); err != nil {
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

func (cfg Config) parseFunc() func(br *bufio.Reader, key, val []byte) ([]byte, []byte, error) {
	if cfg.Simple {
		if cfg.OnlyKeys {
			return func(br *bufio.Reader, key, val []byte) ([]byte, []byte, error) {
				line, err := br.ReadBytes('\n')
				if len(line) != 0 {
					line = line[:len(line)-1]
				}
				return append(key[:0], line...), val[:0], err
			}
		}
		return func(br *bufio.Reader, key, val []byte) ([]byte, []byte, error) {
			line, err := br.ReadBytes('\n')
			if len(line) != 0 {
				line = line[:len(line)-1]
			}
			i := bytes.IndexAny(line, "\t ")
			if i < 0 {
				return append(key[:0], line...), val[:0], err
			}
			return append(key[:0], line[:i]...), append(val[:0], line[i+1:]...), err
		}
	}
	return Parse
}

// Parse one key,value pair from the bufio.Reader of cdbmake format ("+%d,%d:%s->%s\n", len(key), len(value), key, value).
func Parse(br *bufio.Reader, key, val []byte) ([]byte, []byte, error) {
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

type Config struct {
	OnlyKeys, Simple, TryAll bool
}
