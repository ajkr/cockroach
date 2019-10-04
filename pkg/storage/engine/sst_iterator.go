// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pkg/errors"
)

var readerOpts = &sstable.Options{
	Comparer: MVCCComparer{},
}

type sstIterator struct {
	sst  *sstable.Reader
	iter sstable.Iterator

	mvccKey MVCCKey
	value   []byte
	valid   bool
	err     error

	// For allocation avoidance in NextKey.
	nextKeyStart []byte

	// roachpb.Verify k/v pairs on each call to Next()
	verify bool
}

// NewSSTIterator returns a `SimpleIterator` for an in-memory sstable.
// It's compatible with sstables written by `RocksDBSstFileWriter` and
// Pebble's `sstable.Writer`, and assumes the keys use Cockroach's MVCC
// format.
func NewSSTIterator(path string) (SimpleIterator, error) {
	file, err := db.DefaultFileSystem.Open(path)
	if err != nil {
		return nil, err
	}
	return iter, err
}

type memFile struct {
	*bytes.Reader
}

func newMemFile(content []byte) *memFile {
	return &memFile{Reader: bytes.NewReader(content)}
}

// Close implements the vfs.File interface
func (*memFile) Close() error {
	return nil
}

// Write implements the vfs.File interface
func (*memFile) Write(p []byte) (n int, err error) {
	panic("Write unsupported")
}

// Stat implements the vfs.File interface
func (f *memFile) Stat() (os.FileInfo, error) {
	return f.Size(), nil
}

// Sync implements the vfs.File interface
func (*memFile) Sync() error {
	return nil
}

// NewMemSSTIterator returns a `SimpleIterator` for an in-memory sstable.
// It's compatible with sstables written by `RocksDBSstFileWriter` and
// Pebble's `sstable.Writer`, and assumes the keys use Cockroach's MVCC
// format.
func NewMemSSTIterator(data []byte, verify bool) (SimpleIterator, error) {
	return &sstIterator{sst: table.NewReader(newMemFile(data), readerOpts), verify: verify}, nil
}

// Close implements the SimpleIterator interface.
func (r *sstIterator) Close() {
	if r.iter != nil {
		r.err = errors.Wrap(r.iter.Close(), "closing sstable iterator")
	}
	if err := r.sst.Close(); err != nil && r.err == nil {
		r.err = errors.Wrap(err, "closing sstable")
	}
}

// encodeInternalSeekKey encodes an engine.MVCCKey into the RocksDB
// representation and adds padding to the end such that it compares correctly
// with rocksdb "internal" keys which have an 8b suffix, which appear in SSTs
// created by rocks when read directly with a reader like LevelDB's Reader.
func encodeInternalSeekKey(key MVCCKey) []byte {
	return append(EncodeKey(key), []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
}

// Seek implements the SimpleIterator interface.
func (r *sstIterator) Seek(key MVCCKey) {
	if r.iter != nil {
		if r.err = errors.Wrap(r.iter.Close(), "resetting sstable iterator"); r.err != nil {
			return
		}
	}
	r.iter = r.sst.Find(encodeInternalSeekKey(key), nil)
	r.Next()
}

// Valid implements the SimpleIterator interface.
func (r *sstIterator) Valid() (bool, error) {
	return r.valid && r.err == nil, r.err
}

// Next implements the SimpleIterator interface.
func (r *sstIterator) Next() {
	if r.valid = r.iter.Next(); !r.valid {
		r.err = errors.Wrap(r.iter.Close(), "closing sstable iterator")
		r.iter = nil
		return
	}

	// RocksDB uses the last 8 bytes to pack the sequence number and value type
	// into a little-endian encoded uint64. The value type is stored in the
	// low byte and the sequence number is in the high 7 bytes. See dbformat.h.
	rocksdbInternalKey := r.iter.Key()
	if len(rocksdbInternalKey) < 8 {
		r.err = errors.Errorf("invalid rocksdb InternalKey: %x", rocksdbInternalKey)
		return
	}
	seqAndValueType := binary.LittleEndian.Uint64(rocksdbInternalKey[len(rocksdbInternalKey)-8:])
	if valueType := BatchType(seqAndValueType & 0xff); valueType != BatchTypeValue {
		r.err = errors.Errorf("value type not supported: %d", valueType)
		return
	}

	key := rocksdbInternalKey[:len(rocksdbInternalKey)-8]

	if k, ts, err := enginepb.DecodeKey(key); err == nil {
		r.mvccKey.Key = k
		r.mvccKey.Timestamp = ts
		r.err = nil
	} else {
		r.err = errors.Wrapf(err, "decoding key: %s", key)
		return
	}

	if r.verify {
		r.err = roachpb.Value{RawBytes: r.iter.Value()}.Verify(r.mvccKey.Key)
	}
}

// NextKey implements the SimpleIterator interface.
func (r *sstIterator) NextKey() {
	if !r.valid {
		return
	}
	r.nextKeyStart = append(r.nextKeyStart[:0], r.mvccKey.Key...)
	for r.Next(); r.valid && r.err == nil && bytes.Equal(r.nextKeyStart, r.mvccKey.Key); r.Next() {
	}
}

// UnsafeKey implements the SimpleIterator interface.
func (r *sstIterator) UnsafeKey() MVCCKey {
	return r.mvccKey
}

// UnsafeValue implements the SimpleIterator interface.
func (r *sstIterator) UnsafeValue() []byte {
	return r.value
}
