// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"

	"go.etcd.io/etcd/etcdserver/api/snap"

	"github.com/syndtr/goleveldb/leveldb"
)

// a key-value store backed by raft
type leveldbKVStore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	db          *leveldb.DB
	snapshotter *snap.Snapshotter
	config      *backendConfig
}

func newLeveldbKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, config *backendConfig) KVStore {
	s := &leveldbKVStore{proposeC: proposeC, snapshotter: snapshotter, config: config}
	s.db = openLevelDB(config)
	return s
}

func openLevelDB(config *backendConfig) *leveldb.DB {
	db, err := leveldb.OpenFile(fmt.Sprintf("raftexample-%d-snap", config.nodeID)+"/leveldb", nil)
	if err != nil {
		log.Panic("failed to open leveldb")
	}
	return db
}

func (s *leveldbKVStore) Lookup(key string) (string, bool) {
	value, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return "", false
	}
	log.Printf("backend:%s,get key:%s,value:%s succ", s.config.backend, key, value)
	return string(value), true
}

func (s *leveldbKVStore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *leveldbKVStore) ReadCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.RecoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		log.Printf("key %s", dataKv.Key)
		s.Put(dataKv.Key, dataKv.Val)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *leveldbKVStore) Put(key, value string) error {
	err := s.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Printf("failed to put key %s, value %s, err is %v", key, value, err)
		return err
	}
	log.Printf("backend:%s,put key:%s,value:%s succ", s.config.backend, key, value)
	return nil
}

func (s *leveldbKVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// to do
	return nil, nil
}

func (s *leveldbKVStore) RecoverFromSnapshot(snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// to do
	return nil
}

func (s *leveldbKVStore) Close() error {
	return s.db.Close()
}
