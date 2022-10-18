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
	"errors"
	"fmt"
	"log"
	"sync"

	"go.etcd.io/etcd/etcdserver/api/snap"

	bolt "go.etcd.io/bbolt"
)

// a key-value store backed by raft
type boltdbKVStore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	db          *bolt.DB
	snapshotter *snap.Snapshotter
	config      *backendConfig
}

func newBoltdbKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, config *backendConfig) KVStore {
	s := &boltdbKVStore{proposeC: proposeC, snapshotter: snapshotter, config: config}
	s.db = openBoltDB(config)
	return s
}

func openBoltDB(config *backendConfig) *bolt.DB {
	bopts := &bolt.Options{}
	bopts.InitialMmapSize = 4 * 1024 * 1024 * 1024
	db, err := bolt.Open(fmt.Sprintf("raftexample-%d-snap", config.nodeID)+"/boltdb", 0600, bopts)
	if err != nil {
		log.Panic("failed to open boltdb")
	}
	return db
}

func (s *boltdbKVStore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Start a readable transaction.
	tx, err := s.db.Begin(false)
	if err != nil {
		return "", false
	}
	defer tx.Rollback()
	// Use the transaction...
	bucket := tx.Bucket([]byte("keys"))
	if bucket == nil {
		return "", false
	}
	value := bucket.Get([]byte(key))
	log.Printf("backend:%s,get key:%s,value:%s succ", s.config.backend, key, value)
	return string(value), true
}

func (s *boltdbKVStore) Propose(k string, v, o string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v, o}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *boltdbKVStore) ReadCommits(commitC <-chan *string, errorC <-chan error) {
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
		log.Printf("key %s op %s", dataKv.Key, dataKv.Op)
		if dataKv.Op == PutOp {
			s.Put(dataKv.Key, dataKv.Val)
		} else if dataKv.Op == DeleteOp {
			s.Delete(dataKv.Key)
		}
	}

	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *boltdbKVStore) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Start a writable transaction.
	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Use the transaction...
	bucket, err := tx.CreateBucketIfNotExists([]byte("keys"))
	if err != nil {
		log.Printf("failed to put key %s, value %s, err is %v", key, value, err)
		return err
	}
	err = bucket.Put([]byte(key), []byte(value))
	if err != nil {
		log.Printf("failed to put key %s, value %s, err is %v", key, value, err)
		return err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		log.Printf("failed to commit transaction, key %s, err is %v", key, err)
		return err
	}
	log.Printf("backend:%s,put key:%s,value:%s succ", s.config.backend, key, value)
	return nil
}

func (s *boltdbKVStore) Delete(key string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Start a readable transaction.
	tx, err := s.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Use the transaction...
	bucket := tx.Bucket([]byte("keys"))
	if bucket == nil {
		return errors.New("empty buckets")
	}
	err = bucket.Delete([]byte(key))
	if err != nil {
		log.Printf("backend:%s,delete key:%s failed err: %v", s.config.backend, key, err)
		return err
	}
	log.Printf("backend:%s,delete key:%s succ", s.config.backend, key)
	return nil
}


func (s *boltdbKVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// to do
	return nil, nil
}

func (s *boltdbKVStore) RecoverFromSnapshot(snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// to do
	return nil
}

func (s *boltdbKVStore) Close() error {
	return s.db.Close()
}
