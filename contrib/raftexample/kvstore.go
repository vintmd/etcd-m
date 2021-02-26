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
	"log"

	"go.etcd.io/etcd/etcdserver/api/snap"
)

type KVStore interface {
	// LookUp get key value
	Lookup(key string) (string, bool)

	// Propose propose kv request into raft state machine
	Propose(k, v string)

	// ReadCommits consume entry from raft state machine into KvStore map until error
	ReadCommits(commitC <-chan *string, errorC <-chan error)

	// Snapshot return KvStore snapshot
	Snapshot() ([]byte, error)

	// RecoverFromSnapshot recover data from snapshot
	RecoverFromSnapshot(snapshot []byte) error

	// Close close backend databases
	Close() error
}

func newKVStore(config *backendConfig, snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) KVStore {
	var s KVStore
	log.Printf("backend:%s,node id:%d", config.backend, config.nodeID)
	switch config.backend {
	case "memory":
		s = newSimpleMemoryKVStore(snapshotter, proposeC, config)
	case "boltdb":
		s = newBoltdbKVStore(snapshotter, proposeC, config)
	case "leveldb":
		s = newLeveldbKVStore(snapshotter, proposeC, config)
	default:
		log.Panic("invalid backend")
		return nil
	}
	// replay log into key-value map
	s.ReadCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.ReadCommits(commitC, errorC)
	return s
}

type kv struct {
	Key string
	Val string
}
