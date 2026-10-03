package kv

import "sync"

// KVStore is the plain state machine: a mutex-guarded map. It knows nothing
// about Raft; the KVServer feeds it committed commands.
type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

func (kv *KVStore) Apply(cmd Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case OpSet:
		kv.data[cmd.Key] = cmd.Value
	case OpDel:
		delete(kv.data, cmd.Key)
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.data[key]
	return v, ok
}

func (kv *KVStore) Dump() map[string]string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	copyMap := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		copyMap[k] = v
	}
	return copyMap
}