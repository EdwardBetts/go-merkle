package merkle

import (
	"fmt"
	"strings"

	wire "github.com/tendermint/go-wire"
)

type Formatter func(in []byte) (out string)

type KeyValueMapping struct {
	Key   Formatter
	Value Formatter
}

func defaultMapping(value []byte) string {
	return fmt.Sprintf("%X", value)
}

func valueMapping(value []byte) string {
	// underneath make node, wire can throw a panic
	defer func() {
		if recover() != nil {
			return
		}
	}()

	// test to see if this is a node
	node, err := MakeIAVLNode(value, nil)
	if err == nil {
		return nodeMapping(node)
	}

	// Unknown value type
	return stateMapping(value)
}

// This is merkleeyes state, that it is writing to a specific key
type state struct {
	Hash   []byte
	Height uint64
}

func stateMapping(value []byte) string {
	var s state
	err := wire.ReadBinaryBytes(value, &s)
	if err != nil {
		return defaultMapping(value)
	}
	return fmt.Sprintf("Height:%d,[%X]", s.Height, s.Hash)
}

type account struct {
	PubKey   [32]byte
	Sequence int
	Balance  [1]coin
}

type wrapper struct {
	bytes []byte
}

type coin struct {
	Denom  string
	Amount int64
}

func nodeMapping(node *IAVLNode) string {
	var prefix = "base/a/"

	// The key might have come from basecoin
	formattedKey := string(node.key)
	if strings.HasPrefix(formattedKey, prefix) {
		formattedKey = strings.TrimPrefix(formattedKey, prefix)
	} else {
		prefix = ""
	}

	var formattedValue string
	var acc account
	err := wire.ReadBinaryBytes(node.value, &acc)
	if err != nil {
		formattedValue = fmt.Sprintf("%v", acc)
	} else {
		formattedValue = fmt.Sprintf("%X", node.value)
	}

	// Generic key, but still a node
	return fmt.Sprintf("IAVLNode: [height: %d, key: %s%X, value: %s, hash: %X, leftHash: %X, rightHash: %X]",
		node.height, prefix, formattedKey, formattedValue, node.hash, node.leftHash, node.rightHash)
}

// Dump everything in the database
func (t *IAVLTree) Dump(mapping *KeyValueMapping) {
	if t.root == nil {
		fmt.Printf("No root loaded into memory\n")
	}

	if mapping == nil {
		mapping = &KeyValueMapping{Key: defaultMapping, Value: valueMapping}
	}

	stats := t.ndb.db.Stats()
	for key, value := range stats {
		fmt.Printf("%s:\n\t%s\n", key, value)
	}

	iter := t.ndb.db.Iterator()
	for iter.Next() {
		fmt.Printf("DBkey: [%s] DBValue: [%s]\n", mapping.Key(iter.Key()), mapping.Value(iter.Value()))
	}
}
