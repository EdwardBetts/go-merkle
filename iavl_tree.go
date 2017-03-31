package merkle

import (
	"bytes"
	"container/list"
	"fmt"
	"sync"

	cmn "github.com/tendermint/go-common"
	dbm "github.com/tendermint/go-db"
	wire "github.com/tendermint/go-wire"
)

var rootsKey = []byte("go-merkle:roots")     // Database key for the list of versions
var orphansKey = []byte("go-merkle:orphans") // Database keys for each set of orphans
var deletesKey = []byte("go-merkle:deletes") // Database key for nodes to be pruned

const versionCount = 5

var lastId = 0

/*
Immutable AVL Tree (wraps the Node root)
This tree is not goroutine safe.
*/
type IAVLTree struct {
	ndb     *nodeDB
	version int
	roots   []*IAVLNode
	id      int
}

func NewIAVLTree(cacheSize int, db dbm.DB) *IAVLTree {
	fmt.Printf("NewIAVLTree\n")
	lastId++
	if db == nil {
		// In-memory IAVLTree
		return &IAVLTree{
			version: 0,
			roots:   make([]*IAVLNode, versionCount),
			id:      lastId,
		}
	} else {
		// Persistent IAVLTree
		ndb := newNodeDB(cacheSize, db)
		return &IAVLTree{
			ndb:     ndb,
			version: 0,
			roots:   make([]*IAVLNode, versionCount),
			id:      lastId,
		}
	}
}

func (t *IAVLTree) GetRoot(version int) *IAVLNode {
	fmt.Printf("GetRoot on id=%d version: %d Status: ", t.id, version)
	index := t.version - version
	if index >= len(t.roots) {
		fmt.Printf("Missing index: version %d nets index %d\n", version, index)
		return nil
	}

	if t.roots[index] == nil {
		fmt.Printf("Missing Root %d for index %d\n", version, index)
		return nil
	}

	//fmt.Printf("version %d nets index %d\n", version, index)
	//fmt.Printf("root %v\n", t.roots[index])
	fmt.Printf("Found\n")
	return t.roots[index]
}

// The returned tree and the original tree are goroutine independent.
// That is, they can each run in their own goroutine.
// However, upon Save(), any other trees that share a db will become
// outdated, as some nodes will become orphaned.
// Note that Save() clears leftNode and rightNode.  Otherwise,
// two copies would not be goroutine independent.
func (t *IAVLTree) Copy() Tree {
	fmt.Printf("Copy ")

	root := t.GetRoot(t.version)
	if t.roots == nil || root == nil {
		lastId++
		return &IAVLTree{
			ndb:     t.ndb,
			version: t.version,
			roots:   make([]*IAVLNode, versionCount),
			id:      lastId,
		}
	}

	if t.ndb != nil && !root.persisted {
		// Saving a tree finalizes all the nodes.
		// It sets all the hashes recursively,
		// clears all the leftNode/rightNode values recursively,
		// and all the .persisted flags get set.
		cmn.PanicSanity("It is unsafe to Copy() an unpersisted tree.")

	} else if t.ndb == nil && root.hash == nil {
		// An in-memory IAVLTree is finalized when the hashes are
		// calculated.
		root.hashWithCount(t)
	}

	tmp := make([]*IAVLNode, len(t.roots))
	copy(tmp, t.roots)

	lastId++
	return &IAVLTree{
		ndb:     t.ndb,
		version: t.version,
		roots:   tmp,
		id:      lastId,
	}
}

func (t *IAVLTree) Size() int {
	fmt.Printf("Version ")
	root := t.GetRoot(t.version)
	if root == nil {
		return 0
	}
	return root.size
}

func (t *IAVLTree) Height() int8 {
	fmt.Printf("Version ")
	root := t.GetRoot(t.version)
	if root == nil {
		return 0
	}
	return root.height
}

func (t *IAVLTree) Version() int {
	fmt.Printf("Version ")
	root := t.GetRoot(t.version)
	if root == nil {
		return 0
	}
	return root.version
}

func (t *IAVLTree) Has(key []byte) bool {
	fmt.Printf("Has ")
	root := t.GetRoot(t.version)
	if root == nil {
		return false
	}
	return root.has(t, key)
}

// Proof of the latest key
func (t *IAVLTree) Proof(key []byte) (value []byte, proofBytes []byte, exists bool) {
	fmt.Printf("Proof ")
	value, proof := t.ConstructProof(key)
	if proof == nil {
		fmt.Printf("Missing Proof\n")
		return nil, nil, false
	}
	proofBytes = wire.BinaryBytes(proof)
	return value, proofBytes, true
}

// Proof of a key at a specific version
func (t *IAVLTree) ProofVersion(key []byte, version int) (value []byte, proofBytes []byte, exists bool) {
	fmt.Printf("ProofVersion\n")
	value, proof := t.ConstructProof(key)
	if proof == nil {
		return nil, nil, false
	}
	proofBytes = wire.BinaryBytes(proof)
	return value, proofBytes, true
}

func (t *IAVLTree) Set(key []byte, value []byte) (updated bool) {
	fmt.Printf("Set %s/%s ", key, value)

	root := t.GetRoot(t.version)
	if root == nil {
		fmt.Printf("Initializing Tree\n")
		t.roots[0] = NewIAVLNode(key, value)
		root := t.GetRoot(t.version)
		if root == nil {
			cmn.PanicSanity("Didn't take?")
		}
		return false
	}

	fmt.Printf("Added new Root\n")
	t.roots[0], updated = root.set(t, key, value)
	return updated
}

func (t *IAVLTree) Hash() []byte {
	fmt.Printf("Hash ")
	root := t.GetRoot(t.version)
	if root == nil {
		return nil
	}
	hash, _ := root.hashWithCount(t)
	return hash
}

func (t *IAVLTree) HashWithCount() ([]byte, int) {
	fmt.Printf("HashWithCount ")
	root := t.GetRoot(t.version)
	if root == nil {
		return nil, 0
	}
	return root.hashWithCount(t)
}

func (t *IAVLTree) Save() []byte {
	fmt.Printf("****** Save ")
	root := t.GetRoot(t.version)
	if root == nil {
		return nil
	}
	if t.ndb != nil {
		root.save(t)
		t.ndb.Commit()
	}
	t.version++
	return root.hash
}

// Sets the root node by reading from db.
// If the hash is empty, then sets root to nil.
func (t *IAVLTree) Load(hash []byte) {
	fmt.Printf("***** Loading a tree\n")
	if len(hash) == 0 {
		t.roots[0] = nil
	} else {
		fmt.Printf("Loading Root\n")
		t.roots[0] = t.ndb.GetNode(t, hash)
	}
}

func (t *IAVLTree) Get(key []byte) (index int, value []byte, exists bool) {
	fmt.Printf("Get ")
	root := t.GetRoot(t.version)
	if root == nil {
		fmt.Printf("Failed Get for %d\n", t.version)
		return 0, nil, false
	}
	return root.get(t, key)
}

func (t *IAVLTree) GetVersion(key []byte, version int) (index int, value []byte, exists bool) {
	root := t.GetRoot(version)
	if root == nil {
		return 0, nil, false
	}
	return root.get(t, key)
}

func (t *IAVLTree) GetByIndex(index int) (key []byte, value []byte) {
	root := t.GetRoot(t.version)
	if root == nil {
		return nil, nil
	}
	return root.getByIndex(t, index)
}

func (t *IAVLTree) Remove(key []byte) (value []byte, removed bool) {
	fmt.Printf("Remove ")

	root := t.GetRoot(t.version)
	if root == nil {
		return nil, false
	}

	newRootHash, newRoot, _, value, removed := root.remove(t, key)
	if !removed {
		return nil, false
	}

	if newRoot == nil && newRootHash != nil {
		t.roots[0] = t.ndb.GetNode(t, newRootHash)
	} else {
		t.roots[0] = newRoot
	}

	return value, true
}

func (t *IAVLTree) Iterate(fn func(key []byte, value []byte) bool) (stopped bool) {
	fmt.Printf("Iterate ")
	root := t.GetRoot(t.version)
	if root == nil {
		return false
	}
	return root.traverse(t, true, func(node *IAVLNode) bool {
		if node.height == 0 {
			return fn(node.key, node.value)
		} else {
			return false
		}
	})
}

// IterateRange makes a callback for all nodes with key between start and end inclusive
// If either are nil, then it is open on that side (nil, nil is the same as Iterate)
func (t *IAVLTree) IterateRange(start, end []byte, ascending bool, fn func(key []byte, value []byte) bool) (stopped bool) {
	fmt.Printf("IterateRange ")
	root := t.GetRoot(t.version)
	if root == nil {
		return false
	}
	return root.traverseInRange(t, start, end, ascending, func(node *IAVLNode) bool {
		if node.height == 0 {
			return fn(node.key, node.value)
		} else {
			return false
		}
	})
}

//-----------------------------------------------------------------------------

type nodeDB struct {
	mtx        sync.Mutex
	cache      map[string]*list.Element
	cacheSize  int
	cacheQueue *list.List
	db         dbm.DB
	batch      dbm.Batch
	orphans    map[string]struct{}
	delete     map[string]struct{}
}

func newNodeDB(cacheSize int, db dbm.DB) *nodeDB {
	ndb := &nodeDB{
		cache:      make(map[string]*list.Element),
		cacheSize:  cacheSize,
		cacheQueue: list.New(),
		db:         db,
		batch:      db.NewBatch(),
		orphans:    make(map[string]struct{}),
		delete:     make(map[string]struct{}),
	}
	return ndb
}

func (ndb *nodeDB) GetNode(t *IAVLTree, hash []byte) *IAVLNode {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	// Check the cache.
	elem, ok := ndb.cache[string(hash)]
	if ok {
		// Already exists. Move to back of cacheQueue.
		ndb.cacheQueue.MoveToBack(elem)
		return elem.Value.(*IAVLNode)
	} else {
		// Doesn't exist, load.
		buf := ndb.db.Get(hash)
		if len(buf) == 0 {
			// ndb.db.Print()
			cmn.PanicSanity(cmn.Fmt("Value missing for key %X", hash))
		}
		node, err := MakeIAVLNode(buf, t)
		if err != nil {
			cmn.PanicCrisis(cmn.Fmt("Error reading IAVLNode. bytes: %X  error: %v", buf, err))
		}
		node.hash = hash
		node.persisted = true
		ndb.cacheNode(node)
		return node
	}
}

func (ndb *nodeDB) SaveNode(t *IAVLTree, node *IAVLNode) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if node.hash == nil {
		cmn.PanicSanity("Expected to find node.hash, but none found.")
	}
	if node.persisted {
		cmn.PanicSanity("Shouldn't be calling save on an already persisted node.")
	}

	// Save node bytes to db
	buf := bytes.NewBuffer(nil)
	_, err := node.writePersistBytes(t, buf)
	if err != nil {
		cmn.PanicCrisis(err)
	}
	ndb.batch.Set(node.hash, buf.Bytes())
	node.persisted = true
	ndb.cacheNode(node)

	// Re-creating the orphan,
	// Do not garbage collect.
	delete(ndb.orphans, string(node.hash))
}

func (ndb *nodeDB) RemoveNode(t *IAVLTree, node *IAVLNode) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if node.hash == nil {
		cmn.PanicSanity("Expected to find node.hash, but none found.")
	}
	if !node.persisted {
		cmn.PanicSanity("Shouldn't be calling remove on a non-persisted node.")
	}
	elem, ok := ndb.cache[string(node.hash)]
	if ok {
		ndb.cacheQueue.Remove(elem)
		delete(ndb.cache, string(node.hash))
	}
	ndb.orphans[string(node.hash)] = struct{}{}
}

func (ndb *nodeDB) cacheNode(node *IAVLNode) {
	// Create entry in cache and append to cacheQueue.
	elem := ndb.cacheQueue.PushBack(node)
	ndb.cache[string(node.hash)] = elem
	// Maybe expire an item.
	if ndb.cacheQueue.Len() > ndb.cacheSize {
		hash := ndb.cacheQueue.Remove(ndb.cacheQueue.Front()).(*IAVLNode).hash
		delete(ndb.cache, string(hash))
	}
}

func (ndb *nodeDB) Prune() {
	// Delete orphans from last block
	for orphanHashStr, _ := range ndb.orphans {
		ndb.batch.Delete([]byte(orphanHashStr))
	}
}

func (ndb *nodeDB) Commit() {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	// Write saves & orphan deletes
	ndb.batch.Write()
	ndb.db.SetSync(nil, nil)
	ndb.batch = ndb.db.NewBatch()

	// Shift orphans
	ndb.orphans = make(map[string]struct{})
}
