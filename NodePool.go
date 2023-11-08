package mmcmap

import "sync"
import "sync/atomic"


//============================================= MMCMap Node Pool


// NewMMCMapNodePool
//	Creates a new node pool for recycling nodes instead of letting garbage collection handle them.
//	Should help performance when there are a large number of go routines attempting to allocate/deallocate nodes.
func NewMMCMapNodePool(maxSize int64) *MMCMapNodePool {
	pool := &sync.Pool { 
		New: func() interface {} { 
			return &MMCMapNode{} 
		},
	}

	size := int64(0)
	np := &MMCMapNodePool{ maxSize, size, pool }
	np.initializePool()

	return np
}

// Get
//	Attempt to get a pre-allocated node from the node pool and decrement the total allocated nodes.
//	If the pool is empty, a new node is allocated
func (np *MMCMapNodePool) Get() *MMCMapNode {
	node := np.Pool.Get().(*MMCMapNode)
	if atomic.LoadInt64(&np.Size) > 0 { atomic.AddInt64(&np.Size, -1) }

	return node
}

// Put
//	Attempt to put a node back into the pool once a path has been copied + serialized.
//	If the pool is at max capacity, drop the node and let the garbage collector take care of it.
func (np *MMCMapNodePool) Put(node *MMCMapNode) {
	if atomic.LoadInt64(&np.Size) < np.MaxSize { 
		np.Pool.Put(np.resetNode(node))
		atomic.AddInt64(&np.Size, 1)
	}
}

// initializePool
//	When the mmcmap is opened, initialize the pool with the max size of nodes.
func (np *MMCMapNodePool) initializePool() {
	for range make([]int, np.MaxSize) {
		np.Pool.Put(&MMCMapNode{})
		atomic.AddInt64(&np.Size, 1)
	}
}

// resetNode
//	When a node is put back in the pool, reset the values.
func (np *MMCMapNodePool) resetNode(node *MMCMapNode) *MMCMapNode{
	node.Version = 0
	node.Bitmap = 0
	node.StartOffset = 0
	node.EndOffset = 0
	node.KeyLength = 0
	node.Key = nil
	node.Value = nil
	node.Children = nil

	return node
}