package mmcmap

import "sync"
import "sync/atomic"


//============================================= MMCMap Node Pool


// NewMMCMapNodePool
//	Creates a new node pool for recycling nodes instead of letting garbage collection handle them.
//	Should help performance when there are a large number of go routines attempting to allocate/deallocate nodes.
func NewMMCMapNodePool(maxSize int64) *MMCMapNodePool {
	size := int64(0)
	np := &MMCMapNodePool{ MaxSize: maxSize, Size: size }

	iNodePool := &sync.Pool { 
		New: func() interface {} { 
			return np.resetINode(&MMCMapINode{})
		},
	}

	lNodePool := &sync.Pool {
		New: func() interface {} { 
			return np.resetLNode(&MMCMapLNode{})
		},
	}

	np.INodePool = iNodePool
	np.LNodePool = lNodePool
	np.initializePools()

	return np
}

// GetINode
//	Attempt to get a pre-allocated internal node from the node pool and decrement the total allocated nodes.
//	If the pool is empty, a new node is allocated
func (np *MMCMapNodePool) GetINode() *MMCMapINode {
	node := np.INodePool.Get().(*MMCMapINode)
	if atomic.LoadInt64(&np.Size) > 0 { atomic.AddInt64(&np.Size, -1) }

	return node
}

// GetLNode
//	Attempt to get a pre-allocated leaf node from the node pool and decrement the total allocated nodes.
//	If the pool is empty, a new node is allocated
func (np *MMCMapNodePool) GetLNode() *MMCMapLNode {
	node := np.LNodePool.Get().(*MMCMapLNode)
	if atomic.LoadInt64(&np.Size) > 0 { atomic.AddInt64(&np.Size, -1) }

	return node
}

// PutINode
//	Attempt to put an internal node back into the pool once a path has been copied + serialized.
//	If the pool is at max capacity, drop the node and let the garbage collector take care of it.
func (np *MMCMapNodePool) PutINode(node *MMCMapINode) {
	if atomic.LoadInt64(&np.Size) < np.MaxSize { 
		np.INodePool.Put(np.resetINode(node))
		atomic.AddInt64(&np.Size, 1)
	}
}

// PutLNode
//	Attempt to put a leaf node back into the pool once a path has been copied + serialized.
//	If the pool is at max capacity, drop the node and let the garbage collector take care of it.
func (np *MMCMapNodePool) PutLNode(node *MMCMapLNode) {
	if atomic.LoadInt64(&np.Size) < np.MaxSize { 
		np.LNodePool.Put(np.resetLNode(node))
		atomic.AddInt64(&np.Size, 1)
	}
}

// initializePool
//	When the mmcmap is opened, initialize the pool with the max size of nodes.
func (np *MMCMapNodePool) initializePools() {
	for range make([]int, np.MaxSize / 2) {
		np.INodePool.Put(np.resetINode(&MMCMapINode{}))
		atomic.AddInt64(&np.Size, 1)
	}

	for range make([]int, np.MaxSize / 2) {
		np.LNodePool.Put(np.resetLNode(&MMCMapLNode{}))
		atomic.AddInt64(&np.Size, 1)
	}
}

// resetINode
//	When an internal node is put back in the pool, reset the values.
func (np *MMCMapNodePool) resetINode(node *MMCMapINode) *MMCMapINode{
	node.Version = 0
	node.StartOffset = 0
	node.EndOffset = 0
	node.Bitmap = [8]uint32{0, 0, 0, 0, 0, 0, 0, 0}
	
	node.Leaf = &MMCMapLNode{ 
		Version: 0, 
		StartOffset: 0, 
		EndOffset: 0,
		KeyLength: 0, 
		Key: nil, 
		Value: nil, 
	}

	node.Children = make([]*MMCMapINode, 0)

	return node
}

// resetLNode
//	When a leaf node is put back in the pool, reset the values.
func (np *MMCMapNodePool) resetLNode(node *MMCMapLNode) *MMCMapLNode{
	node.Version = 0
	node.StartOffset = 0
	node.EndOffset = 0
	node.KeyLength = 0
	node.Key = nil
	node.Value = nil

	return node
}