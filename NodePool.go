package mmcmap

import "sync"
import "sync/atomic"


func NewMMCMapNodePool(maxSize int64) *MMCMapNodePool {
	pool := &sync.Pool { 
		New: func() interface {} { return &MMCMapNode{} },
	}

	size := int64(0)
	np := &MMCMapNodePool{ maxSize, size, pool }
	np.initializePool()

	return np
}

func (np *MMCMapNodePool) Get() *MMCMapNode {
	node := np.pool.Get().(*MMCMapNode)
	if atomic.LoadInt64(&np.size) > 0 { atomic.AddInt64(&np.size, -1) }

	return node
}

func (np *MMCMapNodePool) Put(node *MMCMapNode) {
	if atomic.LoadInt64(&np.size) < np.maxSize { 
		np.pool.Put(np.resetNode(node))
		atomic.AddInt64(&np.size, 1)
	}
}

func (np *MMCMapNodePool) initializePool() {
	for range make([]int, np.maxSize) {
		np.pool.Put(&MMCMapNode{})
		
		atomic.AddInt64(&np.size, 1)
	}
}

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