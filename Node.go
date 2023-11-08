package mmcmap

import "errors"
import "sync/atomic"
import "unsafe"

import "github.com/sirgallo/mmcmap/common/mmap"


//============================================= MMCMapNode Operations


// ReadNodeFromMemMap
//	Reads a node in the mmcmap from the serialized memory map.
func (mmcMap *MMCMap) ReadNodeFromMemMap(startOffset uint64) (node *MMCMapNode, err error) {
	defer func() {
		r := recover()
		if r != nil {
			node = nil
			err = errors.New("error reading node from mem map")
		}
	}()
	
	endOffsetIdx := startOffset + NodeEndOffsetIdx
	
	mMap := mmcMap.Data.Load().(mmap.MMap)
	sEndOffset := mMap[endOffsetIdx:endOffsetIdx + OffsetSize]

	endOffset, decEndOffErr := deserializeUint64(sEndOffset)
	if decEndOffErr != nil { return nil, decEndOffErr }

	sNode := mMap[startOffset:endOffset + 1]
	
	node, decNodeErr := mmcMap.DeserializeNode(sNode)
	if decNodeErr != nil { return nil, decNodeErr }

	return node, nil
}

// WriteNodeToMemMap
//	Serializes and writes a MMCMapNode instance to the memory map.
func (mmcMap *MMCMap) WriteNodeToMemMap(node *MMCMapNode) (offset uint64, err error) {
	defer func() {
		r := recover()
		if r != nil {
			offset = 0
			err = errors.New("error writing new path to mmap")
		}
	}()

	sNode, serializeErr := node.SerializeNode(node.StartOffset)
	if serializeErr != nil { return 0, serializeErr	}

	sNodeLen := uint64(len(sNode))
	endOffset := node.StartOffset + sNodeLen

	mMap := mmcMap.Data.Load().(mmap.MMap)
	copy(mMap[node.StartOffset:endOffset], sNode)

	flushErr := mmcMap.flushRegionToDisk(node.StartOffset, endOffset)
	if flushErr != nil { return 0, flushErr } 
	
	return endOffset, nil
}

// copyNode
//	Creates a copy of an existing node.
//	This is used for path copying, so on operations that modify the trie, a copy is created instead of modifying the existing node.
//	The data structure is essentially immutable. 
//	If an operation succeeds, the copy replaces the existing node, otherwise the copy is discarded.
func (mmcMap *MMCMap) copyNode(node *MMCMapNode) *MMCMapNode {
	nodeCopy := mmcMap.NodePool.Get()
	
	nodeCopy.Version = node.Version
	nodeCopy.IsLeaf = node.IsLeaf
	nodeCopy.Bitmap = node.Bitmap
	nodeCopy.KeyLength = node.KeyLength
	nodeCopy.Key = node.Key
	nodeCopy.Value = node.Value
	nodeCopy.Children = make([]*MMCMapNode, len(node.Children))

	copy(nodeCopy.Children, node.Children)
	
	return nodeCopy
}

// determineEndOffset
//	Determine the end offset of a serialized MMCMapNode.
//	For Leaf Nodes, this will be the start offset through the key index, plus the length of the key and the length of the value.
//	For Internal Nodes, this will be the start offset through the children index, plus (number of children * 8 bytes).
func (node *MMCMapNode) determineEndOffset() uint64 {
	nodeEndOffset := node.StartOffset

	if node.IsLeaf {
		nodeEndOffset += uint64(NodeKeyIdx + int(node.KeyLength) + len(node.Value))
	} else {
		encodedChildrenLength := func() int {
			totalChildren := calculateHammingWeight(node.Bitmap)
			return totalChildren * NodeChildPtrSize
		}()

		if encodedChildrenLength != 0 {
			nodeEndOffset += uint64(NodeChildrenIdx + encodedChildrenLength)
		} else { nodeEndOffset += NodeChildrenIdx }
	}

	return nodeEndOffset - 1
}

// getSerializedNodeSize
//	Get the length of the node based on the length of its serialized representation.
func getSerializedNodeSize(data []byte) uint64 {
	return uint64(len(data))
}

// initRoot
//	Initialize the Version 0 root where operations will begin traversing.
func (mmcMap *MMCMap) initRoot() (uint64, error) {
	root := &MMCMapNode{
		Version: 0,
		StartOffset: uint64(InitRootOffset),
		Bitmap: 0,
		IsLeaf: false,
		KeyLength: uint16(0),
		Children: []*MMCMapNode{},
	}

	endOffset, writeNodeErr := mmcMap.WriteNodeToMemMap(root)
	if writeNodeErr != nil { return 0, writeNodeErr }

	return endOffset, nil
}

// loadNodeFromPointer
//	Load a mmcmap node from an unsafe pointer.
func loadNodeFromPointer(ptr *unsafe.Pointer) *MMCMapNode {
	return (*MMCMapNode)(atomic.LoadPointer(ptr))
}

// newInternalNode
//	Creates a new internal node in the hash array mapped trie, which is essentially a branch node that contains pointers to child nodes.
func (mmcMap *MMCMap) newInternalNode(version uint64) *MMCMapNode {
	iNode := mmcMap.NodePool.Get()

	iNode.Version = version
	iNode.Bitmap = 0
	iNode.IsLeaf = false
	iNode.KeyLength = uint16(0)
	iNode.Children = []*MMCMapNode{}

	return iNode
}

// newLeafNode
//	Creates a new leaf node when path copying the mmcmap, which stores a key value pair.
//	It will also include the version of the mmcmap.
func (mmcMap *MMCMap) newLeafNode(key, value []byte, version uint64) *MMCMapNode {
	lNode := mmcMap.NodePool.Get()

	lNode.Version = version
	lNode.Bitmap = 0
	lNode.IsLeaf = true
	lNode.KeyLength = uint16(len(key))
	lNode.Key = key
	lNode.Value = value

	return lNode
}

// storeNodeAsPointer
//	Store a mmcmap node as an unsafe pointer.
func storeNodeAsPointer(node *MMCMapNode) *unsafe.Pointer {
	ptr := unsafe.Pointer(node)
	return &ptr
}

// writeNodesToMemMap
//	Write a list of serialized nodes to the memory map. If the mem map is too small for the incoming nodes, dynamically resize.
func (mmcMap *MMCMap) writeNodesToMemMap(snodes []byte, offset uint64) (ok bool, err error) {
	defer func() {
		r := recover()
		if r != nil {
			ok = false
			err = errors.New("error writing new path to mmap")
		}
	}()

	lenSNodes := uint64(len(snodes))
	endOffset := offset + lenSNodes

	mMap := mmcMap.Data.Load().(mmap.MMap)
	copy(mMap[offset:endOffset], snodes)

	return true, nil
}