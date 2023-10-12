package pcmap

// NewLeafNode 
//	Creates a new leaf node when path copying the pcmap, which stores a key value pair.
//	It will also include the version of the pcmap.
//
// Parameters:
//	key: the incoming key to be inserted
//	value: the incoming value associated with the key
//
// Returns:
//	A new leaf node in the hash array mapped trie
func (pcMap *PCMap) NewLeafNode(key []byte, value []byte, version uint64) *PCMapNode {
	return &PCMapNode{
		Version: version,
		IsLeaf: true,
		Key: key,
		KeyLength: uint16(len(key)),
		Value: value,
	}
}

// NewInternalNode 
//	Creates a new internal node in the hash array mapped trie, which is essentially a branch node that contains pointers to child nodes.
//
// Returns:
//	A new internal node with bitmap initialized to 0 and an empty array of child nodes
func (pcMap *PCMap) NewInternalNode(version uint64) *PCMapNode {
	return &PCMapNode{
		Version: version,
		Bitmap: 0,
		IsLeaf: false,
		KeyLength: uint16(0),
		Children: []*PCMapNode{},
	}
}

// CopyNode 
//	Creates a copy of an existing node. 
//	This is used for path copying, so on operations that modify the trie, a copy is created instead of modifying the existing node. 
//	The data structure is essentially immutable. If an operation succeeds, the copy replaces the existing node, otherwise the copy is discarded.
//
// Parameters:
//	node: the existing node to create a copy of
//
// Returns:
//	A copy of the existing node within the hash array mapped trie, which the operation will modify
func (cMap *PCMap) CopyNode(node *PCMapNode) *PCMapNode {
	nodeCopy := &PCMapNode{
		Version: node.Version,
		Key: node.Key,
		Value: node.Value,
		IsLeaf: node.IsLeaf,
		Bitmap: node.Bitmap,
		KeyLength: node.KeyLength,
		Children: make([]*PCMapNode, len(node.Children)),
	}

	copy(nodeCopy.Children, node.Children)

	return nodeCopy
}

// ReadNodeFromMemMap
//	Reads a node in the pcmap from the serialized memory map.
//
// Parameters:
//	startOffset: the offset in the serialized memory map to begin reading the node from
//
// Returns:
//	A deserialized PCMapNode instance in the pcmap
func (pcMap *PCMap) ReadNodeFromMemMap(startOffset uint64) (*PCMapNode, error) {
	endOffsetIdx := startOffset + NodeEndOffsetIdx
	sEndOffset := pcMap.Data[endOffsetIdx:endOffsetIdx + OffsetSize]
	
	endOffset, decEndOffErr := deserializeUint64(sEndOffset)
	if decEndOffErr != nil { return nil, decEndOffErr }

	sNode := pcMap.Data[startOffset:endOffset + 1]

	node, decNodeErr := pcMap.DeserializeNode(sNode)
	if decNodeErr != nil { return nil, decNodeErr }

	return node, nil
}

// WriteNodeToMemMap
//	Serializes and writes a PCMapNode instance to the memory map
//
// Parameters:
//	node: the PCMapNode to be serialized
//	startOffset: the offset in the memory map where the node will begin
//
// Returns
//	true is success, error if unable to serialize or read from meta
func (pcMap *PCMap) WriteNodeToMemMap(node *PCMapNode, startOffset uint64) (bool, error) {
	if pcMap.ExistsInMemMap(startOffset) { return false, nil }

	sNode, serializeErr := node.SerializeNode(node.StartOffset)
	if serializeErr != nil { return false, serializeErr }

	_, readMetaErr := pcMap.ReadMetaFromMemMap()
	if readMetaErr != nil { return false, readMetaErr }

	pcMap.Data = append(pcMap.Data, sNode...)

	return true, nil
}

// WriteNodesToMemMap
//	Write a list of serialized nodes to the memory map
//
// Parameters:
//	snodes: the serialized, byte array representation of a list of PCMapNodes
//
// Returns
//	truthy for success
func (pcMap *PCMap) WriteNodesToMemMap(snodes []byte) bool {
	pcMap.Data = append(pcMap.Data, snodes...)
	return true
}

// determineEndOffset
//	Determine the end offset of a serialized PCMapNode.
//	For Leaf Nodes, this will be the start offset through the key index, plus the length of the key and the length of the value.
//	For Internal Nodes, this will be the start offset through the children index, plus (number of children * 8 bytes)
//
// Returns
//	The end offset for the serialized PCMapNode
func (node *PCMapNode) determineEndOffset() uint64 {
	nodeEndOffset := node.StartOffset
	
	if node.IsLeaf {
		nodeEndOffset += uint64(NodeKeyIdx + int(node.KeyLength) + len(node.Value)) 
	} else {
		encodedChildrenLength := func() int {
			totalChildren := CalculateHammingWeight(node.Bitmap)
			return totalChildren * NodeChildPtrSize
		}()

		if encodedChildrenLength != 0 {
			nodeEndOffset += uint64(NodeChildrenIdx + encodedChildrenLength)
		} else { nodeEndOffset += NodeChildrenIdx }
	}

	return nodeEndOffset - 1
}

// ExistsInMemMap
//	Check if an offset already exists in the memory map
//
// Returns
//	Truthy if the offset is already filled
func (pcMap *PCMap) ExistsInMemMap(offset uint64) bool {
	return int(offset) < len(pcMap.Data)
}

// GetNodeSize
//	Get the size of the node based on the offset values
//
// Returns
//	The size of the byte slice for the serialized node
func (node *PCMapNode) GetNodeSize() uint64 {
	return node.EndOffset - node.StartOffset
}

// GetSerializedNodeSize
//	Get the length of the node based on the length of its serialized representation
//
// Returns
//	The size of the byte slice for the serialized node
func GetSerializedNodeSize(data []byte) uint64 {
	return uint64(len(data))
}