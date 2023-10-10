package pcmap


import "fmt"
// NewLeafNode creates a new leaf node in the hash array mapped trie, which stores a key value pair
//
// Parameters:
//	key: the incoming key to be inserted
//	value: the incoming value associated with the key
//
// Returns:
//	A new leaf node in the hash array mapped trie
func (pcMap *PCMap) NewLeafNode(key []byte, value []byte, version uint64, offset uint64) *PCMapNode {
	return &PCMapNode{
		Version: version,
		StartOffset: offset,
		IsLeaf: true,
		Key: key,
		Value: value,
	}
}

// NewInternalNode creates a new internal node in the hash array mapped trie, which is essentially a branch node that contains pointers to child nodes
//
// Returns:
//	A new internal node with bitmap initialized to 0 and an empty array of child nodes
func (pcMap *PCMap) NewInternalNode(version uint64, offset uint64) *PCMapNode {
	return &PCMapNode{
		Version: version,
		StartOffset: offset,
		Bitmap: 0,
		IsLeaf: false,
		Children: []*PCMapNode{},
	}
}

func (pcMap *PCMap) ReadNodeFromMemMap(startOffset uint64) (*PCMapNode, error) {
	pcMap.RWLock.RLock()
	defer pcMap.RWLock.RUnlock()

	endOffsetIdx := startOffset + NodeEndOffsetIdx
	fmt.Println()
	sEndOffset := pcMap.Data[endOffsetIdx:endOffsetIdx + OffsetSize]
	
	endOffset, decEndOffErr := deserializeUint64(sEndOffset)
	if decEndOffErr != nil { return nil, decEndOffErr }

	sNode := pcMap.Data[startOffset:endOffset]

	node, decNodeErr := pcMap.DeserializeNode(sNode)
	if decNodeErr != nil { return nil, decNodeErr }

	return node, nil
}

func (pcMap *PCMap) WriteNodeToMemMap(node *PCMapNode, startOffset uint64) (bool, error) {
	if pcMap.ExistsInMemMap(startOffset) { return false, nil }

	sNode, serializeErr := node.SerializeNode()
	if serializeErr != nil { return false, serializeErr }

	_, readMetaErr := pcMap.ReadMetaFromMemMap()
	if readMetaErr != nil { return false, readMetaErr }

	pcMap.Data = append(pcMap.Data, sNode...)

	return true, nil
}

func (pcMap *PCMap) WriteNodesToMemMap(snodes []byte) bool {	
	pcMap.Data = append(pcMap.Data, snodes...)
	return true
}

func (node *PCMapNode) determineEndOffset() uint64 {
	nodeEndOffset := node.StartOffset
	
	if node.IsLeaf { 
		nodeEndOffset += uint64(NodeValueIdx + len(node.Value)) 
	} else {
		encodedChildrenLength := func() int {
			totalChildren := CalculateHammingWeight(node.Bitmap)
			return totalChildren * NodeChildPtrSize
		}()

		if encodedChildrenLength != 0 {
			nodeEndOffset += uint64(NodeChildrenIdx + encodedChildrenLength)
		} else { nodeEndOffset += NodeChildrenIdx }
	}

	return nodeEndOffset
}

func (pcMap *PCMap) ExistsInMemMap(offset uint64) bool {
	return int(offset) < len(pcMap.Data)
}

func (node *PCMapNode) GetNodeSize() uint64 {
	return node.EndOffset - node.StartOffset
}

func GetSerializedNodeSize(data []byte) uint64 {
	return uint64(len(data))
}