package pcmap

import "encoding/binary"
import "errors"


func (meta *PCMapMetaData) SerializeMetaData() []byte {
	versionBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(versionBytes, meta.Version)

	rootOffsetBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(rootOffsetBytes, meta.RootOffset)

	return append(versionBytes, rootOffsetBytes...)
}

func DeserializeMetaData(smeta []byte) (*PCMapMetaData, error) {
	if len(smeta) != 16 { return nil, errors.New("meta data incorrect size") }
	
	versionBytes := smeta[MetaVersionIdx:MetaRootOffsetIdx]
	version := binary.LittleEndian.Uint64(versionBytes)

	rootOffsetBytes := smeta[MetaRootOffsetIdx:]
	rootOffset := binary.LittleEndian.Uint64(rootOffsetBytes)

	return &PCMapMetaData{
		Version: version,
		RootOffset: rootOffset,
	}, nil
}

func (pcMap *PCMap) SerializePathToMemMap(root *PCMapNode) (uint64, []byte, error) {
	nextOffsetInMemMap := pcMap.DetermineNextOffset() 
	
	serializedPath, serializeErr := pcMap.serializeRecursive(root, root.Version, 0, nextOffsetInMemMap)
	if serializeErr != nil { return 0, nil, serializeErr }

	return nextOffsetInMemMap, serializedPath, nil
}

func (pcMap *PCMap) serializeRecursive(node *PCMapNode, version uint64, level int, offset uint64) ([]byte, error) {
	node.StartOffset = offset

	sNode, serializeErr := node.SerializeNodeMeta(node.StartOffset)
	if serializeErr != nil { return nil, serializeErr }

	endOffSet, decErr := deserializeUint64(sNode[NodeEndOffsetIdx:NodeBitmapIdx])
	if decErr != nil { return nil, decErr }

	switch {
		case node.IsLeaf:
			serializedKeyVal, sErr := node.SerializeLNode()
			if sErr != nil { return nil, sErr }

			return append(sNode, serializedKeyVal...), nil
		default:
			var childrenOnPaths []byte
			nextStartOffset := endOffSet + 1

			for _, child := range node.Children {
				if child.Version != version {
					sNode = append(sNode, serializeUint64(child.StartOffset)...) 
				} else {
					sNode = append(sNode, serializeUint64(nextStartOffset)...)
					childrenOnPath, serializeErr := pcMap.serializeRecursive(child, node.Version, level + 1, nextStartOffset)
					if serializeErr != nil { return nil, serializeErr }

					nextStartOffset += GetSerializedNodeSize(childrenOnPath)
					childrenOnPaths = append(childrenOnPaths, childrenOnPath...)
				}
			}

			return append(sNode, childrenOnPaths...), nil
	}
}

func (node *PCMapNode) SerializeNodeMeta(offset uint64) ([]byte, error) {
	endOffset := node.determineEndOffset()

	var baseNode []byte
	
	sVersion := serializeUint64(node.Version)
	sStartOffset := serializeUint64(node.StartOffset)
	sEndOffset := serializeUint64(endOffset)
	sBitmap := serializeUint32(node.Bitmap)
	sIsLeaf := serializeBoolean(node.IsLeaf)
	sKeyLength := serializeUint16(node.KeyLength)

	baseNode = append(baseNode, sVersion...)
	baseNode = append(baseNode, sStartOffset...)
	baseNode = append(baseNode, sEndOffset...)
	baseNode = append(baseNode, sBitmap...)
	baseNode = append(baseNode, sIsLeaf)
	baseNode = append(baseNode, sKeyLength...)

	return baseNode, nil
}

func (node *PCMapNode) SerializeNode(offset uint64) ([]byte, error) {
	sNode, serializeErr := node.SerializeNodeMeta(offset)
	if serializeErr != nil { return nil, serializeErr }

	switch {
		case node.IsLeaf: 
			serializedKeyVal, sErr := node.SerializeLNode()
			if sErr != nil { return nil, sErr }

			return append(sNode, serializedKeyVal...), nil
		default:
			serializedChildren, sErr := node.SerializeINode()
			if sErr != nil { return nil, sErr }

			return append(sNode, serializedChildren...), nil
	}
}

func (node *PCMapNode) SerializeLNode() ([]byte, error) {
	var sLNode []byte

	sLNode = append(sLNode, node.Key...)
	sLNode = append(sLNode, node.Value...)

	return sLNode, nil
}

func (node *PCMapNode) SerializeINode() ([]byte, error) {
	var sINode []byte

	for _, cnode := range node.Children {
		snode := serializeUint64(cnode.StartOffset)
		sINode = append(sINode, snode...)
	}

	return sINode, nil
}

func (pcMap *PCMap) DeserializeNode(snode []byte) (*PCMapNode, error) {
	version, decVersionErr := deserializeUint64(snode[NodeVersionIdx:NodeStartOffsetIdx])
	if decVersionErr != nil { return nil, decVersionErr }

	startOffset, decStartOffErr := deserializeUint64(snode[NodeStartOffsetIdx:NodeEndOffsetIdx])
	if decStartOffErr != nil { return nil, decStartOffErr }

	endOffset, decEndOffsetErr := deserializeUint64(snode[NodeEndOffsetIdx:NodeBitmapIdx])
	if decEndOffsetErr != nil { return nil, decEndOffsetErr }

	bitmap, decBitmapErr := deserializeUint32(snode[NodeBitmapIdx:NodeIsLeafIdx])
	if decBitmapErr != nil { return nil, decBitmapErr }

	isLeaf := deserializeBoolean(snode[NodeIsLeafIdx])

	keyLength, decKeyLenErr := deserializeUint16(snode[NodeKeyLength:NodeKeyIdx])
	if decKeyLenErr != nil { return nil, decKeyLenErr }

	node := &PCMapNode {
		Version: version,
		StartOffset: startOffset,
		EndOffset: endOffset,
		Bitmap: bitmap,
		IsLeaf: isLeaf,
		KeyLength: keyLength,
	} 

	if node.IsLeaf {
		key := snode[NodeKeyIdx:NodeKeyIdx + node.KeyLength]
		value := snode[NodeKeyIdx + node.KeyLength:]
		
		node.Key = key
		node.Value = value
	} else {
		totalChildren := CalculateHammingWeight(node.Bitmap)
		currOffset := NodeChildrenIdx
		
		for range(make([]int, totalChildren)) {
			offset, decChildErr := deserializeUint64(snode[currOffset:currOffset + 8])
			if decChildErr != nil { return nil, decChildErr }

			nodePtr := &PCMapNode{ StartOffset: offset }
			node.Children = append(node.Children, nodePtr)
			currOffset += NodeChildPtrSize
		}
	}

	return node, nil
}

func serializeUint64(in uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, in)

	return buf
}

func deserializeUint64(data []byte) (uint64, error) {
	if len(data) != 8 { return uint64(0), errors.New("invalid data length for byte slice to uint64") }
	return binary.LittleEndian.Uint64(data), nil
}

func serializeUint32(in uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, in)

	return buf
}

func deserializeUint32(data []byte) (uint32, error) {
	if len(data) != 4 { return uint32(0), errors.New("invalid data length for byte slice to uint32") }
	return binary.LittleEndian.Uint32(data), nil
}

func serializeUint16(in uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, in)

	return buf
}

func deserializeUint16(data []byte) (uint16, error) {
	if len(data) != 2 { return uint16(0), errors.New("invalid data length for byte slice to uint16") }
	return binary.LittleEndian.Uint16(data), nil
}

func serializeBoolean(val bool) byte {
	if val { return 0x01 } 
	return 0x00 
}

func deserializeBoolean(val byte) bool {
	return val == 0x01
}