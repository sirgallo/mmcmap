package pcmap

import "bytes"
import "fmt"
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

	fmt.Println("\npc map length:", len(pcMap.Data))
	fmt.Println("root", root, "offset:", nextOffsetInMemMap)
	
	serializedPath, serializeErr := pcMap.serializeRecursive(root, root.Version, 0, nextOffsetInMemMap)
	if serializeErr != nil { return 0, nil, serializeErr }

	return nextOffsetInMemMap, serializedPath, nil
}

func (pcMap *PCMap) serializeRecursive(node *PCMapNode, version uint64, level int, offset uint64) ([]byte, error) {
	node.StartOffset = offset

	fmt.Println("node", node, "version", version)

	sNode, serializeErr := node.SerializeNodeMeta(node.StartOffset)
	if serializeErr != nil { return nil, serializeErr }

	endOffSet, decErr := deserializeUint64(sNode[NodeEndOffsetIdx:NodeBitmapIdx])
	if decErr != nil { return nil, decErr }

	fmt.Println("end offset", endOffSet)

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
					fmt.Println("child not latest version", child, version)
					sNode = append(sNode, serializeUint64(child.StartOffset)...) 
				} else {
					fmt.Println("next start off:", nextStartOffset)
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

	baseNode = append(baseNode, sVersion...)
	baseNode = append(baseNode, sStartOffset...)
	baseNode = append(baseNode, sEndOffset...)
	baseNode = append(baseNode, sBitmap...)
	baseNode = append(baseNode, sIsLeaf)

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
	key, serializeKeyErr := serializeKey(node.Key)
	if serializeKeyErr != nil { return nil, serializeKeyErr }

	return append(key, node.Value...), nil
}

func (node *PCMapNode) SerializeINode() ([]byte, error) {
	var buf bytes.Buffer
	
	for _, cnode := range node.Children {
		snode := serializeUint64(cnode.StartOffset)
		buf.Write(snode)
	}

	return buf.Bytes(), nil
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

	node := &PCMapNode {
		Version: version,
		StartOffset: startOffset,
		EndOffset: endOffset,
		Bitmap: bitmap,
		IsLeaf: isLeaf,
	} 

	if node.IsLeaf {
		key, decErr := deserializeKey(snode[NodeKeyIdx:NodeValueIdx])
		if decErr != nil { return nil, decErr }
		value := snode[NodeValueIdx:]
		
		node.Key = key
		node.Value = value
	} else {
		totalChildren := CalculateHammingWeight(node.Bitmap)
		currOffset := NodeChildrenIdx
		for range(make([]int, totalChildren)) {
			offset, decChildErr := deserializeUint64(snode[currOffset:currOffset + 8])
			if decChildErr != nil { return nil, decChildErr }
			
			// fmt.Println("offset:", offset)
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

func serializeBoolean(val bool) byte {
	if val { return 0x01 } 

	return 0x00 
}

func deserializeBoolean(val byte) bool {
	return val == 0x01
}

func serializeKey(key []byte) ([]byte, error) {
	if len(key) > MaxKeyLength { return nil, errors.New("key longer than 64 characters, use a shorter key") }

	var buf []byte 
	if len(key) < MaxKeyLength {
		padding := make([]byte, MaxKeyLength - len(key))
		for idx := range padding {
			padding[idx] = KeyPaddingId
		}

		buf = append(key, padding...)
	} else { buf = key }
		
	return buf, nil
}

func deserializeKey(paddedKey []byte) ([]byte, error) {
	if len(paddedKey) < MaxKeyLength { return nil, errors.New("padded key incorrect length") }
	
	paddingIdx := len(paddedKey)
	for idx := range paddedKey {
		if paddedKey[idx] == KeyPaddingId {
			paddingIdx = idx
			break
		}
	}

	if paddingIdx == len(paddedKey) { 
		return paddedKey, nil
	} else { return paddedKey[:paddingIdx], nil }
}