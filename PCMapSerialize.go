package pcmap

import "bytes"
// import "fmt"
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

func (node *PCMapNode) SerializeNode() ([]byte, error) {
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
	baseNode = append(baseNode, sIsLeaf...)

	switch {
		case node.IsLeaf: 
			serializedKeyVal, sErr := node.SerializeLNode()
			if sErr != nil { return nil, sErr }

			return append(baseNode, serializedKeyVal...), nil
		default:
			serializedChildren, sErr := node.SerializeINode()
			if sErr != nil { return nil, sErr }

			return append(baseNode, serializedChildren...), nil
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

func serializeBoolean(val bool) []byte {
	buf := make([]byte, 1)
	if val {
		buf[0] = 1
	} else { buf[0] = 0 }

	return buf
}

func deserializeBoolean(val byte) bool {
	return val == 1
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