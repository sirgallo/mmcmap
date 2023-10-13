package mmcmap

import "encoding/binary"
import "errors"


//============================================= MMCMap Serialization


// SerializeMetaData
//	Serialize the metadata at the first 0-15 bytes of the memory map. Version is 8 bytes and Root Offset is 8 bytes.
//
// Returns:
//	The serialized meta data object
func (meta *MMCMapMetaData) SerializeMetaData() []byte {
	versionBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(versionBytes, meta.Version)

	rootOffsetBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(rootOffsetBytes, meta.RootOffset)

	endMmapOffsetBytes := make([]byte, OffsetSize)
	binary.LittleEndian.PutUint64(endMmapOffsetBytes, meta.EndMmapOffset)

	offsets := append(rootOffsetBytes, endMmapOffsetBytes...)
	return append(versionBytes, offsets...)
}

// DeserializeMetaData
//	Deserialize the byte representation of the meta data object in the memory mapped file.
//
// Parameters:
//	smeta: the serialized metadata object
//
// Returns:
//	The deserialized metadata object, or error if the operation fails
func DeserializeMetaData(smeta []byte) (*MMCMapMetaData, error) {
	if len(smeta) != 24 { return nil, errors.New("meta data incorrect size") }

	versionBytes := smeta[MetaVersionIdx:MetaRootOffsetIdx]
	version := binary.LittleEndian.Uint64(versionBytes)

	rootOffsetBytes := smeta[MetaRootOffsetIdx:MetaEndMmapOffset]
	rootOffset := binary.LittleEndian.Uint64(rootOffsetBytes)

	endMmapOffsetBytes := smeta[MetaEndMmapOffset:]
	endMmapOffset := binary.LittleEndian.Uint64(endMmapOffsetBytes)

	return &MMCMapMetaData{
		Version:       version,
		RootOffset:    rootOffset,
		EndMmapOffset: endMmapOffset,
	}, nil
}

// SerializePathToMemMap
//	Serializes a path copy by starting at the root, getting the latest available offset in the memory map, and recursively serializing.
//
// Parameters:
//	root: The root node to start at in the path copy to serialize
//
// Returns:
//	The offset of the newly serialized root, the byte slice representation of the serialized path, or error if the operation fails
func (mmcMap *MMCMap) SerializePathToMemMap(root *MMCMapNode, nextOffsetInMMap uint64) ([]byte, error) {
	serializedPath, serializeErr := mmcMap.SerializeRecursive(root, root.Version, 0, nextOffsetInMMap)
	if serializeErr != nil { return nil, serializeErr }

	return serializedPath, nil
}

// SerializeRecursive
//	Traverses the path copy down to the end of the path.
//	If the node is a leaf, serialize it and return. If the node is a internal node, serialize each of the children recursively if
//	the version matches the version of the root. If it is an older version, just serialize the existing offset in the memory map.
//
// Parameters:
//	node: the node being serialized on the path
//	version: the path version in the MMCMap
//	level: the level in the path the recursive operation is at
//	offset: the next start offset of the node being serialized
//
// Returns:
//	The byte slice representation of the path at the current level, or error if the operation fails
func (mmcMap *MMCMap) SerializeRecursive(node *MMCMapNode, version uint64, level int, offset uint64) ([]byte, error) {
	node.StartOffset = offset

	sNode, serializeErr := node.SerializeNodeMeta(node.StartOffset)
	if serializeErr != nil { return nil, serializeErr }

	endOffSet, desErr := deserializeUint64(sNode[NodeEndOffsetIdx:NodeBitmapIdx])
	if desErr != nil { return nil, desErr }

	switch {
		case node.IsLeaf:
			serializedKeyVal, sLeafErr := node.SerializeLNode()
			if sLeafErr != nil { return nil, sLeafErr }

			return append(sNode, serializedKeyVal...), nil
		default:
			var childrenOnPaths []byte
			nextStartOffset := endOffSet + 1

			for _, child := range node.Children {
				if child.Version != version {
					sNode = append(sNode, serializeUint64(child.StartOffset)...)
				} else {
					sNode = append(sNode, serializeUint64(nextStartOffset)...)
					childrenOnPath, serializeErr := mmcMap.SerializeRecursive(child, node.Version, level + 1, nextStartOffset)
					if serializeErr != nil { return nil, serializeErr }

					nextStartOffset += GetSerializedNodeSize(childrenOnPath)
					childrenOnPaths = append(childrenOnPaths, childrenOnPath...)
				}
			}

			return append(sNode, childrenOnPaths...), nil
	}
}

// SerializeNodeMeta
//	Serialize the meta data for the node. These are values at fixed offsets within the MMCMapNode.
//
// Parameters:
//	offset: the start offset of the node. All other offsets are calculated based on this
//
// Returns:
//	The byte slice represntation of the node metadata, or error if failure
func (node *MMCMapNode) SerializeNodeMeta(offset uint64) ([]byte, error) {
	var baseNode []byte

	endOffset := node.determineEndOffset()

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

// SerializeNode
//	First serialize the node metadata. If the node is a leaf node, serialize the key and value.
//	Otherwise, serialize the child offsets within the internal node.
//
// Parameters:
//	offset: the offset in the memory map where the serialized node begins
//
// Returns:
//	The byte slice representation of the node or an error if the operation fails
func (node *MMCMapNode) SerializeNode(offset uint64) ([]byte, error) {
	sNode, serializeErr := node.SerializeNodeMeta(offset)
	if serializeErr != nil { return nil, serializeErr }

	switch {
		case node.IsLeaf:
			serializedKeyVal, sLeafErr := node.SerializeLNode()
			if sLeafErr != nil { return nil, sLeafErr }

			return append(sNode, serializedKeyVal...), nil
		default:
			serializedChildren, sInternalErr := node.SerializeINode()
			if sInternalErr != nil { return nil, sInternalErr }

			return append(sNode, serializedChildren...), nil
	}
}

// SerializeLNode
//	Serialize a leaf node in the mmcmap. Append the key and value together since both are already byte slices.
//
// Returns:
//	The serialized leaf node, or error if operation fails
func (node *MMCMapNode) SerializeLNode() ([]byte, error) {
	var sLNode []byte
	sLNode = append(sLNode, node.Key...)
	sLNode = append(sLNode, node.Value...)

	return sLNode, nil
}

// SerializeINode
//	Serialize an internal node in the mmcmap. This involves scanning the children nodes and serializing the offset in the memory map for each one.
//
// Returns:
//	The serialized internal node, or error if the operation fails
func (node *MMCMapNode) SerializeINode() ([]byte, error) {
	var sINode []byte
	for _, cnode := range node.Children {
		snode := serializeUint64(cnode.StartOffset)
		sINode = append(sINode, snode...)
	}

	return sINode, nil
}

// DeserializeNode
//	Deserialize a node in the memory memory map. Version, StartOffset, EndOffset, Bitmap, IsLeaf, and KeyLength are at fixed offsets in the nodes.
//	For Leaf Node, key is found from the start of the key index (31) up to the key index + key length. Value is the key index + key length up to the end of the node.
//	For Internal Node, the population count is found from the bitmap, and then children offsets are determined from (pop count * 8 bytes for offset)
//
// Parameters:
//	snode: the serialized, byte array representation of the MMCMapNode
//
// Returns:
//	The deserialized MMCMapNode, or error if the operation fails
func (mmcMap *MMCMap) DeserializeNode(snode []byte) (*MMCMapNode, error) {
	version, decVersionErr := deserializeUint64(snode[NodeVersionIdx:NodeStartOffsetIdx])
	if decVersionErr != nil {	return nil, decVersionErr }

	startOffset, decStartOffErr := deserializeUint64(snode[NodeStartOffsetIdx:NodeEndOffsetIdx])
	if decStartOffErr != nil {	return nil, decStartOffErr	}

	endOffset, decEndOffsetErr := deserializeUint64(snode[NodeEndOffsetIdx:NodeBitmapIdx])
	if decEndOffsetErr != nil {	return nil, decEndOffsetErr }

	bitmap, decBitmapErr := deserializeUint32(snode[NodeBitmapIdx:NodeIsLeafIdx])
	if decBitmapErr != nil { return nil, decBitmapErr }

	isLeaf := deserializeBoolean(snode[NodeIsLeafIdx])

	keyLength, decKeyLenErr := deserializeUint16(snode[NodeKeyLength:NodeKeyIdx])
	if decKeyLenErr != nil { return nil, decKeyLenErr	}

	node := &MMCMapNode{
		Version:     version,
		StartOffset: startOffset,
		EndOffset:   endOffset,
		Bitmap:      bitmap,
		IsLeaf:      isLeaf,
		KeyLength:   keyLength,
	}

	if node.IsLeaf {
		key := snode[NodeKeyIdx : NodeKeyIdx+node.KeyLength]
		value := snode[NodeKeyIdx+node.KeyLength:]

		node.Key = key
		node.Value = value
	} else {
		totalChildren := CalculateHammingWeight(node.Bitmap)
		currOffset := NodeChildrenIdx

		for range make([]int, totalChildren) {
			offset, decChildErr := deserializeUint64(snode[currOffset : currOffset+OffsetSize])
			if decChildErr != nil { return nil, decChildErr }

			nodePtr := &MMCMapNode{ StartOffset: offset }
			node.Children = append(node.Children, nodePtr)
			currOffset += NodeChildPtrSize
		}
	}

	return node, nil
}


//============================================= Helper Functions for Serialize/Deserialize primitives


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