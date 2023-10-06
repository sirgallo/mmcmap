package pcmap

import "bytes"
import "errors"
import "encoding/binary"
import "encoding/gob"


func (node *PCMapNode) SerializeNode() ([]byte, error) {
	switch {
		case node.IsLeaf: 
			return node.SerializeLNode()
		default:
			return node.SerializeINode()
	}
}

func (node *PCMapNode) SerializeLNode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	
	encBitmapErr := enc.Encode(node.Bitmap)
	if encBitmapErr != nil { return nil, encBitmapErr }

	encKeyErr := enc.Encode(node.Key)
	if encKeyErr != nil { return nil, encKeyErr }

	encValErr := enc.Encode(node.Value)
	if encValErr != nil { return nil, encValErr }

	encPageIdErr := enc.Encode(node.PageId)
	if encPageIdErr != nil { return nil, encPageIdErr }

	encOffsetErr := enc.Encode(node.Offset)
	if encOffsetErr != nil { return nil, encOffsetErr }

	return buf.Bytes(), nil
}

func (node *PCMapNode) SerializeINode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	
	encBitmapErr := enc.Encode(node.Bitmap)
	if encBitmapErr != nil { return nil, encBitmapErr }

	encPageIdErr := enc.Encode(node.PageId)
	if encPageIdErr != nil { return nil, encPageIdErr }

	encOffsetErr := enc.Encode(node.Offset)
	if encOffsetErr != nil { return nil, encOffsetErr }

	for _, cnode := range node.Children {
		snode := serializeChildPointer(cnode)
		buf.Write(snode)
	}

	return buf.Bytes(), nil
}

func DeserializeNode(snode []byte) (*PCMapNode, error) {
	buf := bytes.NewBuffer(snode)
	dec := gob.NewDecoder(buf)
	
	var node *PCMapNode

	decKeyErr := dec.Decode(&node.Key)
	if decKeyErr != nil { return nil, decKeyErr }
	
	decValErr := dec.Decode(&node.Value)
	if decValErr != nil { return nil, decValErr }

	decIsLeafErr := dec.Decode(&node.IsLeaf)
	if decIsLeafErr != nil { return nil, decIsLeafErr }

	decBitmapErr := dec.Decode(&node.Bitmap)
	if decBitmapErr != nil { return nil, decBitmapErr }

	decChildrenErr := dec.Decode(&node.Children)
	if decChildrenErr != nil { return nil, decChildrenErr }

	decPageIdErr := dec.Decode(&node.PageId)
	if decPageIdErr != nil { return nil, decPageIdErr }

	decOffsetErr := dec.Decode(&node.Offset)
	if decOffsetErr != nil { return nil, decOffsetErr }

	return node, nil
}

func serializeBitmap(bitmap uint32) ([]byte, error) {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bitmap)

	return bytes, nil
}

func serializeChildPointer(pointer *PCMapNode) []byte {
	pageIdBytes := make([]byte, 8)
  binary.LittleEndian.PutUint64(pageIdBytes, uint64(pointer.PageId))

	offsetBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(offsetBytes, uint32(pointer.Offset))

	serializedData := append(pageIdBytes, offsetBytes...)

	return serializedData
}

// Deserialize ChildPointer from a byte slice
func deserializeChildPointer(data []byte) (*PCMapNode, error) {
	if len(data) != 12 { // Check if the data length is correct (8 bytes for PageID + 4 bytes for Offset)
    return nil, errors.New("Invalid data length for ChildPointer")
  }

	// Extract PageID bytes (first 8 bytes)
	pageIDBytes := data[:8]

	// Extract Offset bytes (last 4 bytes)
	offsetBytes := data[8:]

	// Convert PageID bytes to uint64
	pageID := binary.LittleEndian.Uint64(pageIDBytes)

	// Convert Offset bytes to uint32
	offset := binary.LittleEndian.Uint32(offsetBytes)

	return &PCMapNode{
		PageId: PageId(pageID), // Assuming PageID is a uint64 type
		Offset: int(offset),
	}, nil
}