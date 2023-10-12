package pcmaptests

import (
	"bytes"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sirgallo/pcmap"
)

var sTestPath = filepath.Join(os.TempDir(), "testserialize")
var serializePcMap *pcmap.PCMap

func init() {
	opts := pcmap.PCMapOpts{Filepath: sTestPath}

	var initPCMapErr error
	serializePcMap, initPCMapErr = pcmap.Open(opts)
	if initPCMapErr != nil {
		panic(initPCMapErr.Error())
	}
}

func TestSerializeMeta(t *testing.T) {
	expected := &pcmap.PCMapMetaData{
		Version:    0,
		RootOffset: 16,
	}

	metaPtr := atomic.LoadPointer(&serializePcMap.Meta)
	sMeta := (*pcmap.PCMapMetaData)(metaPtr).SerializeMetaData()

	deserialized, desErr := pcmap.DeserializeMetaData(sMeta)
	if desErr != nil {
		t.Errorf("error deserializing metadata, (%s)", desErr.Error())
	}

	if deserialized.Version != expected.Version {
		t.Errorf("deserialized meta not expected: actual(%v), expected(%v)", deserialized.Version, expected.Version)
	}

	if deserialized.RootOffset != expected.RootOffset {
		t.Errorf("deserialized meta root offset not expected: actual(%d), expected(%d)", deserialized.RootOffset, expected.RootOffset)
	}
}

func TestMetaFromMemMap(t *testing.T) {
	expected := &pcmap.PCMapMetaData{
		Version:    1,
		RootOffset: 0,
	}

	sMeta := expected.SerializeMetaData()
	serializePcMap.WriteMetaToMemMap(sMeta)

	deserialized, desErr := serializePcMap.ReadMetaFromMemMap()
	if desErr != nil {
		t.Errorf("error deserializing metadata, (%s)", desErr.Error())
	}

	if deserialized.Version != expected.Version {
		t.Errorf("deserialized meta not expected: actual(%d), expected(%d)", deserialized.Version, expected.Version)
	}

	if deserialized.RootOffset != expected.RootOffset {
		t.Errorf("deserialized meta root offset not expected: actual(%d), expected(%d)", deserialized.RootOffset, expected.RootOffset)
	}
}

func TestReadWriteLNodeMemMap(t *testing.T) {
	startOffset := serializePcMap.DetermineNextOffset()

	newNode := &pcmap.PCMapNode{
		Version:     1,
		StartOffset: startOffset,
		Bitmap:      0,
		IsLeaf:      true,
		KeyLength:   uint16(len([]byte("test"))),
		Key:         []byte("test"),
		Value:       []byte("test"),
	}

	_, writeErr := serializePcMap.WriteNodeToMemMap(newNode, startOffset)
	if writeErr != nil {
		t.Errorf("error writing node, (%s)", writeErr.Error())
	}

	deserialized, readErr := serializePcMap.ReadNodeFromMemMap(startOffset)
	if readErr != nil {
		t.Errorf("error reading node, (%s)", readErr.Error())
	}

	if deserialized.Version != newNode.Version {
		t.Errorf("deserialized version not expected: actual(%d), expected(%d)", deserialized.Version, newNode.Version)
	}

	if deserialized.StartOffset != newNode.StartOffset {
		t.Errorf("deserialized start not expected: actual(%d), expected(%d)", deserialized.StartOffset, newNode.StartOffset)
	}

	expectedEndOffset := startOffset + pcmap.NodeKeyIdx + 4 + 4 - 1
	if deserialized.EndOffset != expectedEndOffset {
		t.Errorf("deserialized end not expected: actual(%d), expected(%d)", deserialized.EndOffset, expectedEndOffset)
	}

	if deserialized.Bitmap != newNode.Bitmap {
		t.Errorf("deserialized bitmap not expected: actual(%d), expected(%d)", deserialized.Bitmap, newNode.Bitmap)
	}

	if deserialized.IsLeaf != newNode.IsLeaf {
		t.Errorf("deserialized isLeaf not expected: actual(%t), expected(%t)", deserialized.IsLeaf, newNode.IsLeaf)
	}

	if !bytes.Equal(deserialized.Key, newNode.Key) {
		t.Errorf("deserialized key not expected: actual(%b), expected(%b)", deserialized.Key, newNode.Key)
	}

	if !bytes.Equal(deserialized.Value, newNode.Value) {
		t.Errorf("deserialized value not expected: actual(%b), expected(%b)", deserialized.Value, newNode.Value)
	}
}

func TestReadWriteINodeMemMap(t *testing.T) {
	startOffset := serializePcMap.DetermineNextOffset()

	newNode := &pcmap.PCMapNode{
		Version:     1,
		StartOffset: startOffset,
		Bitmap:      1,
		IsLeaf:      false,
		KeyLength:   uint16(0),
		Children: []*pcmap.PCMapNode{
			{StartOffset: 0},
		},
	}

	_, writeErr := serializePcMap.WriteNodeToMemMap(newNode, startOffset)
	if writeErr != nil {
		t.Errorf("error writing node, (%s)", writeErr.Error())
	}

	deserialized, readErr := serializePcMap.ReadNodeFromMemMap(startOffset)
	if readErr != nil {
		t.Errorf("error reading node, (%s)", readErr.Error())
	}

	if deserialized.Version != newNode.Version {
		t.Errorf("deserialized version not expected: actual(%d), expected(%d)", deserialized.Version, newNode.Version)
	}

	if deserialized.StartOffset != newNode.StartOffset {
		t.Errorf("deserialized start not expected: actual(%d), expected(%d)", deserialized.StartOffset, newNode.StartOffset)
	}

	expectedEndOffset := startOffset + pcmap.NodeChildrenIdx + 8 - 1
	if deserialized.EndOffset != expectedEndOffset {
		t.Errorf("deserialized end not expected: actual(%d), expected(%d)", deserialized.EndOffset, expectedEndOffset)
	}

	if deserialized.Bitmap != newNode.Bitmap {
		t.Errorf("deserialized bitmap not expected: actual(%d), expected(%d)", deserialized.Bitmap, newNode.Bitmap)
	}

	if deserialized.IsLeaf != newNode.IsLeaf {
		t.Errorf("deserialized isLeaf not expected: actual(%t), expected(%t)", deserialized.IsLeaf, newNode.IsLeaf)
	}
}
