package mmcmaptests

import "bytes"
import "os"
import "path/filepath"
import "testing"

import "github.com/sirgallo/mmcmap"
import "github.com/sirgallo/mmcmap/common/mmap"


var sTestPath = filepath.Join(os.TempDir(), "testserialize")
var serializePcMap *mmcmap.MMCMap


func init() {
	os.Remove(sTestPath)

	opts := mmcmap.MMCMapOpts{Filepath: sTestPath}

	var initPCMapErr error
	
	serializePcMap, initPCMapErr = mmcmap.Open(opts)
	if initPCMapErr != nil { panic(initPCMapErr.Error()) }
}


func TestMMCMapSerialize(t *testing.T) {
	defer serializePcMap.Remove()

	t.Run("Test Put Meta From Mem Map", func(t *testing.T) {
		expected := &mmcmap.MMCMapMetaData{
			Version: 0,
			RootOffset: 24,
			EndMmapOffset: 55,
		}

		mMap := serializePcMap.Data.Load().(mmap.MMap)

		deserialized, desErr := mmcmap.DeserializeMetaData(mMap[mmcmap.MetaVersionIdx:mmcmap.MetaEndMmapOffset + mmcmap.OffsetSize])
		if desErr != nil { t.Errorf("error deserializing metadata, (%s)", desErr.Error()) }

		if deserialized.Version != expected.Version {
			t.Errorf("deserialized meta not expected: actual(%v), expected(%v)", deserialized.Version, expected.Version)
		}

		if deserialized.RootOffset != expected.RootOffset {
			t.Errorf("deserialized meta root offset not expected: actual(%d), expected(%d)", deserialized.RootOffset, expected.RootOffset)
		}

		if deserialized.EndMmapOffset != expected.EndMmapOffset {
			t.Errorf("deserialized end mmap offset not expected: actual(%d), expected(%d)", deserialized.EndMmapOffset, expected.EndMmapOffset)
		}
	})

	t.Run("Test Get Meta From Mem Map", func(t *testing.T) {
		expected := &mmcmap.MMCMapMetaData{
			Version: 0,
			RootOffset: 24,
			EndMmapOffset: 55,
		}

		sMeta := expected.SerializeMetaData()
		serializePcMap.WriteMetaToMemMap(sMeta)

		deserialized, desErr := serializePcMap.ReadMetaFromMemMap()
		if desErr != nil { t.Errorf("error deserializing metadata, (%s)", desErr.Error()) }

		if deserialized.Version != expected.Version {
			t.Errorf("deserialized meta not expected: actual(%d), expected(%d)", deserialized.Version, expected.Version)
		}

		if deserialized.RootOffset != expected.RootOffset {
			t.Errorf("deserialized meta root offset not expected: actual(%d), expected(%d)", deserialized.RootOffset, expected.RootOffset)
		}

		if deserialized.EndMmapOffset != expected.EndMmapOffset {
			t.Errorf("deserialized meta end mmap not expected: actual(%d), expected(%d)", deserialized.EndMmapOffset, expected.EndMmapOffset)
		}
	})

	t.Run("Test Read Write LNode From Mem Map", func(t *testing.T) {
		newNode := &mmcmap.MMCMapNode{
			Version: 0,
			StartOffset: 24,
			Bitmap: 0,
			IsLeaf: true,
			KeyLength: uint16(len([]byte("test"))),
			Key: []byte("test"),
			Value: []byte("test"),
		}

		_, writeErr := serializePcMap.WriteNodeToMemMap(newNode)
		if writeErr != nil { t.Errorf("error writing node, (%s)", writeErr.Error()) }

		deserialized, readErr := serializePcMap.ReadNodeFromMemMap(24)
		if readErr != nil { t.Errorf("error reading node, (%s)", readErr.Error()) }

		if deserialized.Version != newNode.Version {
			t.Errorf("deserialized version not expected: actual(%d), expected(%d)", deserialized.Version, newNode.Version)
		}

		if deserialized.StartOffset != newNode.StartOffset {
			t.Errorf("deserialized start not expected: actual(%d), expected(%d)", deserialized.StartOffset, newNode.StartOffset)
		}

		expectedEndOffset := 24 + uint64(mmcmap.NodeKeyIdx + 4 + 4 - 1)
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
	})

	t.Run("Test Read Write INode From Mem Map", func(t *testing.T) {
		newNode := &mmcmap.MMCMapNode{
			Version: 1,
			StartOffset: 24,
			Bitmap: 1,
			IsLeaf: false,
			KeyLength: uint16(0),
			Children: []*mmcmap.MMCMapNode{
				{ StartOffset: 0 },
			},
		}

		_, writeErr := serializePcMap.WriteNodeToMemMap(newNode)
		if writeErr != nil { t.Errorf("error writing node, (%s)", writeErr.Error()) }

		deserialized, readErr := serializePcMap.ReadNodeFromMemMap(24)
		if readErr != nil { t.Errorf("error reading node, (%s)", readErr.Error()) }

		if deserialized.Version != newNode.Version {
			t.Errorf("deserialized version not expected: actual(%d), expected(%d)", deserialized.Version, newNode.Version)
		}

		if deserialized.StartOffset != newNode.StartOffset {
			t.Errorf("deserialized start not expected: actual(%d), expected(%d)", deserialized.StartOffset, newNode.StartOffset)
		}

		expectedEndOffset := 24 + uint64(mmcmap.NodeChildrenIdx + 8 - 1)
		if deserialized.EndOffset != expectedEndOffset {
			t.Errorf("deserialized end not expected: actual(%d), expected(%d)", deserialized.EndOffset, expectedEndOffset)
		}

		if deserialized.Bitmap != newNode.Bitmap {
			t.Errorf("deserialized bitmap not expected: actual(%d), expected(%d)", deserialized.Bitmap, newNode.Bitmap)
		}

		if deserialized.IsLeaf != newNode.IsLeaf {
			t.Errorf("deserialized isLeaf not expected: actual(%t), expected(%t)", deserialized.IsLeaf, newNode.IsLeaf)
		}
	})

	t.Log("Done")
}