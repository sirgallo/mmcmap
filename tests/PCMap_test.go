package pcmaptests

import "fmt"
import "os"
import "path/filepath"
import "sync/atomic"
import "testing"
import "unsafe"

import "github.com/sirgallo/pcmap"


var TestPath = filepath.Join(os.TempDir(), "testpcmap")
var pcMap *pcmap.PCMap


func init() {
	opts := pcmap.PCMapOpts{ Filepath: sTestPath }

	fmt.Println("initing mmap file for pcmap")
	var initPCMapErr error
	pcMap, initPCMapErr = pcmap.Open(opts)
	if initPCMapErr != nil { panic(initPCMapErr.Error()) }
}

func TestPCMap(t *testing.T) {
	var currMetaPtr unsafe.Pointer
	var currMeta *pcmap.PCMapMetaData
	var currRoot *pcmap.PCMapNode
	var delErr, getErr, readRootErr, putErr error
	var val1, val2, val3, val4 []byte

	t.Run("Test PCMap Put", func(t *testing.T) {
		_, putErr = pcMap.Put([]byte("hello"), []byte("world"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("new"), []byte("wow!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("again"), []byte("test!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("woah"), []byte("random entry"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("key"), []byte("Saturday!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("sup"), []byte("6"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("final"), []byte("the!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("6"), []byte("wow!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("asdfasdf"), []byte("add 10"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("asdfasdf"), []byte("123123"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("asd"), []byte("queue!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("fasdf"), []byte("interesting"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("yup"), []byte("random again!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("asdf"), []byte("hello"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("asdffasd"), []byte("uh oh!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("fasdfasdfasdfasdf"), []byte("error message"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("fasdfasdf"), []byte("info!"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		_, putErr = pcMap.Put([]byte("woah"), []byte("done"))
		if putErr != nil { t.Errorf("error putting key in pcmap: %s", putErr.Error()) }
	
		currMetaPtr = atomic.LoadPointer(&pcMap.Meta)
		currMeta = (*pcmap.PCMapMetaData)(currMetaPtr)
	
		currRoot, readRootErr = pcMap.ReadNodeFromMemMap(currMeta.RootOffset)
		if readRootErr != nil { t.Errorf("error reading current root: %s", readRootErr.Error()) }
		rootBitMap := currRoot.Bitmap
	
		t.Logf("pcMap after inserts")
		pcMap.PrintChildren()
	
		expectedBitMap := uint32(542198999)
		t.Logf("actual root bitmap: %d, expected root bitmap: %d\n", rootBitMap, expectedBitMap)
		t.Logf("actual root bitmap: %032b, expected root bitmap: %032b\n", rootBitMap, expectedBitMap)
		if expectedBitMap != rootBitMap {
			t.Errorf("actual bitmap does not match expected bitmap: actual(%032b), expected(%032b)\n", rootBitMap, expectedBitMap)
		}
	})

	t.Run("Test PCMap Get", func(t *testing.T) {
		expVal1 :=  "world"
		val1, getErr = pcMap.Get([]byte("hello"))
		if getErr != nil { t.Errorf("error getting val1: %s", getErr.Error()) }
		
		t.Logf("actual: %s, expected: %s", val1, expVal1)
		if string(val1) != expVal1 {
			t.Errorf("val 1 does not match expected val 1: actual(%s), expected(%s)\n", val1, expVal1)
		}
	
		expVal2 :=  "wow!"
		val2, getErr = pcMap.Get([]byte("new"))
		if getErr != nil { t.Errorf("error getting val2: %s", getErr.Error()) }
		
		t.Logf("actual: %s, expected: %s", val2, expVal2)
		if string(val2) != expVal2 {
			t.Errorf("val 2 does not match expected val 2: actual(%s), expected(%s)\n", val2, expVal2)
		}
	
		expVal3 := "hello"
		val3, getErr = pcMap.Get([]byte("asdf"))
		if getErr != nil { t.Errorf("error getting val3: %s", getErr.Error()) }
	
		t.Logf("actual: %s, expected: %s", val3, expVal3)
		if string(val3) != expVal3 {
			t.Errorf("val 3 does not match expected val 3: actual(%s), expected(%s)", val3, expVal3)
		}
	
		expVal4 := "123123"
		val4, getErr = pcMap.Get([]byte("asdfasdf"))
		if getErr != nil { t.Errorf("error getting val3: %s", getErr.Error()) }
	
		t.Logf("actual: %s, expected: %s", val4, expVal4)
		if string(val4) != expVal4 {
			t.Errorf("val 4 does not match expected val 4: actual(%s), expected(%s)", val4, expVal4)
		}
	})

	t.Run("Test PCMap Delete", func(t *testing.T) {
		_, delErr = pcMap.Delete([]byte("hello"))
		if delErr != nil { t.Errorf("error deleting key from pcmap: %s", delErr.Error()) }
	
		_, delErr = pcMap.Delete([]byte("yup"))
		if delErr != nil { t.Errorf("error deleting key from pcmap: %s", delErr.Error()) }
	
		_, delErr = pcMap.Delete([]byte("asdf"))
		if delErr != nil { t.Errorf("error deleting key from pcmap: %s", delErr.Error()) }
	
		_, delErr = pcMap.Delete([]byte("asdfasdf"))
		if delErr != nil { t.Errorf("error deleting key from pcmap: %s", delErr.Error()) }
	
		_, delErr = pcMap.Delete([]byte("new"))
		if delErr != nil { t.Errorf("error deleting key from pcmap: %s", delErr.Error()) }
	
		_, delErr = pcMap.Delete([]byte("6"))
		if delErr != nil { t.Errorf("error deleting key from pcmap: %s", delErr.Error()) }
	
		currMetaPtr = atomic.LoadPointer(&pcMap.Meta)
		currMeta = (*pcmap.PCMapMetaData)(currMetaPtr)
		
		currRoot, readRootErr = pcMap.ReadNodeFromMemMap(currMeta.RootOffset)
		if readRootErr != nil { t.Errorf("error reading current root: %s", readRootErr.Error()) }
		rootBitMapAfterDelete := currRoot.Bitmap
	
		t.Logf("bitmap of root after deletes: %032b\n", rootBitMapAfterDelete)
		t.Logf("bitmap of root after deletes: %d\n", rootBitMapAfterDelete)
	
		t.Log("pcmap after deletes")
		pcMap.PrintChildren()
	
		expectedRootBitmapAfterDelete := uint32(536956102)
		t.Log("actual bitmap:", rootBitMapAfterDelete, "expected bitmap:", expectedRootBitmapAfterDelete)
		if expectedRootBitmapAfterDelete != rootBitMapAfterDelete {
			t.Errorf("actual bitmap does not match expected bitmap: actual(%032b), expected(%032b)\n", rootBitMapAfterDelete, expectedRootBitmapAfterDelete)
		}
	})

	pcMap.Remove()
}