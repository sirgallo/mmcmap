package mmcmaptests

import "os"
import "path/filepath"
import "sync/atomic"
import "testing"
import "unsafe"

import "github.com/sirgallo/mmcmap"
import "github.com/sirgallo/mmcmap/common/mmap"


var TestPath = filepath.Join(os.TempDir(), "testmmcmap")
var mmcMap *mmcmap.MMCMap


func init() {
	os.Remove(TestPath)
	opts := mmcmap.MMCMapOpts{ Filepath: TestPath }
	
	var initPCMapErr error
	
	mmcMap, initPCMapErr = mmcmap.Open(opts)
	if initPCMapErr != nil { panic(initPCMapErr.Error()) }
}


func TestMMCMap(t *testing.T) {
	defer mmcMap.Remove()

	var currRoot *mmcmap.MMCMapNode
	var delErr, getErr, readRootErr, putErr error
	var val1, val2, val3, val4 []byte

	t.Run("Test MMCMap Put", func(t *testing.T) {
		_, putErr = mmcMap.Put([]byte("hello"), []byte("world"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("new"), []byte("wow!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("again"), []byte("test!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("woah"), []byte("random entry"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("key"), []byte("Saturday!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("sup"), []byte("6"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("final"), []byte("the!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("6"), []byte("wow!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("asdfasdf"), []byte("add 10"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("asdfasdf"), []byte("123123"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("asd"), []byte("queue!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("fasdf"), []byte("interesting"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("yup"), []byte("random again!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("asdf"), []byte("hello"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("asdffasd"), []byte("uh oh!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("fasdfasdfasdfasdf"), []byte("error message"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("fasdfasdf"), []byte("info!"))
		if putErr != nil {
			t.Errorf("error putting key in mmcmap: %s", putErr.Error())
		}

		_, putErr = mmcMap.Put([]byte("woah"), []byte("done"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		mMap := mmcMap.Data.Load().(mmap.MMap)
		
		rootOffsetPtr := (*uint64)(unsafe.Pointer(&mMap[mmcmap.MetaRootOffsetIdx]))
		rootOffset := atomic.LoadUint64(rootOffsetPtr)
		
		currRoot, readRootErr = mmcMap.ReadNodeFromMemMap(rootOffset)
		if readRootErr != nil { t.Errorf("error reading current root: %s", readRootErr.Error()) }
		
		rootBitMap := currRoot.Bitmap

		t.Logf("mmcMap after inserts")
		mmcMap.PrintChildren()

		expectedBitMap := uint32(542198999)
		t.Logf("actual root bitmap: %d, expected root bitmap: %d\n", rootBitMap, expectedBitMap)
		t.Logf("actual root bitmap: %032b, expected root bitmap: %032b\n", rootBitMap, expectedBitMap)
		if expectedBitMap != rootBitMap {
			t.Errorf("actual bitmap does not match expected bitmap: actual(%032b), expected(%032b)\n", rootBitMap, expectedBitMap)
		}
	})

	t.Run("Test MMCMap Get", func(t *testing.T) {
		expVal1 := "world"
		val1, getErr = mmcMap.Get([]byte("hello"))
		if getErr != nil { t.Errorf("error getting val1: %s", getErr.Error()) }

		t.Logf("actual: %s, expected: %s", val1, expVal1)
		if string(val1) != expVal1 {
			t.Errorf("val 1 does not match expected val 1: actual(%s), expected(%s)\n", val1, expVal1)
		}

		expVal2 := "wow!"
		val2, getErr = mmcMap.Get([]byte("new"))
		if getErr != nil { t.Errorf("error getting val2: %s", getErr.Error()) }

		t.Logf("actual: %s, expected: %s", val2, expVal2)
		if string(val2) != expVal2 {
			t.Errorf("val 2 does not match expected val 2: actual(%s), expected(%s)\n", val2, expVal2)
		}

		expVal3 := "hello"
		val3, getErr = mmcMap.Get([]byte("asdf"))
		if getErr != nil { t.Errorf("error getting val3: %s", getErr.Error()) }

		t.Logf("actual: %s, expected: %s", val3, expVal3)
		if string(val3) != expVal3 {
			t.Errorf("val 3 does not match expected val 3: actual(%s), expected(%s)", val3, expVal3)
		}

		expVal4 := "123123"
		val4, getErr = mmcMap.Get([]byte("asdfasdf"))
		if getErr != nil { t.Errorf("error getting val3: %s", getErr.Error()) }

		t.Logf("actual: %s, expected: %s", val4, expVal4)
		if string(val4) != expVal4 {
			t.Errorf("val 4 does not match expected val 4: actual(%s), expected(%s)", val4, expVal4)
		}
	})

	t.Run("Test MMCMap Delete", func(t *testing.T) {
		_, delErr = mmcMap.Delete([]byte("hello"))
		if delErr != nil { t.Errorf("error deleting key from mmcmap: %s", delErr.Error()) }

		_, delErr = mmcMap.Delete([]byte("yup"))
		if delErr != nil { t.Errorf("error deleting key from mmcmap: %s", delErr.Error()) }

		_, delErr = mmcMap.Delete([]byte("asdf"))
		if delErr != nil { t.Errorf("error deleting key from mmcmap: %s", delErr.Error()) }

		_, delErr = mmcMap.Delete([]byte("asdfasdf"))
		if delErr != nil { t.Errorf("error deleting key from mmcmap: %s", delErr.Error()) }

		_, delErr = mmcMap.Delete([]byte("new"))
		if delErr != nil { t.Errorf("error deleting key from mmcmap: %s", delErr.Error())
		}

		_, delErr = mmcMap.Delete([]byte("6"))
		if delErr != nil { t.Errorf("error deleting key from mmcmap: %s", delErr.Error()) }

		mMap := mmcMap.Data.Load().(mmap.MMap)

		rootOffsetPtr := (*uint64)(unsafe.Pointer(&mMap[mmcmap.MetaRootOffsetIdx]))
		rootOffset := atomic.LoadUint64(rootOffsetPtr)

		currRoot, readRootErr = mmcMap.ReadNodeFromMemMap(rootOffset)
		if readRootErr != nil { t.Errorf("error reading current root: %s", readRootErr.Error()) }
		
		rootBitMapAfterDelete := currRoot.Bitmap

		t.Logf("bitmap of root after deletes: %032b\n", rootBitMapAfterDelete)
		t.Logf("bitmap of root after deletes: %d\n", rootBitMapAfterDelete)

		t.Log("mmcmap after deletes")
		mmcMap.PrintChildren()

		expectedRootBitmapAfterDelete := uint32(536956102)
		t.Log("actual bitmap:", rootBitMapAfterDelete, "expected bitmap:", expectedRootBitmapAfterDelete)
		if expectedRootBitmapAfterDelete != rootBitMapAfterDelete {
			t.Errorf("actual bitmap does not match expected bitmap: actual(%032b), expected(%032b)\n", rootBitMapAfterDelete, expectedRootBitmapAfterDelete)
		}
	})
}