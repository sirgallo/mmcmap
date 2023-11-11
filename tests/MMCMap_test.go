package mmcmaptests

import "os"
import "fmt"
import "path/filepath"
// import "sync/atomic"
import "testing"
// import "unsafe"

import "github.com/sirgallo/mmcmap"
// import "github.com/sirgallo/mmcmap/common/mmap"


var TestPath = filepath.Join(os.TempDir(), "testmmcmap")
var mmcMap *mmcmap.MMCMap


func init() {
	var initPCMapErr error
	os.Remove(TestPath)
	
	opts := mmcmap.MMCMapOpts{ Filepath: TestPath }
	mmcMap, initPCMapErr = mmcmap.Open(opts)
	if initPCMapErr != nil { panic(initPCMapErr.Error()) }

	fmt.Println("op test mmcmap initialized")
}


func TestMMCMap(t *testing.T) {
	defer mmcMap.Remove()

	// var currRoot *mmcmap.MMCMapINode
	// var delErr, getErr, readRootErr, putErr error

	var delErr, getErr, putErr error
	var val1, val2, val3, val4 *mmcmap.KeyValuePair

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

		/*
		_, putErr = mmcMap.Put([]byte("06"), []byte("wow!"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }
		*/

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
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		_, putErr = mmcMap.Put([]byte("woah"), []byte("done"))
		if putErr != nil { t.Errorf("error putting key in mmcmap: %s", putErr.Error()) }

		// mMap := mmcMap.Data.Load().(mmap.MMap)
		
		// rootOffsetPtr := (*uint64)(unsafe.Pointer(&mMap[mmcmap.MetaRootOffsetIdx]))
		//rootOffset := atomic.LoadUint64(rootOffsetPtr)
		
		// currRoot, readRootErr = mmcMap.ReadINodeFromMemMap(rootOffset)
		// if readRootErr != nil { t.Errorf("error reading current root: %s", readRootErr.Error()) }
		
		// rootBitMap := currRoot.Bitmap

		t.Logf("mmcMap after inserts")
		mmcMap.PrintChildren()

		/*
		expectedBitMap := uint32(542198999)
		t.Logf("actual root bitmap: %d, expected root bitmap: %d\n", rootBitMap, expectedBitMap)
		t.Logf("actual root bitmap: %032b, expected root bitmap: %032b\n", rootBitMap, expectedBitMap)
		if expectedBitMap != rootBitMap { 
			t.Errorf("actual bitmap does not match expected bitmap: actual(%032b), expected(%032b)\n", rootBitMap, expectedBitMap)
		}
		*/
	})

	t.Run("Test MMCMap Get", func(t *testing.T) {
		expVal1 := "world"
		val1, getErr = mmcMap.Get([]byte("hello"))
		if getErr != nil { t.Errorf("error getting val1: %s", getErr.Error()) }
		if val1 == nil { t.Error("val actually nil") }

		t.Logf("actual: %s, expected: %s", string(val1.Value), expVal1)
		if string(val1.Value) != expVal1 { t.Errorf("val 1 does not match expected val 1: actual(%s), expected(%s)\n", val1.Value, expVal1) }

		expVal2 := "wow!"
		val2, getErr = mmcMap.Get([]byte("new"))
		if getErr != nil { t.Errorf("error getting val2: %s", getErr.Error()) }
		if val2 == nil { t.Error("val actually nil") }

		t.Logf("actual: %s, expected: %s", val2.Value, expVal2)
		if string(val2.Value) != expVal2 { t.Errorf("val 2 does not match expected val 2: actual(%s), expected(%s)\n", val2.Value, expVal2) }

		expVal3 := "hello"
		val3, getErr = mmcMap.Get([]byte("asdf"))
		if getErr != nil { t.Errorf("error getting val3: %s", getErr.Error()) }
		if val3 == nil { t.Error("val actually nil") }
		
		t.Logf("actual: %s, expected: %s", val3.Value, expVal3)
		if string(val3.Value) != expVal3 { t.Errorf("val 3 does not match expected val 3: actual(%s), expected(%s)", val3.Value, expVal3) }

		expVal4 := "123123"
		val4, getErr = mmcMap.Get([]byte("asdfasdf"))
		if getErr != nil { t.Errorf("error getting val4: %s", getErr.Error()) }
		if val4 == nil { t.Error("val actually nil") }

		t.Logf("actual: %s, expected: %s", val4.Value, expVal4)
		if string(val4.Value) != expVal4 { t.Errorf("val 4 does not match expected val 4: actual(%s), expected(%s)", val4.Value, expVal4) }
	})

	t.Run("Test Range Operation", func(t *testing.T) {
		kvPairs, getErr := mmcMap.Range([]byte("hello"), []byte("yup"), nil)
		if getErr != nil { t.Errorf("error on mmcmap range: %s", getErr.Error()) }

		t.Log("keys in kv pairs", func() []string{
			var keys []string
			for _, kv := range kvPairs { 
				keys = append(keys, string(kv.Key))
			}

			return keys
		}())

		isSorted := IsSorted(kvPairs)
		t.Logf("is sorted: %t", isSorted)

		if ! isSorted {
			t.Errorf("key value pairs are not in sorted order: %t", isSorted)
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
		if delErr != nil { t.Errorf("error deleting key from mmcmap: %s", delErr.Error()) }

		/*
		_, delErr = mmcMap.Delete([]byte("06"))
		if delErr != nil { t.Errorf("error deleting key from mmcmap: %s", delErr.Error()) }
		*/

		/*
		mMap := mmcMap.Data.Load().(mmap.MMap)

		rootOffsetPtr := (*uint64)(unsafe.Pointer(&mMap[mmcmap.MetaRootOffsetIdx]))
		rootOffset := atomic.LoadUint64(rootOffsetPtr)

		currRoot, readRootErr = mmcMap.ReadINodeFromMemMap(rootOffset)
		if readRootErr != nil { t.Errorf("error reading current root: %s", readRootErr.Error()) }
		
		rootBitMapAfterDelete := currRoot.Bitmap

		t.Logf("bitmap of root after deletes: %032b\n", rootBitMapAfterDelete)
		t.Logf("bitmap of root after deletes: %d\n", rootBitMapAfterDelete)
		*/

		t.Log("mmcmap after deletes")
		mmcMap.PrintChildren()

		/*
		expectedRootBitmapAfterDelete := uint32(536956102)
		t.Log("actual bitmap:", rootBitMapAfterDelete, "expected bitmap:", expectedRootBitmapAfterDelete)
		if expectedRootBitmapAfterDelete != rootBitMapAfterDelete {
			t.Errorf("actual bitmap does not match expected bitmap: actual(%032b), expected(%032b)\n", rootBitMapAfterDelete, expectedRootBitmapAfterDelete)
		}
		*/
	})

	t.Log("Done")
}