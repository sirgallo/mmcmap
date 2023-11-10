package mmcmaptests

import "bytes"
import "fmt"
import "os"
import "path/filepath"
import "testing"

import "github.com/sirgallo/mmcmap"


var stTestPath = filepath.Join(os.TempDir(), "testsinglethread")
var stConcurrentTestMap *mmcmap.MMCMap
var stkeyValPairs []KeyVal
var stInitMMCMapErr error


func init() {
	os.Remove(stTestPath)
	
	opts := mmcmap.MMCMapOpts{ Filepath: stTestPath }
	stConcurrentTestMap, stInitMMCMapErr = mmcmap.Open(opts)
	if stInitMMCMapErr != nil {
		stConcurrentTestMap.Remove()
		panic(stInitMMCMapErr.Error())
	}

	fmt.Println("concurrent test mmcmap initialized")

	stkeyValPairs = make([]KeyVal, INPUT_SIZE)

	for idx := range stkeyValPairs {
		randomBytes, _ := GenerateRandomBytes(32)
		stkeyValPairs[idx] = KeyVal{ Key: randomBytes, Value: randomBytes }
	}
}


func TestMMCMapSingleThreadOperations(t *testing.T) {
	defer stConcurrentTestMap.Remove()

	t.Run("Test Write Operations", func(t *testing.T) {
		for _, val := range stkeyValPairs {
			_, putErr := stConcurrentTestMap.Put(val.Key, val.Value)
			if putErr != nil { t.Errorf("error on mmcmap put: %s", putErr.Error()) }
		}
	})

	t.Run("Test Read Operations", func(t *testing.T) {
		defer stConcurrentTestMap.Close()
		
		for _, val := range stkeyValPairs {
			kvPair, getErr := stConcurrentTestMap.Get(val.Key)
			if getErr != nil { t.Errorf("error on mmcmap get: %s", getErr.Error()) }

			if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
				t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
			}
		}
	})

	t.Run("Test Read Operations After Reopen", func(t *testing.T) {
		opts := mmcmap.MMCMapOpts{ Filepath: stTestPath }
		
		stConcurrentTestMap, stInitMMCMapErr = mmcmap.Open(opts)
		if stInitMMCMapErr != nil {
			stConcurrentTestMap.Remove()
			t.Error("unable to open file")
		}

		for _, val := range stkeyValPairs {
			kvPair, getErr := stConcurrentTestMap.Get(val.Key)
			if getErr != nil { t.Errorf("error on mmcmap get: %s", getErr.Error()) }

			if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
				t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
			}
		}
	})

	t.Run("Test Delete Operations", func(t *testing.T) {
		for _, val := range stkeyValPairs {
			_, delErr := stConcurrentTestMap.Delete(val.Key)
			if delErr != nil { t.Errorf("error on mmcmap delete: %s", delErr.Error()) }
		}
	})

	t.Run("MMCMap File Size", func(t *testing.T) {
		fSize, sizeErr := stConcurrentTestMap.FileSize()
		if sizeErr != nil { t.Errorf("error getting file size: %s", sizeErr.Error()) }

		t.Log("File Size In Bytes:", fSize)
	})

	t.Log("Done")
}