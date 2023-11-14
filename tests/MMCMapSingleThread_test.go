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

		// stConcurrentTestMap.PrintChildren()
	})

	t.Run("Test Read Operations", func(t *testing.T) {
		defer stConcurrentTestMap.Close()
		
		for _, val := range stkeyValPairs {
			kvPair, getErr := stConcurrentTestMap.Get(val.Key)
			if getErr != nil { t.Errorf("error on mmcmap get: %s", getErr.Error()) }
			
			if ! bytes.Equal(kvPair.Value, val.Value) {
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

	t.Run("Test Range Operation", func(t *testing.T) {
		first, second, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
		if randomErr != nil { t.Error("error generating random min max") }

		var start, end []byte
		switch {
			case bytes.Compare(stkeyValPairs[first].Key, stkeyValPairs[second].Key) == 1:
				start = stkeyValPairs[second].Key
				end = stkeyValPairs[first].Key
			default:
				start = stkeyValPairs[first].Key
				end = stkeyValPairs[second].Key
		}

		kvPairs, rangeErr := stConcurrentTestMap.Range(start, end, nil)
		if rangeErr != nil { t.Errorf("error on mmcmap get: %s", rangeErr.Error()) }
		
		t.Log("len kvPairs", len(kvPairs))
		
		isSorted := IsSorted(kvPairs)
		if ! isSorted { t.Errorf("key value pairs are not in sorted order1: %t", isSorted) }
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