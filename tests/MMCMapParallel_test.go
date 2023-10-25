package mmcmaptests

import "bytes"
import "os"
import "path/filepath"
import "sync"
import "testing"


import "github.com/sirgallo/mmcmap"


var pTestPath = filepath.Join(os.TempDir(), "testparallel")
var parallelTestMap *mmcmap.MMCMap
var pInputSize int
var initKeyValPairs []KeyVal
var pKeyValPairs []KeyVal
var pInitMMCMapErr error


func init() {
	os.Remove(pTestPath)
	opts := mmcmap.MMCMapOpts{ Filepath: pTestPath }
	
	parallelTestMap, pInitMMCMapErr = mmcmap.Open(opts)
	if pInitMMCMapErr != nil {
		parallelTestMap.Remove()
		panic(pInitMMCMapErr.Error())
	}

	pInputSize = 1000000
	initKeyValPairs = make([]KeyVal, pInputSize)
	pKeyValPairs = make([]KeyVal, pInputSize / 5)

	for idx := range initKeyValPairs {
		iRandomBytes, _ := GenerateRandomBytes(32)
		initKeyValPairs[idx] = KeyVal{ Key: iRandomBytes, Value: iRandomBytes }
	}

	for idx := range pKeyValPairs {
		pRandomBytes, _ := GenerateRandomBytes(32)
		pKeyValPairs[idx] = KeyVal{ Key: pRandomBytes, Value: pRandomBytes }
	}

	var initMapWG sync.WaitGroup

	for _, val := range initKeyValPairs {
		initMapWG.Add(1)
		go func(val KeyVal) {
			defer initMapWG.Done()

			_, putErr := parallelTestMap.Put(val.Key, val.Value)
			if putErr != nil { panic(putErr.Error()) }
		}(val)
	}

	initMapWG.Wait()
}


func TestMMCMapParallelReadWrites(t *testing.T) {
	t.Run("test read init key vals in map", func(t *testing.T) {
		t.Parallel()

		var retrieveWG sync.WaitGroup

		for _, val := range initKeyValPairs[:len(initKeyValPairs) - pInputSize / 5] {
			retrieveWG.Add(1)
			go func(val KeyVal) {
				defer retrieveWG.Done()

				value, getErr := parallelTestMap.Get(val.Key)
				if getErr != nil { t.Errorf("error on mmcmap get: %s", getErr.Error()) }

				if ! bytes.Equal(value, val.Value) {
					t.Errorf("actual value not equal to expected: actual(%s), expected(%s)", value, val.Value)
				}
			}(val)
		}

		retrieveWG.Wait()
	})

	t.Run("test write new key vals in map", func(t *testing.T) {
		t.Parallel()

		var insertWG sync.WaitGroup

		for _, val := range pKeyValPairs {
			insertWG.Add(1)
			go func(val KeyVal) {
				defer insertWG.Done()

				_, putErr := parallelTestMap.Put(val.Key, val.Value)
				if putErr != nil { t.Errorf("error on mmcmap put: %s", putErr.Error()) }
			}(val)
		}

		insertWG.Wait()
	})

	t.Log("Done")
}