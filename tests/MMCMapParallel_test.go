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

	pInputSize = 500000
	pKeyValPairs = make([]KeyVal, pInputSize)
	initKeyValPairs = make([]KeyVal, pInputSize)

	for idx := range pKeyValPairs {
		iRandomBytes, _ := GenerateRandomBytes(32)
		pRandomBytes, _ := GenerateRandomBytes(32)
		
		initKeyValPairs[idx] = KeyVal{ Key: iRandomBytes, Value: iRandomBytes }
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
	defer parallelTestMap.Remove()
	
	t.Run("test read init key vals in map", func(t *testing.T) {
		t.Parallel()

		var retrieveWG sync.WaitGroup

		for _, val := range initKeyValPairs {
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