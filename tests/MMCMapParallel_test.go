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
var initMapWG, pInsertWG, pRetrieveWG sync.WaitGroup


func setup() {
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

func cleanup() {
	parallelTestMap.Remove()
}


func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	cleanup()

	os.Exit(code)
}

func TestMMCMapParallelReadWrites(t *testing.T) {
	t.Run("Test Read Init Key Vals In MMap", func(t *testing.T) {
		t.Parallel()

		for _, val := range initKeyValPairs[:len(initKeyValPairs) - pInputSize / 5] {
			pRetrieveWG.Add(1)
			go func(val KeyVal) {
				defer pRetrieveWG.Done()

				value, getErr := parallelTestMap.Get(val.Key)
				if getErr != nil { t.Errorf("error on mmcmap get: %s", getErr.Error()) }

				if ! bytes.Equal(value, val.Value) {
					t.Errorf("actual value not equal to expected: actual(%s), expected(%s)", value, val.Value)
				}
			}(val)
		}

		pRetrieveWG.Wait()
	})

	t.Run("Test Write New Key Vals In MMap", func(t *testing.T) {
		t.Parallel()

		for _, val := range pKeyValPairs {
			pInsertWG.Add(1)
			go func(val KeyVal) {
				defer pInsertWG.Done()

				_, putErr := parallelTestMap.Put(val.Key, val.Value)
				if putErr != nil { t.Errorf("error on mmcmap put: %s", putErr.Error()) }
			}(val)
		}

		pInsertWG.Wait()
	})
}