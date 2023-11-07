package mmcmaptests

import "bytes"
import "fmt"
import "os"
import "path/filepath"
import "sync"
import "testing"

import "github.com/sirgallo/mmcmap"


var pTestPath = filepath.Join(os.TempDir(), "testparallel")
var parallelTestMap *mmcmap.MMCMap
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

	fmt.Println("parallel test mmcmap initialized")

	initKeyValPairs = make([]KeyVal, INPUT_SIZE)
	pKeyValPairs = make([]KeyVal, PWRITE_INPUT_SIZE)

	for idx := range initKeyValPairs {
		iRandomBytes, _ := GenerateRandomBytes(32)
		initKeyValPairs[idx] = KeyVal{ Key: iRandomBytes, Value: iRandomBytes }
	}

	for idx := range pKeyValPairs {
		pRandomBytes, _ := GenerateRandomBytes(32)
		pKeyValPairs[idx] = KeyVal{ Key: pRandomBytes, Value: pRandomBytes }
	}

	fmt.Println("seeding parallel test mmcmap")

	for _, val := range initKeyValPairs {
		initMapWG.Add(1)
		go func(val KeyVal) {
			defer initMapWG.Done()

			_, putErr := parallelTestMap.Put(val.Key, val.Value)
			if putErr != nil { panic(putErr.Error()) }
		}(val)
	}

	initMapWG.Wait()

	fmt.Println("finished seeding parallel test mmcmap")
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

		readData := initKeyValPairs[:len(initKeyValPairs) - PWRITE_INPUT_SIZE]

		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := readData[i * PCHUNK_SIZE_READ:(i + 1) * PCHUNK_SIZE_READ]

			pRetrieveWG.Add(1)
			go func() {
				defer pRetrieveWG.Done()

				for _, val := range chunk {
					value, getErr := parallelTestMap.Get(val.Key)
					if getErr != nil { t.Errorf("error on mmcmap get: %s", getErr.Error()) }

					if ! bytes.Equal(value, val.Value) {
						t.Errorf("actual value not equal to expected: actual(%s), expected(%s)", value, val.Value)
					}
				}
			}()
		}

		pRetrieveWG.Wait()
	})

	t.Run("Test Write New Key Vals In MMap", func(t *testing.T) {
		t.Parallel()

		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			chunk := pKeyValPairs[i * PCHUNK_SIZE_WRITE:(i + 1) * PCHUNK_SIZE_WRITE]

			pInsertWG.Add(1)
			go func() {
				defer pInsertWG.Done()

				for _, val := range chunk {
					_, putErr := parallelTestMap.Put(val.Key, val.Value)
					if putErr != nil { t.Errorf("error on mmcmap put: %s", putErr.Error()) }
				}
			}()
		}

		pInsertWG.Wait()
	})
}