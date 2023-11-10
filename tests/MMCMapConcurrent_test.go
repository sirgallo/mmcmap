package mmcmaptests

import "bytes"
import "fmt"
import "os"
import "path/filepath"
import "sync"
import "testing"

import "github.com/sirgallo/mmcmap"


var cTestPath = filepath.Join(os.TempDir(), "testconcurrent")
var concurrentTestMap *mmcmap.MMCMap
var keyValPairs []KeyVal
var initMMCMapErr error
var delWG, insertWG, retrieveWG, rangeWG sync.WaitGroup


func init() {
	os.Remove(cTestPath)
	
	opts := mmcmap.MMCMapOpts{ Filepath: cTestPath }
	concurrentTestMap, initMMCMapErr = mmcmap.Open(opts)
	if initMMCMapErr != nil {
		concurrentTestMap.Remove()
		panic(initMMCMapErr.Error())
	}

	fmt.Println("concurrent test mmcmap initialized")

	keyValPairs = make([]KeyVal, INPUT_SIZE)

	for idx := range keyValPairs {
		randomBytes, _ := GenerateRandomBytes(32)
		keyValPairs[idx] = KeyVal{ Key: randomBytes, Value: randomBytes }
	}
}


func TestMMCMapConcurrentOperations(t *testing.T) {
	defer concurrentTestMap.Remove()

	t.Run("Test Write Operations", func(t *testing.T) {
		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			chunk := keyValPairs[i * WRITE_CHUNK_SIZE:(i + 1) * WRITE_CHUNK_SIZE]

			insertWG.Add(1)
			go func () {
				defer insertWG.Done()
					for _, val := range chunk {
						_, putErr := concurrentTestMap.Put(val.Key, val.Value)
						if putErr != nil { t.Errorf("error on mmcmap put: %s", putErr.Error()) }
					}
			}()
		}

		insertWG.Wait()
	})

	t.Run("Test Read Operations", func(t *testing.T) {
		defer concurrentTestMap.Close()
		
		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := keyValPairs[i * READ_CHUNK_SIZE:(i + 1) * READ_CHUNK_SIZE]
			
			retrieveWG.Add(1)
			go func() {
				defer retrieveWG.Done()

				for _, val := range chunk {
					kvPair, getErr := concurrentTestMap.Get(val.Key)
					if getErr != nil { t.Errorf("error on mmcmap get: %s", getErr.Error()) }

					if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
						t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
					}
				}
			}()
		}

		retrieveWG.Wait()
	})

	t.Run("Test Read Operations After Reopen", func(t *testing.T) {
		opts := mmcmap.MMCMapOpts{ Filepath: cTestPath }
		
		concurrentTestMap, initMMCMapErr = mmcmap.Open(opts)
		if initMMCMapErr != nil {
			concurrentTestMap.Remove()
			t.Error("unable to open file")
		}

		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := keyValPairs[i * READ_CHUNK_SIZE:(i + 1) * READ_CHUNK_SIZE]

			retrieveWG.Add(1)
			go func() {
				defer retrieveWG.Done()

				for _, val := range chunk {
					kvPair, getErr := concurrentTestMap.Get(val.Key)
					if getErr != nil { t.Errorf("error on mmcmap get: %s", getErr.Error()) }

					if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
						t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
					}
				}
			}()
		}

		retrieveWG.Wait()
	})

	t.Run("Test Range Operation", func(t *testing.T) {
		first := stkeyValPairs[(INPUT_SIZE / 2) - (INPUT_SIZE / 4)].Key
		second := stkeyValPairs[(INPUT_SIZE / 2) + (INPUT_SIZE / 4)].Key

		var start, end []byte
		switch {
			case bytes.Compare(first, second) == 1:
				start = second
				end = first
			default:
				start = first
				end = second
		}

		for range make([]int, NUM_READER_GO_ROUTINES) {
			rangeWG.Add(1)
			
			go func() {
				defer rangeWG.Done()

				kvPairs, rangeErr := concurrentTestMap.Range(start, end, nil)
				if rangeErr != nil { t.Errorf("error on mmcmap get: %s", rangeErr.Error()) }
				
				t.Log("len kvPairs", len(kvPairs))
				isSorted := IsSorted(kvPairs)
				if ! isSorted { t.Errorf("key value pairs are not in sorted order: %t", isSorted) }
			}()
		}
		
		rangeWG.Wait()
	})

	t.Run("Test Delete Operations", func(t *testing.T) {
		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			chunk := keyValPairs[i * WRITE_CHUNK_SIZE:(i + 1) * WRITE_CHUNK_SIZE]

			delWG.Add(1)
			go func() {
				defer delWG.Done()

				for _, val := range chunk {

					_, delErr := concurrentTestMap.Delete(val.Key)
					if delErr != nil { t.Errorf("error on mmcmap delete: %s", delErr.Error()) }
				}
			}()
		}

		delWG.Wait()
	})

	t.Run("MMCMap File Size", func(t *testing.T) {
		fSize, sizeErr := concurrentTestMap.FileSize()
		if sizeErr != nil { t.Errorf("error getting file size: %s", sizeErr.Error()) }

		t.Log("File Size In Bytes:", fSize)
	})

	t.Log("Done")
}