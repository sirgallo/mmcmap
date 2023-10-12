package pcmaptests

import "bytes"
import "crypto/rand"
import "os"
import "path/filepath"
import "sync"
import "testing"

import "github.com/sirgallo/pcmap"


type KeyVal struct {
	Key []byte
	Value []byte
}


var cTestPath = filepath.Join(os.TempDir(), "testconcurrent")
var concurrentPcMap *pcmap.PCMap


func init() {
	opts := pcmap.PCMapOpts{ Filepath: cTestPath }

	var initPCMapErr error
	concurrentPcMap, initPCMapErr = pcmap.Open(opts)
	if initPCMapErr != nil { panic(initPCMapErr.Error()) }
}


func TestMapRandomSmallConcurrentOperations(t *testing.T) {
	defer concurrentPcMap.Remove()

	inputSize := 100000
	keyValPairs := make([]KeyVal, inputSize)

	for idx := range keyValPairs {
		randomBytes, _ := GenerateRandomBytes(32)
		keyValPairs[idx] = KeyVal{ Key: randomBytes, Value: randomBytes }
	}

	t.Log("seeded keyValPairs array:", inputSize)

	t.Log("inserting values -->")

	var insertWG sync.WaitGroup

	for _, val := range keyValPairs {
		insertWG.Add(1)
		go func (val KeyVal) {
			defer insertWG.Done()

			_, putErr := concurrentPcMap.Put(val.Key, val.Value)
			if putErr != nil { t.Errorf("error on pcmap put: %s", putErr.Error()) }
		}(val)
	}

	insertWG.Wait()

	t.Log("retrieving values -->")

	var retrieveWG sync.WaitGroup

	for _, val := range keyValPairs {
		retrieveWG.Add(1)
		go func (val KeyVal) {
			defer retrieveWG.Done()

			value, getErr := concurrentPcMap.Get(val.Key)
			if getErr != nil { t.Errorf("error on pcmap get: %s", getErr.Error()) }

			// t.Logf("actual: %s, expected: %s", value, val.Value)
			if ! bytes.Equal(value, val.Value) {
				t.Errorf("actual value not equal to expected: actual(%s), expected(%s)", value, val.Value)
			}
		}(val)
	}

	retrieveWG.Wait()

	t.Log("done")
}


//=================================== helper

func GenerateRandomBytes(length int) ([]byte, error) {
	randomBytes := make([]byte, length)

	_, err := rand.Read(randomBytes)
	if err != nil {
		return nil, err
	}

	for i := 0; i < length; i++ {
		randomBytes[i] = 'a' + (randomBytes[i] % 26)
	}

	return randomBytes, nil
}