package mmcmaptests

import "crypto/rand"


const NUM_WRITER_GO_ROUTINES = 2
const NUM_READER_GO_ROUTINES = 1000000
const INPUT_SIZE = 1000000
const PWRITE_INPUT_SIZE = INPUT_SIZE / 5
const WRITE_CHUNK_SIZE = INPUT_SIZE / NUM_WRITER_GO_ROUTINES
const READ_CHUNK_SIZE = INPUT_SIZE / NUM_READER_GO_ROUTINES
const PCHUNK_SIZE_READ = (INPUT_SIZE - PWRITE_INPUT_SIZE) / NUM_READER_GO_ROUTINES
const PCHUNK_SIZE_WRITE = PWRITE_INPUT_SIZE / NUM_WRITER_GO_ROUTINES


type KeyVal struct {
	Key   []byte
	Value []byte
}


func GenerateRandomBytes(length int) ([]byte, error) {
	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil { return nil, err }

	for i := 0; i < length; i++ {
		randomBytes[i] = 'a' + (randomBytes[i] % 26)
	}

	return randomBytes, nil
}