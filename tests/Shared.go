package mmcmaptests

import "crypto/rand"


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