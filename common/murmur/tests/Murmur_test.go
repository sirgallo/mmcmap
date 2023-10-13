package murmurtests

import "testing"

import "github.com/sirgallo/mmcmap/common/murmur"


func TestMurmur(t *testing.T) {
	t.Run("Test Hashing", func(t *testing.T) {
		key := []byte("hello")
		seed := uint32(1)
	
		hash := murmur.Murmur32(key, seed)
		t.Log("hash:", hash)
	})
}