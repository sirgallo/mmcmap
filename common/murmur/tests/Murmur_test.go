package murmurtests

import "testing"

import "github.com/sirgallo/pcmap/common/murmur"


func TestMurmur32(t *testing.T) {
	key := []byte("hello")
	seed := uint32(1)

	hash := murmur.Murmur32(key, seed)
	t.Log("hash:", hash)
}