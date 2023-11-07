package murmur

import "encoding/binary"


//============================================= Murmur32


// Murmur32
//	The Murmur32 non-cryptographic hash function.
func Murmur32(data []byte, seed uint32) uint32 {
	hash := seed
	
	length := uint32(len(data))
	total4ByteChunks := len(data) / 4
	
	for idx := range make([]int, total4ByteChunks) {
		startIdxOfChunk := idx * 4 
		endIdxOfChunk := (idx + 1) * 4
		chunk := binary.LittleEndian.Uint32(data[startIdxOfChunk:endIdxOfChunk])

		rotateRight32(&hash, chunk)
	}

	handleRemainingBytes32(&hash, data)

	hash ^= length
	hash ^= hash >> 16
	hash *= c32_4
	hash ^= hash >> 13
	hash *= c32_5
	hash ^= hash >> 16

	return hash
}

// rotateRight32
//	For each 4-byte chunk, a series of rotations, mixings, and XOR operations are applied.
func rotateRight32(hash *uint32, chunk uint32) {
	chunk *= c32_1
	chunk = (chunk << 15) | (chunk >> 17) // Rotate right by 15
	chunk *= c32_2

	*hash ^= chunk
	*hash = (*hash << 13) | (*hash >> 19) // Rotate right by 13
	*hash = *hash * 5 + c32_3
}

// handleRemainingBytes32
//	If there are any remaining bytes that are not a chunk of 4, perform mixing and rotating on these chunks.
func handleRemainingBytes32(hash *uint32, dataAsBytes []byte) {
	remaining := dataAsBytes[len(dataAsBytes)-len(dataAsBytes) % 4:]
	
	if len(remaining) > 0 {
		var chunk uint32
		
		switch len(remaining) {
			case 3:
				chunk |= uint32(remaining[2]) << 16
				fallthrough
			case 2:
				chunk |= uint32(remaining[1]) << 8
				fallthrough
			case 1:
				chunk |= uint32(remaining[0])
				chunk *= c32_1
				chunk = (chunk << 15) | (chunk >> 17) // Rotate right by 15
				chunk *= c32_2
				*hash ^= chunk
			}
	}
}