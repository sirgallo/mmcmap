package mmcmap

import "fmt"
import "math"
import "math/bits"
import "sync/atomic"
import "unsafe"

import "github.com/sirgallo/mmcmap/common/mmap"
import "github.com/sirgallo/mmcmap/common/murmur"


//============================================= MMCMap Utilities


// CalculateHashForCurrentLevel
//	Calculates the hash for value based on what level of the trie the operation is at.
//	Hash is reseeded every 6 levels.
//
// Parameters:
//	key: the key for a key-value pair within the hamt
//	level: the current level within the hamt that the operation is at
//
// Returns:
//	The 32 bit representation of the key
func (mmcMap *MMCMap) CalculateHashForCurrentLevel(key []byte, level int) uint32 {
	currChunk := level / mmcMap.HashChunks
	seed := uint32(currChunk + 1)
	return murmur.Murmur32(key, seed)
}

// getSparseIndex
//	Gets the index at a particular level in the trie. Pass through function.
//
// Parameters:
//	hash: the hash representation of the incoming key
//	level: the level within the trie the operation is at
//
// Returns:
//	The index the key is located at
func (mmcMap *MMCMap) getSparseIndex(hash uint32, level int) int {
	return GetIndexForLevel(hash, mmcMap.BitChunkSize, level, mmcMap.HashChunks)
}

// getPosition
//	Calculates the position in the child node array based on the sparse index and the current bitmap of internal node.
//	The sparse index is calculated using the hash and bitchunk size.
//	A mask is calculated by performing a bitwise left shift operation, which shifts the binary representation of the value 1 the number of positions associated with the sparse index value and then subtracts 1.
//	This creates a binary number with all 1s to the right sparse index positions.
//	The mask is then applied the bitmap and the resulting isolated bits are the 1s right of the sparse index. 
//	The hamming weight, or total bits right of the sparse index, is then calculated.
//
// Parameters:
//	bitMap: the bitmap in the current node where the operation is occuring
//	hash: the hash representation of the current key for the operation
//	level: the level within the hamt that current operation is occuring
//
// Returns:
//	The position in the child node array of the current node
func (mmcMap *MMCMap) getPosition(bitMap uint32, hash uint32, level int) int {
	sparseIdx := GetIndexForLevel(hash, mmcMap.BitChunkSize, level, mmcMap.HashChunks)

	mask := uint32((1 << sparseIdx) - 1)
	isolatedBits := bitMap & mask
	return CalculateHammingWeight(isolatedBits)
}

// GetIndexForLevel
//	Determines the local level for a hash at a particular seed.
//
// Parameters:
//	hash: the incoming hashed key
//	chunkSize: the chunkSize of the hash, so 5 for 32 bit
//	hashChunks: the total hash chunks, which determines the number of levels before a reseed. This is 6 for 32 bit
//
// Returns:
//	The sparse index for the level
func GetIndexForLevel(hash uint32, chunkSize int, level int, hashChunks int) int {
	updatedLevel := level % hashChunks
	return GetIndex(hash, chunkSize, updatedLevel)
}

// GetIndex
//	Gets the index at a particular level in the trie by shifting the hash over the chunk size t (5 for 32 bits).
//	Apply a mask to the shifted hash to return an index mapped in the sparse index.
//	Non-zero values in the sparse index represent indexes where nodes are populated. 
//	The mask is the value 31 in binary form.
//
// Parameters:
//	hash: the incoming hashed key to determine the location in the index
//	chunkSize: the bit chunk size of the hash. Will be 5 bits for a 32 bit hash
//	level: the current level the operation is at
//
// Return:
//	The index in the sparse index represented by the bitmap
func GetIndex(hash uint32, chunkSize int, level int) int {
	slots := int(math.Pow(float64(2), float64(chunkSize)))
	shiftSize := slots - (chunkSize * (level + 1))

	mask := uint32(slots - 1)
	return int(hash >> shiftSize & mask)
}

// CalculateHammingWeight
//	Determines the total number of 1s in the binary representation of a number. 0s are ignored.
//
// Parameters:
//	bitmap: the isolated bits from the bitmap, which is the bits right of the position of the index
//
// Returns:
//	Total number of 1s in the isolated bits from the bit map
func CalculateHammingWeight(bitmap uint32) int {
	return bits.OnesCount32(bitmap)
}

// SetBit
//	Performs a logical xor operation on the current bitmap and the a 32 bit value where the value is all 0s except for at the position of the incoming index.
//	Essentially flips the bit if incoming is 1 and bitmap is 0 at that position, or 0 to 1. 
//	If 0 and 0 or 1 and 1, bitmap is not changed.
//
// Parameters:
//	bitmap: the incoming bitmap from the current node
//	position: the position of the index calculated at the current level for the incoming key
//
// Returns:
//	The updated bitmap with the index of the hashed key
func SetBit(bitmap uint32, position int) uint32 {
	return bitmap ^ (1 << position)
}

// IsBitSet
//	Determines whether or not a bit is set in a bitmap by taking the bitmap and applying a mask with a 1 at the position in the bitmap to check.
//	A logical and operation is applied and if the value is not equal to 0, then the bit is set.
//
// Parameters:
//	bitmap: the bitmap of the current node
//	position: the position in the bitmap where the index of the hashed key is
//
// Returns:
//	Whether the bit is set or not
func IsBitSet(bitmap uint32, position int) bool {
	return (bitmap & (1 << position)) != 0
}

// ExtendTable
//	Utility function for dynamically expanding the child node array if a bit is set and a value needs to be inserted into the array.
//
// Parameters:
//	orig: the original child node array
//	bitmap: the current bitmap of the node where the array is being extended
//	pos: the position in the array where the new node is being inserted
//	newNode: the new node being inserted
//
// Returns:
//	The updated child node array
func ExtendTable(orig []*MMCMapNode, bitMap uint32, pos int, newNode *MMCMapNode) []*MMCMapNode {
	tableSize := CalculateHammingWeight(bitMap)
	newTable := make([]*MMCMapNode, tableSize)

	copy(newTable[:pos], orig[:pos])
	newTable[pos] = newNode
	copy(newTable[pos + 1:], orig[pos:])
	return newTable
}

// ShrinkTable
//	Inverse of the ExtendTable utility function.
//	It dynamically shrinks a table by removing an element at a given position.
//
// Parameters:
//	orig: the original child node array
//	bitmap: the bitmap in the node for indexes of the keys
//	pos: the position of the value to be removed
//
// Returns:
//	The updated child node array
func ShrinkTable(orig []*MMCMapNode, bitMap uint32, pos int) []*MMCMapNode {
	tableSize := CalculateHammingWeight(bitMap)
	newTable := make([]*MMCMapNode, tableSize)

	copy(newTable[:pos], orig[:pos])
	copy(newTable[pos:], orig[pos + 1:])
	return newTable
}

// Print Children
//	Debugging function for printing nodes in the hash array mapped trie.
func (mmcMap *MMCMap) PrintChildren() error {
	mMap := mmcMap.Data.Load().(mmap.MMap)

	rootOffsetPtr := (*uint64)(unsafe.Pointer(&mMap[MetaRootOffsetIdx]))
	rootOffset := atomic.LoadUint64(rootOffsetPtr)

	currRoot, readRootErr := mmcMap.ReadNodeFromMemMap(rootOffset)
	if readRootErr != nil { return readRootErr }

	readChildrenErr := mmcMap.printChildrenRecursive(currRoot, 0)
	if readChildrenErr != nil { return readChildrenErr }

	return nil
}

func (mmcMap *MMCMap) printChildrenRecursive(node *MMCMapNode, level int) error {
	if node == nil { return nil }

	for idx := range node.Children {
		childPtr := node.Children[idx]

		child, desErr := mmcMap.ReadNodeFromMemMap(childPtr.StartOffset)
		if desErr != nil { return desErr }

		if child != nil {
			fmt.Printf("Level: %d, Index: %d, Key: %d, Value: %d\n", level, idx, child.Key, child.Value)
			mmcMap.printChildrenRecursive(child, level + 1)
		}
	}

	return nil
}