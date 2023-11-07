package mmcmap

import "fmt"
import "math"
import "math/bits"
import "sync/atomic"
import "unsafe"

import "github.com/sirgallo/mmcmap/common/mmap"
import "github.com/sirgallo/mmcmap/common/murmur"


//============================================= MMCMap Utilities


// GetIndex
//	Gets the index at a particular level in the trie by shifting the hash over the chunk size t (5 for 32 bits).
//	Apply a mask to the shifted hash to return an index mapped in the sparse index.
//	Non-zero values in the sparse index represent indexes where nodes are populated. 
//	The mask is the value 31 in binary form.
func GetIndex(hash uint32, chunkSize int, level int) int {
	slots := int(math.Pow(float64(2), float64(chunkSize)))
	shiftSize := slots - (chunkSize * (level + 1))

	mask := uint32(slots - 1)
	return int(hash >> shiftSize & mask)
}

// IsBitSet
//	Determines whether or not a bit is set in a bitmap by taking the bitmap and applying a mask with a 1 at the position in the bitmap to check.
//	A logical and operation is applied and if the value is not equal to 0, then the bit is set.
func IsBitSet(bitmap uint32, position int) bool {
	return (bitmap & (1 << position)) != 0
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

// SetBit
//	Performs a logical xor operation on the current bitmap and the a 32 bit value where the value is all 0s except for at the position of the incoming index.
//	Essentially flips the bit if incoming is 1 and bitmap is 0 at that position, or 0 to 1. 
//	If 0 and 0 or 1 and 1, bitmap is not changed.
func SetBit(bitmap uint32, position int) uint32 {
	return bitmap ^ (1 << position)
}

// CalculateHammingWeight
//	Determines the total number of 1s in the binary representation of a number. 0s are ignored.
func calculateHammingWeight(bitmap uint32) int {
	return bits.OnesCount32(bitmap)
}

// CalculateHashForCurrentLevel
//	Calculates the hash for value based on what level of the trie the operation is at.
//	Hash is reseeded every 6 levels.
func (mmcMap *MMCMap) calculateHashForCurrentLevel(key []byte, level int) uint32 {
	currChunk := level / mmcMap.HashChunks
	seed := uint32(currChunk + 1)
	return murmur.Murmur32(key, seed)
}

// extendTable
//	Utility function for dynamically expanding the child node array if a bit is set and a value needs to be inserted into the array.
func extendTable(orig []*MMCMapNode, bitMap uint32, pos int, newNode *MMCMapNode) []*MMCMapNode {
	tableSize := calculateHammingWeight(bitMap)
	newTable := make([]*MMCMapNode, tableSize)

	copy(newTable[:pos], orig[:pos])
	newTable[pos] = newNode
	copy(newTable[pos + 1:], orig[pos:])
	
	return newTable
}

// GetIndexForLevel
//	Determines the local level for a hash at a particular seed.
func getIndexForLevel(hash uint32, chunkSize int, level int, hashChunks int) int {
	updatedLevel := level % hashChunks
	return GetIndex(hash, chunkSize, updatedLevel)
}

// getPosition
//	Calculates the position in the child node array based on the sparse index and the current bitmap of internal node.
//	The sparse index is calculated using the hash and bitchunk size.
//	A mask is calculated by performing a bitwise left shift operation, which shifts the binary representation of the value 1 the number of positions associated with the sparse index value and then subtracts 1.
//	This creates a binary number with all 1s to the right sparse index positions.
//	The mask is then applied the bitmap and the resulting isolated bits are the 1s right of the sparse index. 
//	The hamming weight, or total bits right of the sparse index, is then calculated.
func (mmcMap *MMCMap) getPosition(bitMap uint32, hash uint32, level int) int {
	sparseIdx := getIndexForLevel(hash, mmcMap.BitChunkSize, level, mmcMap.HashChunks)

	mask := uint32((1 << sparseIdx) - 1)
	isolatedBits := bitMap & mask
	return calculateHammingWeight(isolatedBits)
}

// getSparseIndex
//	Gets the index at a particular level in the trie. 
//	Pass through function.
func (mmcMap *MMCMap) getSparseIndex(hash uint32, level int) int {
	return getIndexForLevel(hash, mmcMap.BitChunkSize, level, mmcMap.HashChunks)
}

// shrinkTable
//	Inverse of the extendTable utility function.
//	It dynamically shrinks a table by removing an element at a given position.
func shrinkTable(orig []*MMCMapNode, bitMap uint32, pos int) []*MMCMapNode {
	tableSize := calculateHammingWeight(bitMap)
	newTable := make([]*MMCMapNode, tableSize)

	copy(newTable[:pos], orig[:pos])
	copy(newTable[pos:], orig[pos + 1:])
	
	return newTable
}

// printChildrenRecursive
//	Recursively print nodes in the mmcmap as we traverse down levels.
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