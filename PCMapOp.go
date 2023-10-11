package pcmap

import "bytes"
import "fmt"
import "sync/atomic"
import "unsafe"


// Put inserts or updates key-value pair into the hash array mapped trie. 
//	The operation begins at the root of the trie and traverses through the tree until the correct location is found, copying the entire path. 
//	If the operation fails, the copied and modified path is discarded and the operation retries back at the root until completed.
//
// Parameters:
//	key: the key in the key-value pair
//	value: the value in the key-value pair
//
// Returns:
//	truthy on successful completion
func (pcMap *PCMap) Put(key []byte, value []byte) (bool, error) {
	pcMap.RWLock.Lock()
	defer pcMap.RWLock.Unlock()

	for {
		currMetaPtr := atomic.LoadPointer(&pcMap.Meta)
		currMeta := (*PCMapMetaData)(currMetaPtr)

		currRoot, readRootErr := pcMap.ReadNodeFromMemMap(currMeta.RootOffset)
		if readRootErr != nil { return false, readRootErr }
			
		currRoot.Version = currRoot.Version + 1
		rootPtr := unsafe.Pointer(currRoot)

		_, putErr := pcMap.putRecursive(&rootPtr, key, value, 0)
		if putErr != nil { return false, putErr }

		if currMetaPtr == atomic.LoadPointer(&pcMap.Meta) {
			updatedRootCopy := (*PCMapNode)(atomic.LoadPointer(&rootPtr))

			ok, writeErr := pcMap.ExclusiveWriteMmap(updatedRootCopy)
			if writeErr != nil { return false , writeErr }
			if ok { return true, nil }
		}
	}
}

// putRecursive attempts to traverse through the trie, locating the node at a given level to modify for the key-value pair. 
//	It first hashes the key, determines the sparse index in the bitmap to modify, and createsa copy of the current node to be modified. 
//	If the bit in the bitmap of the node is not set, a new leaf node is created, the bitmap of the copy is modified to reflect the position of the 
//	new leaf node, and the child node array is extended to include the new leaf node. 
// 	Then, an atomic compare and swap operation is performed where the operation attempts to replace the current node with the modified copy. 
//	If the operation succeeds the response is returned by moving back up the tree. If it fails, the copy is discarded and the operation returns to the root to be reattempted. 
//	If the current bit is set in the bitmap, the operation checks if the node at the location in the child node array is a leaf node or an internal node. 
//	If it is a leaf node and the key is the same as the incoming key, the copy is modified with the new value and we attempt to compare and swap the current child leaf node with the new copy. 
//	If the leaf node does not contain the same key, the operation creates a new internal node, and inserts the new leaf node for the incoming key and value as well as the existing child node into the new internal node.
//	Attempts to compare and swap the current leaf node with the new internal node containing the existing child node and the new leaf node for the incoming key and value. 
//	If the node is an internal node, the operation traverses down the tree to the internal node and the above steps are repeated until the key-value pair is inserted.
//
// Parameters:
//	node: the node in the tree where the operation is currently
//	key: the key for the incoming key-value pair
//	value: the value for the incoming key-value pair
//	level: the current level in the tree the operation is at
//
// Returns:
//	truthy value from successful or failed compare and swap operations
func (pcMap *PCMap) putRecursive(node *unsafe.Pointer, key []byte, value []byte, level int) (bool, error) {
	var childNode *PCMapNode
	var decErr, putErr error

	hash := pcMap.CalculateHashForCurrentLevel(key, level)
	index := pcMap.getSparseIndex(hash, level)

	currNode := (*PCMapNode)(atomic.LoadPointer(node))
	nodeCopy := pcMap.CopyNode(currNode)

	fmt.Println("node copy:", nodeCopy)

	if ! IsBitSet(nodeCopy.Bitmap, index) {		
		newLeaf := pcMap.NewLeafNode(key, value, nodeCopy.Version)
		nodeCopy.Bitmap = SetBit(nodeCopy.Bitmap, index)
		
		pos := pcMap.getPosition(nodeCopy.Bitmap, hash, level)
		nodeCopy.Children = ExtendTable(nodeCopy.Children, nodeCopy.Bitmap, pos, newLeaf)

		return pcMap.compareAndSwap(node, currNode, nodeCopy), nil
	} else {
		pos := pcMap.getPosition(nodeCopy.Bitmap, hash, level)
		childMMapPtr := nodeCopy.Children[pos]
		childNode, decErr = pcMap.ReadNodeFromMemMap(childMMapPtr.StartOffset)
		if decErr != nil { return false, decErr }
		
		childNode.Version = nodeCopy.Version

		if childNode.IsLeaf {
			if bytes.Equal(key, childNode.Key) {
				childNode.Value = value
				nodeCopy.Children[pos] =  childNode

				return pcMap.compareAndSwap(node, currNode, nodeCopy), nil
			} else {
				newINode := pcMap.NewInternalNode(nodeCopy.Version)
				iNodePtr := unsafe.Pointer(newINode)

				_, putErr = pcMap.putRecursive(&iNodePtr, childNode.Key, childNode.Value, level + 1)
				if putErr != nil { return false, putErr }

				_, putErr = pcMap.putRecursive(&iNodePtr, key, value, level + 1)
				if putErr != nil { return false, putErr }

				nodeCopy.Children[pos] = (*PCMapNode)(atomic.LoadPointer(&iNodePtr))
				return pcMap.compareAndSwap(node, currNode, nodeCopy), nil
			}
		} else {
			childPtr := unsafe.Pointer(childNode)

			_, putErr = pcMap.putRecursive(&childPtr, key, value, level + 1)
			if putErr != nil { return false, putErr }
			
			nodeCopy.Children[pos] = (*PCMapNode)(atomic.LoadPointer(&childPtr))
			return pcMap.compareAndSwap(node, currNode, nodeCopy), nil
		}
	}
}

// Get attempts to retrieve the value for a key within the hash array mapped trie. 
//	The operation begins at the root of the trie and traverses down the path to the key.
//
// Returns:
//	either the value for the key in byte array representation or nil if the key does not exist
func (pcMap *PCMap) Get(key []byte) ([]byte, error) {
	currMetaPtr := atomic.LoadPointer(&pcMap.Meta)
	currMeta := (*PCMapMetaData)(currMetaPtr)
	
	currRoot, readRootErr := pcMap.ReadNodeFromMemMap(currMeta.RootOffset)
	if readRootErr != nil { return nil, readRootErr }

	rootPtr := unsafe.Pointer(currRoot)
	return pcMap.getRecursive(&rootPtr, key, 0)
}

// getRecursive attempts to recursively retrieve a value for a given key within the hash array mapped trie. 
//	For each node traversed to at each level the operation travels to, the sparse index is calculated for the hashed key. 
//	If the bit is not set in the bitmap, return nil since the key has not been inserted yet into the trie. 
//	Otherwise, determine the position in the child node array for the sparse index. 
//	If the child node is a leaf node and the key to be searched for is the same as the key of the child node, the value has been found. 
//	Since the trie utilizes path copying, any threads modifying the trie are modifying copies so it the get operation returns the value at the point in time of the get operation. 
//	If the node is node a leaf node, but instead an internal node, recurse down the path to the next level to the child node in the position of the child node array and repeat the above.
//
// Parameters:
//	node: the pointer to the node to be checked for the key-value pair
//	key: the key being searched for
//	level: the current level within the trie the operation is at
//
// Returns:
//	either the value for the given key or nil if non-existent or if the node is being modified
func (pcMap *PCMap) getRecursive(node *unsafe.Pointer, key []byte, level int) ([]byte, error) {
	var childNode *PCMapNode
	var decErr error

	hash := pcMap.CalculateHashForCurrentLevel(key, level)
	index := pcMap.getSparseIndex(hash, level)

	currNode := (*PCMapNode)(atomic.LoadPointer(node))

	if ! IsBitSet(currNode.Bitmap, index) {
		return nil, nil
	} else {
		pos := pcMap.getPosition(currNode.Bitmap, hash, level)
		childMMapPtr := currNode.Children[pos]

		childNode, decErr = pcMap.ReadNodeFromMemMap(childMMapPtr.StartOffset)
		if decErr != nil { return nil, decErr }

		// fmt.Println("child node:", childNode)

		if childNode.IsLeaf && bytes.Equal(key, childNode.Key) {
			return childNode.Value, nil
		} else { 
			childPtr := unsafe.Pointer(childNode)
			return pcMap.getRecursive(&childPtr, key, level + 1) 
		}
	}
}

// Delete attempts to delete a key-value pair within the hash array mapped trie. 
//	It starts at the root of the trie and recurses down the path to the key to be deleted. If the operation succeeds truthy value is returned, otherwise the operation returns to the root to retry the operation.
//
// Parameters:
//	key: the key to attempt to delete
//
// Returns:
//	truthy on successful completion
func (pcMap *PCMap) Delete(key []byte) (bool, error) {
	for {
		currMetaPtr := atomic.LoadPointer(&pcMap.Meta)
		currMeta := (*PCMapMetaData)(currMetaPtr)
	
		currRoot, readRootErr := pcMap.ReadNodeFromMemMap(currMeta.RootOffset)
		if readRootErr != nil { return false, readRootErr }
			
		currRoot.Version = currRoot.Version + 1
		rootPtr := unsafe.Pointer(currRoot)

		_, putErr := pcMap.deleteRecursive(&rootPtr, key, 0)
		if putErr != nil { return false, putErr }

		if currMetaPtr == atomic.LoadPointer(&pcMap.Meta) {
			updatedRootCopy := (*PCMapNode)(atomic.LoadPointer(&rootPtr))

			ok, writeErr := pcMap.ExclusiveWriteMmap(updatedRootCopy)
			if writeErr != nil { return false, writeErr }
			if ok { return true, nil }
		}
	}
}

// deleteRecursive attempts to recursively move down the path of the trie to the key-value pair to be deleted. 
//	The hash for the key is calculated, the sparse index in the bitmap is determined for the given level, and a copy of the current node is created to be modifed. 
//	If the bit in the bitmap is not set, the key doesn't exist so truthy is returned since there is nothing to delete and the operation completes. 
//	If the bit is set, the child node for the position within the child node array is found. 
//	If the child node is a leaf node and the key of the child node is equal to the key of the key to delete, the copy is modified to update the bitmap and shrink the table and remove the given node. 
//	A compare and swap operation is performed, and if successful traverse back up the trie and complete, otherwise the operation is returned to the root to retry. 
//	If the child node is an internal node, the operation recurses down the trie to the next level. 
//	On return, if the internal node is empty, the copy modified so the bitmap is updated and table is shrunk. 
//	A compare and swap operation is performed on the current node with the new copy.
//
// Parameters:
//	node: a pointer to the node that is being modified
//	key: the key to be deleted
//	level: the current level within the trie
// 
// Returns:
//	truthy response on success and falsey on failure
func (pcMap *PCMap) deleteRecursive(node *unsafe.Pointer, key []byte, level int) (bool, error) {
	var childNode *PCMapNode
	var decErr, delErr error
	
	hash := pcMap.CalculateHashForCurrentLevel(key, level)
	index := pcMap.getSparseIndex(hash, level)

	currNode := (*PCMapNode)(atomic.LoadPointer(node))
	nodeCopy := pcMap.CopyNode(currNode)

	if ! IsBitSet(nodeCopy.Bitmap, index) {
		return true, nil
	} else {
		pos := pcMap.getPosition(nodeCopy.Bitmap, hash, level)
		childPtr := nodeCopy.Children[pos]

		childNode, decErr = pcMap.ReadNodeFromMemMap(childPtr.StartOffset)
		if decErr != nil { return false, decErr }

		childNode.Version = nodeCopy.Version

		if childNode.IsLeaf {
			if bytes.Equal(key, childNode.Key) { 
				nodeCopy.Bitmap = SetBit(nodeCopy.Bitmap, index)
				nodeCopy.Children = ShrinkTable(nodeCopy.Children, nodeCopy.Bitmap, pos)
				
				return pcMap.compareAndSwap(node, currNode, nodeCopy) , nil 
			}

			return false, nil
		} else {
			childPtr := unsafe.Pointer(childNode)
			_, delErr = pcMap.deleteRecursive(&childPtr, key, level + 1)
			if delErr != nil { return false, delErr }

			popCount := CalculateHammingWeight(nodeCopy.Bitmap)
			if popCount == 0 {
				nodeCopy.Bitmap = SetBit(nodeCopy.Bitmap, index)
				nodeCopy.Children = ShrinkTable(nodeCopy.Children, nodeCopy.Bitmap, pos)
			}

			return pcMap.compareAndSwap(node, currNode, nodeCopy), nil
		}
	}
}

func (cMap *PCMap) compareAndSwap(node *unsafe.Pointer, currNode *PCMapNode, nodeCopy *PCMapNode) bool {
	if atomic.CompareAndSwapPointer(node, unsafe.Pointer(currNode), unsafe.Pointer(nodeCopy)) {
		return true
	} else { return false }
}