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
	for {
		currMetaPtr := atomic.LoadPointer(&pcMap.Meta)
		currMeta := (*PCMapMetaData)(currMetaPtr)

		metaFromMmap, readMmapErr := pcMap.ReadMetaFromMemMap()
		if readMmapErr != nil { return false, readMmapErr }

		if currMeta.Version == metaFromMmap.Version {
			currRoot, readRootErr := pcMap.ReadNodeFromMemMap(currMeta.RootOffset)
			if readRootErr != nil { return false, readRootErr }

			currRootSize := currRoot.GetNodeSize()

			currRoot.Version = currRoot.Version + 1
			currRoot.StartOffset = pcMap.DetermineNextOffset()
			currRoot.EndOffset = currRoot.StartOffset + currRootSize 

			fmt.Println("curr root:", currRoot)
			
			if currMetaPtr == atomic.LoadPointer(&pcMap.Meta) {
				serializedPath, ok, putErr := pcMap.putRecursive(currRoot, key, value, 0)
				if putErr != nil { return false, putErr }

				updatedMeta := &PCMapMetaData{
					Version: currRoot.Version,
					RootOffset: currRoot.StartOffset,
				}
		
				if atomic.CompareAndSwapPointer(&pcMap.Meta, unsafe.Pointer(currMeta), unsafe.Pointer(updatedMeta)) {
					writeOk, writeErr := pcMap.ExclusiveWriteMmap(updatedMeta, serializedPath, currRoot.StartOffset)
					if writeErr != nil { return false, writeErr }
					if ok && writeOk { return true, nil }
				}
			}
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
func (pcMap *PCMap) putRecursive(node *PCMapNode, key []byte, value []byte, level int) ([]byte, bool, error) {
	var ok bool
	var childNode *PCMapNode
	var sNode, sLeafNode, sChild, sUpdatedChildNodePath, sNewINodeAndChildren []byte
	var decErr, putErr, serializeErr error

	hash := pcMap.CalculateHashForCurrentLevel(key, level)
	index := pcMap.getSparseIndex(hash, level)

	if ! IsBitSet(node.Bitmap, index) {
		fmt.Println("new node:", node)
		newLeafOffset := func() uint64 { return node.EndOffset + NodeChildPtrSize + 1 }()
		
		sNode, sLeafNode, serializeErr = pcMap.AddLeafToPath(node, hash, key, value, index, level, newLeafOffset)
		if serializeErr != nil { return nil, false, serializeErr }

		var newINode, newOrigChild *PCMapNode
		newINode, decErr = pcMap.DeserializeNode(sNode)
		if decErr != nil { return nil, false, decErr }
		newOrigChild, decErr = pcMap.DeserializeNode(sLeafNode)
		if decErr != nil { return nil, false, decErr }


		fmt.Println("new node:", newINode, "new leaf:", newOrigChild, "leaf offset:", newLeafOffset, "node endoffset", node.EndOffset)

		return append(sNode, sLeafNode...), true, nil
	} else {
		pos := pcMap.getPosition(node.Bitmap, hash, level)
		childPtr := node.Children[pos]
		
		fmt.Println("childPtr", childPtr, "child start off", childPtr.StartOffset)
		childNode, decErr = pcMap.ReadNodeFromMemMap(childPtr.StartOffset)
		if decErr != nil { return nil, false, decErr }
		
		childNode.Version = node.Version
		childNode.StartOffset = node.EndOffset + 1

		if childNode.IsLeaf {
			if bytes.Equal(key, childNode.Key) {
				childNode.Value = value
				
				sNode, serializeErr = node.SerializeNode()
				if serializeErr != nil { return nil, false, serializeErr }

				sChild, serializeErr = childNode.SerializeNode()
				if serializeErr != nil { return nil, false, serializeErr }

				return append(sNode, sChild...), true, nil
			} else {
				iNodeStartOffset := node.EndOffset + 1
				newINode := pcMap.NewInternalNode(node.Version, iNodeStartOffset)

				sNewINodeAndChildren, ok, putErr = pcMap.UpdateLeafNodeToINode(newINode, key, value, childNode.Key, childNode.Value, level + 1)
				if putErr != nil { return nil, false, putErr }
				
				node.Children[pos] = &PCMapNode{ StartOffset: iNodeStartOffset }

				sNode, serializeErr := node.SerializeNode()
				if serializeErr != nil { return nil, false, serializeErr }

				return append(sNode, sNewINodeAndChildren...), ok, nil
			}
		} else {
			sUpdatedChildNodePath, ok, putErr = pcMap.putRecursive(childNode, key, value, level + 1)
			if putErr != nil { return nil, false, putErr }
			
			node.Children[pos] = &PCMapNode{ StartOffset: childNode.StartOffset }
			
			sNode, serializeErr := node.SerializeNode()
			if serializeErr != nil { return nil, false, serializeErr }

			return append(sNode, sUpdatedChildNodePath...), ok, nil
		}
	}
}

func (pcMap *PCMap) UpdateLeafNodeToINode(iNode *PCMapNode, key, value, origKey, origValue []byte, level int) ([]byte, bool, error) {
	// var sNode, sOrigChild, sNewChild []byte
	var sNode, sOrigChild []byte
	var decErr, serializeErr error
	// var serializeErr error

	// origChildOffset := func() uint64 { return iNode.StartOffset + NewINodeSize + (NodeChildPtrSize * 2) }()
	origChildOffset := func() uint64 { return iNode.StartOffset + NewINodeSize + NodeChildPtrSize + 1 }()

	origChildhash := pcMap.CalculateHashForCurrentLevel(origKey, level)
	origNewIndex := pcMap.getSparseIndex(origChildhash, level)

	sNode, sOrigChild, serializeErr = pcMap.AddLeafToPath(iNode, origChildhash, origKey, origValue, origNewIndex, level, origChildOffset)
	if serializeErr != nil { return nil, false, serializeErr }
	
	var newINode, newOrigChild *PCMapNode
	newINode, decErr = pcMap.DeserializeNode(sNode)
	if decErr != nil { return nil, false, decErr }
	newOrigChild, decErr = pcMap.DeserializeNode(sOrigChild)
	if decErr != nil { return nil, false, decErr }

	fmt.Println("new i node:", newINode, "new orig child:", newOrigChild)


	/*
	var updatedINode *PCMapNode
	updatedINode, decErr = pcMap.DeserializeNode(sNode)
	if decErr != nil { return nil, false, decErr }

	// fmt.Println("first iteration:", updatedINode)

	newChildOffset := func() uint64 { return origChildOffset + GetSerializedNodeSize(sOrigChild) }()
	newChildHash := pcMap.CalculateHashForCurrentLevel(key, level)
	newChildIndex := pcMap.getSparseIndex(newChildHash, level)

	if ! IsBitSet(updatedINode.Bitmap, newChildIndex) {
		sNode, sNewChild, serializeErr = pcMap.AddLeafToPath(updatedINode, newChildHash, key, value, newChildIndex, level, newChildOffset)
		if serializeErr != nil { return nil, false, serializeErr }

		updatedINode, decErr = pcMap.DeserializeNode(sNode)
		if decErr != nil { return nil, false, decErr }

		fmt.Println("here in second child on node split?", updatedINode, "children:", func() []uint64 {
			var childOffsets []uint64
			for _, node := range updatedINode.Children {
				childOffsets = append(childOffsets, node.StartOffset)
			}

			return childOffsets
		}())

		origChildN, _ := pcMap.DeserializeNode(sOrigChild)
		fmt.Println("orig child:", origChildN)

		newLeaves := append(sOrigChild, sNewChild...)
		return append(sNode, newLeaves...), true, nil
	} 

	updatedINode, decErr = pcMap.DeserializeNode(sNode)
	if decErr != nil { return nil, false, decErr }

	fmt.Println("updated inode:", updatedINode)

	*/

	return append(sNode, sOrigChild...), false, nil
}

func (pcMap *PCMap) AddLeafToPath(parent *PCMapNode, hash uint32, key, value []byte, index, level int, offset uint64) ([]byte, []byte, error) {
	var sNode, sLeafNode []byte
	var serializeErr error

	parent.Bitmap = SetBit(parent.Bitmap, index)
	pos := pcMap.getPosition(parent.Bitmap, hash, level)
	
	newLeaf := pcMap.NewLeafNode(key, value, parent.Version, offset)
	parent.Children = ExtendTable(parent.Children, parent.Bitmap, pos, newLeaf)

	sNode, serializeErr = parent.SerializeNode()
	if serializeErr != nil { return nil, nil, serializeErr }

	sLeafNode, serializeErr = newLeaf.SerializeNode()
	if serializeErr != nil { return nil, nil, serializeErr }

	return sNode, sLeafNode, nil
}

// Get attempts to retrieve the value for a key within the hash array mapped trie. 
//	The operation begins at the root of the trie and traverses down the path to the key.
//
// Returns:
//	either the value for the key in byte array representation or nil if the key does not exist
func (pcMap *PCMap) Get(key []byte) ([]byte, error) {
	for {
		currMetaPtr := atomic.LoadPointer(&pcMap.Meta)
		currMeta := (*PCMapMetaData)(currMetaPtr)
		
		metaFromMmap, readMmapErr := pcMap.ReadMetaFromMemMap()
		if readMmapErr != nil { return nil, readMmapErr }

		if currMeta.Version == metaFromMmap.Version { 
			currRoot, readRootErr := pcMap.ReadNodeFromMemMap(currMeta.RootOffset)
			if readRootErr != nil { return nil, readRootErr }

			if currMetaPtr == atomic.LoadPointer(&pcMap.Meta) {
				val, getErr := pcMap.getRecursive(currRoot, key, 0)
				if getErr != nil { return nil, getErr }
			
				return val, nil
			}
		}
	}
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
func (pcMap *PCMap) getRecursive(node *PCMapNode, key []byte, level int) ([]byte, error) {
	var childNode *PCMapNode
	var decErr error

	hash := pcMap.CalculateHashForCurrentLevel(key, level)
	index := pcMap.getSparseIndex(hash, level)

	if ! IsBitSet(node.Bitmap, index) {
		return nil, nil
	} else {
		pos := pcMap.getPosition(node.Bitmap, hash, level)
		childPtr := node.Children[pos]
		
		// fmt.Println("node:", node, "child ptr:", childPtr)

		childNode, decErr = pcMap.ReadNodeFromMemMap(childPtr.StartOffset)
		if decErr != nil { return nil, decErr }

		if childNode.IsLeaf && bytes.Equal(key, childNode.Key) {
			return childNode.Value, nil
		} else { return pcMap.getRecursive(childNode, key, level + 1) }
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
		
		metaFromMmap, readMmapErr := pcMap.ReadMetaFromMemMap()
		if readMmapErr != nil { return false, readMmapErr }
		
		if currMeta.Version == metaFromMmap.Version {
			currRoot, readRootErr := pcMap.ReadNodeFromMemMap(currMeta.RootOffset)
			if readRootErr != nil { return false, readRootErr }
	
			if currMetaPtr == atomic.LoadPointer(&pcMap.Meta) {
				currRootSize := currRoot.GetNodeSize()
	
				currRoot.Version = currRoot.Version + 1
				currRoot.StartOffset = pcMap.DetermineNextOffset()
				currRoot.EndOffset = currRoot.StartOffset + currRootSize 
	
				updatedMeta := &PCMapMetaData{
					Version: currRoot.Version,
					RootOffset: currRoot.StartOffset,
				}
		
				serializedPath, putErr := pcMap.deleteRecursive(currRoot, key, 0)
				if putErr != nil { return false, putErr }
		
				if atomic.CompareAndSwapPointer(&pcMap.Meta, unsafe.Pointer(currMeta), unsafe.Pointer(updatedMeta)) {
					return pcMap.ExclusiveWriteMmap(updatedMeta, serializedPath, currRoot.StartOffset)
				}
			}
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
func (pcMap *PCMap) deleteRecursive(node *PCMapNode, key []byte, level int) ([]byte, error) {
	// var childNode, updatedNode *PCMapNode
	var childNode *PCMapNode
	var sChildNode, sNode []byte
	var decErr, delErr, serializeErr error
	
	hash := pcMap.CalculateHashForCurrentLevel(key, level)
	index := pcMap.getSparseIndex(hash, level)

	if ! IsBitSet(node.Bitmap, index) {
		return nil, nil
	} else {
		pos := pcMap.getPosition(node.Bitmap, hash, level)
		childPtr := node.Children[pos]

		childNode, decErr = pcMap.ReadNodeFromMemMap(childPtr.StartOffset)
		if decErr != nil { return nil, decErr }

		childNode.Version = node.Version
		childNode.StartOffset = node.EndOffset

		if childNode.IsLeaf {
			if bytes.Equal(key, childNode.Key) {
				node.Bitmap = SetBit(node.Bitmap, index)
				node.Children = ShrinkTable(node.Children, node.Bitmap, pos)
				
				sNode, serializeErr = node.SerializeNode()
				if serializeErr != nil { return nil, serializeErr }

				return sNode, nil
			}

			return nil, nil
		} else {
			sChildNode, delErr = pcMap.deleteRecursive(childNode, key, level + 1)
			if delErr != nil { return nil, delErr }

			if sChildNode == nil {
				node.Bitmap = SetBit(node.Bitmap, index)
				node.Children = ShrinkTable(node.Children, node.Bitmap, pos)
			}

			sNode, serializeErr = node.SerializeNode()
			if serializeErr != nil { return nil, serializeErr}

			return sNode, nil
		}
	}
}