package mmcmap

import "sync/atomic"
import "unsafe"


func (mmcMap *MMCMap) BuildCache() error {
	rootOffsetPtr, _ := mmcMap.LoadMetaRootOffsetPointer()
	rootOffset := atomic.LoadUint64(rootOffsetPtr)

	currRoot, readRootErr := mmcMap.ReadNodeFromMemMap(rootOffset)
	if readRootErr != nil { return readRootErr }

	cLog.Debug("curr root:", currRoot)
	rootPtr := unsafe.Pointer(currRoot)
	for {
		ok, buildErr := mmcMap.getChildrenRecursive(&rootPtr, 0)
		if buildErr != nil { return buildErr }
		if ok { break }
	}

	atomic.StorePointer(&mmcMap.PartialMapCache, rootPtr)
	mmcMap.PrintCache()

	return nil
}

func (mmcMap *MMCMap) getChildrenRecursive(node *unsafe.Pointer, level int) (bool, error) {
	if level <= MaxCacheLevel {
		currNode := (*MMCMapNode)(atomic.LoadPointer(node))
		nodeCopy := mmcMap.CopyNode(currNode)
		// cLog.Debug("node copy", nodeCopy)
	
		/*
		if nodeCopy.Version == 100000 {
			// cLog.Debug("nodeCopy", nodeCopy)
		}
		*/

		if ! nodeCopy.IsLeaf {
			if nodeCopy.Bitmap == 0 { return true, nil }

			for idx := range currNode.Children {
				childOffset := nodeCopy.Children[idx]
				
				child, desErr := mmcMap.ReadNodeFromMemMap(childOffset.StartOffset)
				if desErr != nil { 
					cLog.Debug("child offset in error:", childOffset)
					return false, desErr 
				}
		
				childPtr := unsafe.Pointer(child)
				_, getErr := mmcMap.getChildrenRecursive(&childPtr, level + 1)
				if getErr != nil { return false, getErr }
				
				nodeCopy.Children[idx] = (*MMCMapNode)(atomic.LoadPointer(&childPtr))
			}
		}
	
		return atomic.CompareAndSwapPointer(node, unsafe.Pointer(currNode), unsafe.Pointer(nodeCopy)), nil
	}

	return true, nil
}