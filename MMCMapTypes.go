package mmcmap

import "os"
import "sync"
import "sync/atomic"


// MMCMapOpts initialize the MMCMap
type MMCMapOpts struct {
	// Filepath: the path to the memory mapped file
	Filepath string
}

// MMCMapMetaData contains information related to where the root is located in the mem map and the version.
type MMCMapMetaData struct {
	// Version: a tag for Copy-on-Write indicating the version of the MMCMap
	Version uint64
	// RootOffset: the offset of the latest version root node in the mmcmap
	RootOffset uint64
	// NextStartOffset: the offset where the last node in the mmap is located
	NextStartOffset uint64
}

// MMCMapNode represents a singular node within the hash array mapped trie data structure. This is the 32 bit implementation
type MMCMapINode struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	Version uint64
	// StartOffset: the offset from the beginning of the serialized node is located
	StartOffset uint64
	// EndOffset: the offset from the end of the serialized node is located
	EndOffset uint64
	// Bitmap: a 32 bit sparse index that indicates the location of each hashed key within the array of child nodes. Only stored in internal nodes
	Bitmap [8]uint32
	// LeafOffset: the offset of the leaf node associated with the current byte chunk
	Leaf *MMCMapLNode
	// Children: an array of child nodes, which are MMCMapNodes. Location in the array is determined by the sparse index
	Children []*MMCMapINode
}

type MMCMapLNode struct {
	Version uint64
	// StartOffset: the offset from the beginning of the serialized node is located
	StartOffset uint64
	// EndOffset: the offset from the end of the serialized node is located
	EndOffset uint64
	// KeyLength: the length of the key in a Leaf Node. Keys can be variable size
	KeyLength uint16
	// Key: The key associated with a value. Keys are in byte array representation. Keys are only stored within leaf nodes
	Key []byte
	// Value: The value associated with a key, in byte array representation. Values are only stored within leaf nodes
	Value []byte
}

type KeyValuePair struct {
	Version uint64
	Key []byte
	Value []byte
}

// MMCMap contains the memory mapped buffer for the mmcmap, as well as all metadata for operations to occur
type MMCMap struct {
	// HashChunks: the total chunks of the 32 bit hash determining the levels within the hash array mapped trie
	HashChunks int
	// BitChunkSize: the size of each chunk in the 32 bit hash. Since a 32 bit hash is 2^5, each chunk will be 5 bits long
	BitChunkSize int
	// Filepath: path to the MMCMap file
	Filepath string
	// File: the MMCMap file
	File *os.File
	// Opened: flag indicating if the file has been opened
	Opened bool
	// Data: the memory mapped file as a byte slice
	Data atomic.Value
	// IsResizing: atomic flag to determine if the mem map is being resized or not
	IsResizing uint32
	// SignalResize: send a signal to the resize go routine with the offset for resizing
	SignalResize chan bool
	// SignalFlush: send a signal to flush to disk on writes to avoid contention
	SignalFlush chan bool
	// ReadResizeLock: A Read-Write mutex for locking reads on resize operations
	RWResizeLock sync.RWMutex
	// NodePool: the sync.Pool for recycling nodes so nodes are not constantly allocated/deallocated
	NodePool *MMCMapNodePool
}

// MMCMapNodePool contains pre-allocated mmcmap nodes to improve performance so go garbage collection doesn't handle allocating/deallocating nodes on every op
type MMCMapNodePool struct {
	// MaxSize: the max size for the node pool
	MaxSize int64
	// Size: the current number of allocated nodes in the node pool
	Size int64
	// Pool: the node pool that contains pre-allocated nodes
	INodePool *sync.Pool

	LNodePool *sync.Pool
}

// DefaultPageSize is the default page size set by the underlying OS. Usually will be 4KiB
var DefaultPageSize = os.Getpagesize()

const (
	// Index of MMCMap Version in serialized metadata
	MetaVersionIdx = 0
	// Index of Root Offset in serialized metadata
	MetaRootOffsetIdx = 8
	// Index of Node Version in serialized node
	MetaEndSerializedOffset = 16
	// The current node version index in serialized node
	NodeVersionIdx = 0
	// Index of StartOffset in serialized node
	NodeStartOffsetIdx = 8
	// Index of EndOffset in serialized node
	NodeEndOffsetIdx = 16
	// Index of Bitmap in serialized node
	NodeBitmapIdx = 24
	// Index of IsLeaf in serialized node
	NodeLeafOffsetIdx = 56
	// Index of Children in serialized internal node
	NodeChildrenIdx = 64
	// Index of Key Length in serialized node
	NodeKeyLength = 24
	// Index of Key in serialized leaf node node
	NodeKeyIdx = 26
	// OffsetSize for uint64 in serialized node
	OffsetSize = 8
	// Bitmap size in bytes since bitmap sis uint32
	BitmapSize = 4
	// Size of child pointers, where the pointers are uint64 offsets in the memory map
	NodeChildPtrSize = 8
	// Offset for the first version of root on mmcmap initialization
	InitRootOffset = 24
	// 1 GB MaxResize
	MaxResize = 1000000000
)

/*
	Offsets explained:

	Meta:
		0 Version - 8 bytes
		8 RootOffset - 8 bytes
		16 EndMmapOffset - 8 bytes

	[0-7, 8-15, 16-23, 24-27, 28, 29-92, 93+]
	Node (Leaf):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 8 bytes
		24 KeyLength - 2 bytes, size of the key
		26 Key - variable length


	Node (Internal):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 8 bytes
		24 8 Bitmaps - 32 bytes
		56 LeafOffset - 8 bytes
		64 Children -->
			every child will then be 8 bytes, up to 256 * 8 = 2048 bytes
*/