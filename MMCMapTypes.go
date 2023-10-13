package mmcmap

import "os"
import "sync"
import "unsafe"

import "github.com/sirgallo/mmcmap/common/mmap"


// MMCMapOpts initialize the MMCMap
type MMCMapOpts struct {
	Filepath string
	// amount of space allocated when db needs to create new pages
	AllocSize *int
	// initial mmap size of pcmap in bytes
	InitialMmapSize *int
	// overrides default OS page size (normally 4KB)
	PageSize *int
}

// MMCMapMetaData contains information related to where the root is located in the mem map and the version.
type MMCMapMetaData struct {
	// Version: a tag for Copy-on-Write indicating the version of the MMCMap
	Version uint64
	// RootOffset: the offset of the latest version root node in the pcmap
	RootOffset uint64
	// EndMmapOffset: the offset where the last node in the mmap is located
	EndMmapOffset uint64
}

// MMCMapNode represents a singular node within the hash array mapped trie data structure. This is the 32 bit implementation
type MMCMapNode struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	Version uint64
	// StartOffset: the offset from the beginning of the serialized node is located
	StartOffset uint64
	// EndOffset: the offset from the end of the serialized node is located
	EndOffset uint64
	// Bitmap: a 32 bit sparse index that indicates the location of each hashed key within the array of child nodes. Only stored in internal nodes
	Bitmap uint32
	// IsLeaf: flag indicating if the current node is a leaf node or an internal node
	IsLeaf    bool
	KeyLength uint16
	// Key: The key associated with a value. Keys are 32 bit hashes in byte array representation. Keys are only stored within leaf nodes of the hamt
	Key []byte
	// Value: The value associated with a key, in byte array representation. Values are only stored within leaf nodes
	Value []byte
	// Children: an array of child nodes, which are CMapNodes. Location in the array is determined by the sparse index
	Children []*MMCMapNode
}

// MMCMap contains the memory mapped buffer for the pcmap, as well as all metadata for operations to occur
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
	Data mmap.MMap
	// Meta: the metadata for the current version and offset of root node in the MMCMap
	Meta unsafe.Pointer
	// AllocSize: the size to allocate for the MMCMap in the memory mapped file
	AllocSize int
	// RWLock: A Read-Write mutex for synchronizing writes to the memory map
	RWLock sync.RWMutex
}

// DefaultPageSize is the default page size set by the underlying OS. Usually will be 4KiB
var DefaultPageSize = os.Getpagesize()

const (
	// Index of MMCMap Version in serialized metadata
	MetaVersionIdx = 0
	// Index of Root Offset in serialized metadata
	MetaRootOffsetIdx = 8
	// Index of Node Version in serialized node
	MetaEndMmapOffset = 16
	// The current node version index in serialized node
	NodeVersionIdx = 0
	// Index of StartOffset in serialized node
	NodeStartOffsetIdx = 8
	// Index of EndOffset in serialized node
	NodeEndOffsetIdx = 16
	// Index of Bitmap in serialized node
	NodeBitmapIdx = 24
	// Index of IsLeaf in serialized node
	NodeIsLeafIdx = 28
	// Index of Key Length in serialized node
	NodeKeyLength = 29
	// Index of Key in serialized leaf node node
	NodeKeyIdx = 31
	// Index of Children in serialized internal node
	NodeChildrenIdx = 31
	// OffsetSize for uint64 in serialized node
	OffsetSize = 8
	// Bitmap size in bytes since bitmap sis uint32
	BitmapSize = 4
	// Size of child pointers, where the pointers are uint64 offsets in the memory map
	NodeChildPtrSize = 8
	// Size of a new empty internal not
	NewINodeSize = 29
	// Offset for the first version of root on pcmap initialization
	InitRootOffset = 24
	// 1 GB MaxResize
	MaxResize = 1000000000
)

/*
offsets explained

	Meta:
		0 Version - 8 bytes
		8 RootOffset - 8 bytes

	[0-7, 8-15, 16-23, 24-27, 28, 29-92, 93+]
	Node (Leaf):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 8 bytes
		24 Bitmap - 4 bytes
		28 IsLeaf - 1 bytes
		29 KeyLength - 2 bytes, size of the key
		31 Key - variable length


	Node (Internal):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 8 bytes
		24 Bitmap - 4 bytes
		28 IsLeaf - 1 byte
		29 KeyLength - 2 bytes
		31 Children -->
			every child will then be 8 bytes, up to 32 * 8 = 256 bytes
*/