package pcmap

import "os"
import "unsafe"
import "sync"

import "github.com/sirgallo/pcmap/common/mmap"


type PCMapOpts struct {
	Filepath string
	// amount of space allocated when db needs to create new pages
	AllocSize *int
	// initial mmap size of pcmap in bytes
	InitialMmapSize *int
	// overrides default OS page size (normally 4KB)
	PageSize *int
}

type PCMapMetaData struct {
	Version uint64
	RootOffset uint64
}

// PCMapNode represents a singular node within the hash array mapped trie data structure. This is the 32 bit implementation
type PCMapNode struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	Version uint64
	// StartOffset: the offset from the beginning of the serialized node is located
	StartOffset uint64
	// EndOffset: the offset from the end of the serialized node is located
	EndOffset uint64
	// Bitmap: a 32 bit sparse index that indicates the location of each hashed key within the array of child nodes. Only stored in internal nodes
	Bitmap uint32
	// IsLeaf: flag indicating if the current node is a leaf node or an internal node
	IsLeaf bool
	KeyLength uint16
	// Key: The key associated with a value. Keys are 32 bit hashes in byte array representation. Keys are only stored within leaf nodes of the hamt
	Key []byte
	// Value: The value associated with a key, in byte array representation. Values are only stored within leaf nodes
	Value []byte
	// Children: an array of child nodes, which are CMapNodes. Location in the array is determined by the sparse index
	Children []*PCMapNode
}

// CMap is the root of the hash array mapped trie
type PCMap struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	// Version uint32
	// Root: the root PCMapNode within the hash array mapped trie. Stored as a pointer to the location in memory of the root
	// Root unsafe.Pointer
	// HashChunks: the total chunks of the 32 bit hash determining the levels within the hash array mapped trie
	HashChunks int
	// BitChunkSize: the size of each chunk in the 32 bit hash. Since a 32 bit hash is 2^5, each chunk will be 5 bits long
	BitChunkSize int
	// Filepath: path to the PCMap file
	Filepath string
	// File: the PCMap file
	File *os.File
	// Opened: flag indicating if the file has been opened
	Opened bool
	// Data: the memory mapped file as a byte slice
	Data mmap.MMap

	Meta unsafe.Pointer
	// PageSize: the size of each page in in bytes. 4KiB is typical for most OS
	// PageSize int
	// Pages: a map of the current pages in the PCMap
	// Pages map[PageId] *Page
	// CurrentPageId: a reference to the current page in the hash array mapped trie where new data can be written
	// CurrentPageId PageId
	// CurrentPageOffset: the offset from the start of the current page. When CMapNodes are serialized, the offset is increased by the size of new node
	// NextPageId: the id of the next page to be allocated once the current page has been filled
	// NextPageId PageId
	// AllocSize: the size of data to allocate when expanding the memory mapped buffer
	AllocSize int

	RWLock sync.RWMutex
}


// DefaultPageSize is the default page size set by the underlying OS. Usually will be 4KiB
var DefaultPageSize = os.Getpagesize()

const (
	DefaultAllocSize = 16 * 1024 * 1024
	
	MaxMapSize = 0xFFFFFFFFFFFF // ARM, 256TB
	MaxAllocSize = 0x7FFFFFFF// ARM

	MetaVersionIdx = 0
	MetaRootOffsetIdx = 8
	MetaNextOffsetIdx = 16

	NodeVersionIdx = 0
	NodeStartOffsetIdx = 8
	NodeEndOffsetIdx = 16
	NodeBitmapIdx = 24
	NodeIsLeafIdx = 28
	NodeKeyLength = 29
	NodeKeyIdx = 31
	NodeChildrenIdx = 31

	OffsetSize = 8
	BitmapSize = 4
	NodeChildPtrSize = 8

	NewINodeSize = 29

	KeyPaddingId = 0x00

	InitRootOffset = 16

	MaxKeyLength = 64
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
		29 Key - 64 bytes
		93 Value - variable length

	Node (Internal):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 8 bytes
		24 Bitmap - 4 bytes
		28 IsLeaf - 1 byte
		29 Children -->
			every child will then be 8 bytes, up to 32 * 8 = 256 bytes

	New Initial setup will be
	
	mmap:
		Meta
			0
			8
		First Version Root
			16
			24
			32
			36
			37
*/