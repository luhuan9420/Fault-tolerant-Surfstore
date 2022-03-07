package surfstore

import (
	context "context"
	"log"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var metaLock sync.Mutex

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	metaLock.Lock()
	defer metaLock.Unlock()

	res := &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}
	return res, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	metaLock.Lock()
	defer metaLock.Unlock()

	filename := fileMetaData.GetFilename()
	// check if file exists in the map
	if _, ok := m.FileMetaMap[filename]; ok {
		curr := m.FileMetaMap[filename].GetVersion()
		new := fileMetaData.GetVersion()
		log.Printf("curr version %v: %v\n", filename, curr)
		log.Printf("new version %v: %v\n", fileMetaData.GetFilename(), new)
		if new-curr == 1 {
			m.FileMetaMap[filename] = fileMetaData
			return &Version{Version: fileMetaData.GetVersion()}, nil
		} else {
			return &Version{Version: -1}, nil //fmt.Errorf("New version number is not right\n")
		}
	} else {
		log.Printf("curr version %v: %v\n", fileMetaData.GetFilename(), fileMetaData.GetVersion())
		m.FileMetaMap[filename] = fileMetaData
		return &Version{Version: fileMetaData.GetVersion()}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	metaLock.Lock()
	defer metaLock.Unlock()

	// log.Printf("block store address: %v\n", m.BlockStoreAddr)
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	// log.Printf("block store addr: %v\n", blockStoreAddr)
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
