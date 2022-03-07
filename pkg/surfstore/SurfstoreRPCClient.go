package surfstore

import (
	context "context"
	"log"
	"time"

	grpc "google.golang.org/grpc"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// var blockLock sync.Mutex
// var metaLock sync.Mutex

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {

	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {

	// log.Printf("block store addr: %v\n", blockStoreAddr)
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	bs := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := bs.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = s.GetFlag()
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	bs := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bh, err := bs.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = bh.GetHashes()
	return conn.Close()
}

func (surfClient *RPCClient) GetLeaderIndex() int {
	// log.Printf("MetaStoreAddrs: %v\n", surfClient.MetaStoreAddrs)
	for i, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		internalState, err := c.GetInternalState(ctx, &emptypb.Empty{})
		if err != nil {
			continue
		}
		if internalState.IsLeader {
			return i
		}
	}
	return -1
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	leaderIdx := surfClient.GetLeaderIndex()

	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[leaderIdx], grpc.WithInsecure())
	if err != nil {
		return err
	}
	ms := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fileInfoMap, err := ms.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	for k, v := range fileInfoMap.GetFileInfoMap() {
		(*serverFileInfoMap)[k] = &FileMetaData{
			Filename:      v.Filename,
			Version:       v.Version,
			BlockHashList: v.BlockHashList,
		}
	}
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// log.Printf("client update file...\n")
	leaderIdx := surfClient.GetLeaderIndex()
	log.Printf("leaderIdx: %v\n", leaderIdx)

	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[leaderIdx], grpc.WithInsecure())
	if err != nil {
		return err
	}
	ms := NewRaftSurfstoreClient(conn)
	log.Printf("Connect successfully\n")
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	version, err := ms.UpdateFile(ctx, fileMetaData)
	log.Printf("Version: %v\n", version)
	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = version.GetVersion()
	// *&fileMetaData.Version = *latestVersion

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	leaderIdx := surfClient.GetLeaderIndex()

	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[leaderIdx], grpc.WithInsecure())
	if err != nil {
		return err
	}

	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddr = addr.Addr

	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
