package surfstore

import (
	"bufio"
	"net"

	//	"google.golang.org/grpc"
	"io"
	"log"

	//	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	grpc "google.golang.org/grpc"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal("Surfstore RPCClient:", "Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}

	return
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	// TODO any initialization you need to do here

	isCrashedMutex := &sync.RWMutex{}
	next := make([]int64, len(ips))
	match := make([]int64, len(ips))
	for i, _ := range ips {
		next[i] = 0
		match[i] = -1
	}

	server := RaftSurfstore{
		// TODO initialize any fields you add here
		ip:       ips[id],
		ipList:   ips,
		serverId: id,

		lastApplied: -1,
		commitIndex: -1,

		nextIndex:  next,
		matchIndex: match,

		isLeader:       false,
		term:           0,
		metaStore:      NewMetaStore(blockStoreAddr),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		notCrashedCond: sync.NewCond(isCrashedMutex),
		isCrashedMutex: isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)

	l, e := net.Listen("tcp", server.ip)
	if e != nil {
		return e
	}

	return s.Serve(l)
}
