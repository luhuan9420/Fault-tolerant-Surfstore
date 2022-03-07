package surfstore

import (
	context "context"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex    int64       // last completely committed index
	pendingCommits []chan bool // not yet committed index
	lastApplied    int64

	nextIndex  []int64
	matchIndex []int64

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// // Leader protection
	// isLeaderMutex sync.RWMutex
	// isLeaderCond  *sync.Cond

	// rpcClients []*grpc.ClientConn

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

// type Crash struct {
// 	NotCrash *bool
// }

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	log.Println("Get file info map...")

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if s.isLeader == false {
		return nil, ERR_NOT_LEADER
	}

	// check majority
	// if majroity of nodes are crashed, block until majrotiy recover
	// for {
	// 	if s.CheckMajority(ctx, empty) {
	// 		break
	// 	}
	// }
	if s.CheckMajority(ctx, empty) == false {
		return nil, ERR_MAJORITY_CRASHED
	}
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil

	// commitChan := make(chan *bool, len(s.ipList))
	// for i, _ := range s.ipList {
	// 	if i == int(s.serverId) {
	// 		continue
	// 	}
	// 	go s.CheckAlive(int64(i), commitChan)
	// }

	// aliveCount := 1
	// for {
	// 	commit := <-commitChan
	// 	if commit != nil && (*commit) == true {
	// 		aliveCount++
	// 	}
	// 	if aliveCount > len(s.ipList)/2 {
	// 		return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
	// 	}
	// }

	// return nil, ERR_MAJORITY_CRASHED
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if s.isLeader == false {
		return nil, ERR_NOT_LEADER
	}

	// for {
	// 	if s.CheckMajority(ctx, empty) {
	// 		break
	// 	}
	// }
	if s.CheckMajority(ctx, empty) == false {
		return nil, ERR_MAJORITY_CRASHED
	}
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil

	// commitChan := make(chan *bool, len(s.ipList))
	// for i, _ := range s.ipList {
	// 	if i == int(s.serverId) {
	// 		continue
	// 	}
	// 	go s.CheckAlive(int64(i), commitChan)
	// }

	// aliveCount := 1
	// for {
	// 	commit := <-commitChan
	// 	if commit != nil && (*commit) == true {
	// 		aliveCount++
	// 	}
	// 	if aliveCount > len(s.ipList)/2 {
	// 		return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
	// 	}
	// }
	// return nil, ERR_MAJORITY_CRASHED
}

func (s *RaftSurfstore) CheckMajority(ctx context.Context, empty *emptypb.Empty) bool {
	alive := 0
	for i, addr := range s.ipList {
		if i == int(s.serverId) {
			alive++
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		defer conn.Close()
		client := NewRaftSurfstoreClient(conn)
		clientCrashed, _ := client.IsCrashed(ctx, empty)
		if clientCrashed.IsCrashed == false {
			alive++
		}
		if alive > len(s.ipList)/2 {
			return true
		}
	}
	return false
}

// func (s *RaftSurfstore) CheckAlive(serverIdx int64, commitChan chan *bool) {
// 	for {
// 		conn, err := grpc.Dial(s.ipList[serverIdx])
// 		if err != nil {
// 			return
// 		}
// 		defer conn.Close()
// 		client := NewRaftSurfstoreClient(conn)
// 		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 		defer cancel()
// 		clientCrashed, _ := client.IsCrashed(ctx, &emptypb.Empty{})
// 		if clientCrashed.IsCrashed == false {
// 			b := true
// 			notCrash := Crash{&b}
// 			commitChan <- notCrash.NotCrash
// 			return
// 		} else {
// 			continue
// 		}
// 	}
// }

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// update fileMetaData
	// if leader & majority nodes working, return correct answer (call updateFile function)
	// if majority node crashed, block until majority recover
	// if not leader, return error back to client
	log.Printf("Server %v call raft update file...\n", s.serverId)
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if s.isLeader == false {
		return nil, ERR_NOT_LEADER
	}
	log.Printf("Server %v is leader\n", s.serverId)

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)
	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed)

	go s.attemptCommit()

	success := <-committed
	if success {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) attemptCommit() {
	log.Printf("Server %v call attempt commit...\n", s.serverId)
	targetIdx := s.commitIndex + 1
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for i, _ := range s.ipList {
		if i == int(s.serverId) {
			continue
		}
		go s.commitEntry(int64(i), targetIdx, commitChan)
	}

	commitCount := 1
	for {
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		if commitCount > len(s.ipList)/2 {
			s.pendingCommits[targetIdx] <- true
			s.commitIndex = targetIdx
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	log.Printf("Server %v call commit entry...\n", serverIdx)
	fmt.Printf("Server %v call commit entry...\n log: %v\n", serverIdx, s.log[entryIdx])
	trial := 1
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		defer conn.Close()
		client := NewRaftSurfstoreClient(conn)
		log.Printf("Server %v connects successfully", serverIdx)
		prevLogIndex := s.nextIndex[serverIdx] - 1
		log.Printf("Prev log index: %v\n", prevLogIndex)
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = int(s.log[prevLogIndex].Term)
		}

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  int64(prevLogTerm),
			Entries:      []*UpdateOperation{s.log[entryIdx]},
			LeaderCommit: s.commitIndex + 1,
		}

		log.Printf("Append Entry Input: %v\n", input)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		// trial := 1
		// for {
		log.Printf("Server %v calls AppendEntries: trial %v", serverIdx, trial)
		trial++
		output, err := client.AppendEntries(ctx, input)
		if err != nil {
			if strings.Contains(err.Error(), ERR_WRONG_TERM.Error()) {
				s.isLeader = false
				return
			}
			if strings.Contains(err.Error(), ERR_PREVLOGTERM_MISMATCH.Error()) {
				input.Entries = append([]*UpdateOperation{s.log[input.PrevLogIndex]}, input.Entries...)
				input.PrevLogIndex -= 1
				s.nextIndex[serverIdx] -= 1
				if input.PrevLogIndex >= 0 {
					input.PrevLogTerm = s.log[input.PrevLogIndex].Term
				} else {
					input.PrevLogTerm = -1
				}
			}
		}
		if output.Success {
			log.Printf("Server %v Append entry success\n", serverIdx)
			commitChan <- output
			return
		} else if s.isCrashed {
			continue
		}
		// }

	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	log.Printf("Append entries function...\n")
	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}
	// log.Printf("Initial output: %v\n", output)
	if s.isCrashed {
		log.Printf("Server %v is crashed\n", s.serverId)
		return output, ERR_SERVER_CRASHED
	}
	log.Printf("follower %v term: %v", s.serverId, s.term)
	//1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		input.Term = s.term
		output.Term = s.term
		return output, ERR_WRONG_TERM
	}

	if input.Term > s.term {
		//there is a new leader
		if s.isLeader {
			s.isLeader = false
		}
		s.term = input.Term
	}
	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	//matches prevLogTerm (§5.3)
	if int(input.PrevLogIndex) > len(s.log) {
		return output, ERR_PREVLOGTERM_MISMATCH
	}

	if input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		return output, ERR_PREVLOGTERM_MISMATCH
	}

	//3. If an existing entry conflicts with a new one (same index but different
	//terms), delete the existing entry and all that follow it (§5.3)

	// if int(input.LeaderCommit)+1 > len(s.log) {
	// 	return output, ERR_LOG_INCONSISTENT
	// }
	// if input.Entries[input.LeaderCommit] != s.log[input.LeaderCommit] {
	// 	return output, ERR_LOG_INCONSISTENT
	// }
	s.log = s.log[:input.PrevLogIndex+1]

	//4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)
	log.Printf("Commit index: %v\n", s.commitIndex)
	log.Printf("Leader commit: %v\n", input.LeaderCommit)

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}
	log.Printf("Last Applied: %v\n", s.lastApplied)
	log.Printf("Commit index: %v\n", s.commitIndex)
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	s.nextIndex[s.serverId] = int64(len(s.log))
	s.matchIndex[s.serverId] = int64(len(s.log) - 1)

	output.Success = true
	output.Term = input.Term
	output.ServerId = s.serverId
	output.MatchedIndex = s.matchIndex[s.serverId]

	fmt.Printf("Server %v append entries\nlog: %v\n", s.serverId, s.log)
	// fmt.Printf("Server %v output: %v\n", s.serverId, output)
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// check crash
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	fmt.Printf("Server %v sets to be a leader...\n", s.serverId)
	// need lock ?
	s.isLeader = true
	s.term = s.term + 1
	// update next match
	// log.Printf("Number of servers: %v \n", len(s.ipList))
	for i, _ := range s.ipList {
		s.nextIndex[i] = int64(len(s.log))
	}
	s.matchIndex[s.serverId] = int64(len(s.log) - 1)

	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// fmt.Printf("Server %v start sends heartbeat...\n", s.serverId)
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	if s.isLeader == false {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	for i, addr := range s.ipList {
		if i == int(s.serverId) {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		client := NewRaftSurfstoreClient(conn)
		// log.Printf("Server %v connects successfully", i)

		prevLogIndex := s.nextIndex[i] - 1
		// log.Printf("Prev log index: %v\n", prevLogIndex)
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = int(s.log[prevLogIndex].Term)
		}
		// log.Printf("Prev log term: %v\n", prevLogTerm)

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  int64(prevLogTerm),
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}
		// log.Printf("Append Entry Input: %v\n", input)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		trial := 1
		for {
			log.Printf("Server %v calls AppendEntries: trial %v", i, trial)
			trial++
			output, err := client.AppendEntries(ctx, input)
			if err != nil {
				if strings.Contains(err.Error(), ERR_WRONG_TERM.Error()) {
					s.isLeader = false
					return &Success{Flag: false}, ERR_NOT_LEADER
				}
				if strings.Contains(err.Error(), ERR_PREVLOGTERM_MISMATCH.Error()) {
					input.Entries = append([]*UpdateOperation{s.log[input.PrevLogIndex]}, input.Entries...)
					input.PrevLogIndex -= 1
					s.nextIndex[i] -= 1
					if input.PrevLogIndex >= 0 {
						input.PrevLogTerm = s.log[input.PrevLogIndex].Term
					} else {
						input.PrevLogTerm = -1
					}
				}
				if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
					log.Printf("Server %v crash\n", i)
					break
				}
			}
			if output.Success {
				log.Printf("Server %v Append entry success\n", i)
				break
			}
		}

	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
