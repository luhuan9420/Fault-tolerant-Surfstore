package surfstore

import (
	context "context"
	"fmt"
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
	fmt.Printf("[Server %v]: Raft GetFileInfoMap...\n", s.serverId)

	if s.isCrashed {
		fmt.Printf("[Server %v]: is crashed\n", s.serverId)
		return nil, ERR_SERVER_CRASHED
	}

	if s.isLeader == false {
		fmt.Printf("[Server %v]: is not a leader\n", s.serverId)
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
		fmt.Printf("[Server %v]: Majority of followers crash\n", s.serverId)
		return nil, ERR_MAJORITY_CRASHED
	}
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	fmt.Printf("[Server %v]: Raft GetBlockStoreAddr...\n", s.serverId)
	if s.isCrashed {
		fmt.Printf("[Server %v]: is crashed\n", s.serverId)
		return nil, ERR_SERVER_CRASHED
	}

	if s.isLeader == false {
		fmt.Printf("[Server %v]: is not a leader\n", s.serverId)
		return nil, ERR_NOT_LEADER
	}

	// for {
	// 	if s.CheckMajority(ctx, empty) {
	// 		break
	// 	}
	// }
	if s.CheckMajority(ctx, empty) == false {
		fmt.Printf("[Server %v]: Majority of followers crash\n", s.serverId)
		return nil, ERR_MAJORITY_CRASHED
	}
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
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
		} else {
			fmt.Printf("[Server %v]: is crash\n", i)
		}
		if alive > len(s.ipList)/2 {
			return true
		}
	}
	return false
}

func (s *RaftSurfstore) WaitMajorityRecover() {
	commitChan := make(chan bool, len(s.ipList))
	for i, _ := range s.ipList {
		if i == int(s.serverId) {
			continue
		}
		go s.CheckRecovery(int64(i), commitChan)
	}
	commitCount := 1
	for {
		fmt.Printf("[Server %v]: wait for follower nodes recover: %v\n", s.serverId, commitChan)
		commit := <-commitChan
		if commit == true {
			commitCount++
		}
		if commitCount > len(s.ipList)/2 {
			break
		}
	}
}

func (s *RaftSurfstore) CheckRecovery(serverIdx int64, commitChan chan bool) {
	for {
		conn, err := grpc.Dial(s.ipList[serverIdx], grpc.WithInsecure())
		if err != nil {
			return
		}
		defer conn.Close()
		client := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		clientCrashed, _ := client.IsCrashed(ctx, &emptypb.Empty{})
		if clientCrashed.IsCrashed == false && clientCrashed != nil {
			fmt.Printf("[Server %v]: is restored\n", serverIdx)
			commitChan <- true
			return
		}
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// update fileMetaData
	// if leader & majority nodes working, return correct answer (call updateFile function)
	// if majority node crashed, block until majority recover
	// if not leader, return error back to client
	fmt.Printf("[Server %v]: Raft update file...\n", s.serverId)
	if s.isCrashed {
		fmt.Printf("[Server %v]: is crash\n", s.serverId)
		return nil, ERR_SERVER_CRASHED
	}

	if s.isLeader == false {
		fmt.Printf("[Server %v]: is not a leader\n", s.serverId)
		return nil, ERR_NOT_LEADER
	}
	fmt.Printf("[Server %v]: is leader\n", s.serverId)

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)
	fmt.Printf("[Server %v]: Log (before committed): %v\n", s.serverId, s.log)
	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed)

	idx := len(s.pendingCommits) - 1
	fmt.Printf("Index for pending commits: %v\n", idx)
	go s.attemptCommit(idx)
	fmt.Printf("[Server %v]: Pending commit: %v\n", s.serverId, s.pendingCommits)

	// check if majority of servers alive
	if s.CheckMajority(ctx, &emptypb.Empty{}) == false {
		fmt.Println("Majority of followers are down...\t Wait for recovery")
		s.WaitMajorityRecover()
	}
	fmt.Println("Majroity of floowers respond...")

	success := <-committed
	if success {
		fmt.Printf("[Server %v]: Commit return true, leader update file\n", s.serverId)
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) attemptCommit(idx int) {
	fmt.Printf("[Server %v]: attempt commit...\n", s.serverId)
	targetIdx := s.commitIndex + 1
	fmt.Printf("[Server %v]: target index: %v\n", s.serverId, targetIdx)
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for i, _ := range s.ipList {
		if i == int(s.serverId) {
			continue
		}
		go s.commitEntry(int64(i), targetIdx, commitChan)
	}

	commitCount := 1
	for {
		fmt.Printf("[Server %v]: followers commit result (AppendEntryOutput): %v\n", s.serverId, commitChan)
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		if commitCount > len(s.ipList)/2 {
			fmt.Printf("%v followers committed (majority), pending to commit\n", commitCount)
			fmt.Printf("[Server %v]: Pending commit: %v\n", s.serverId, s.pendingCommits)
			s.pendingCommits[idx] <- true
			s.commitIndex = targetIdx
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	// log.Printf("Server %v call commit entry...\n", serverIdx)
	fmt.Printf("[Server %v]: try to commit entry...\n commit item: %v\n", serverIdx, s.log[entryIdx])
	// trial := 1
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		defer conn.Close()
		client := NewRaftSurfstoreClient(conn)
		fmt.Printf("[Server %v]: connects successfully\n", serverIdx)
		prevLogIndex := s.nextIndex[serverIdx] - 1
		fmt.Printf("[Server %v]: Prev log index: %v\n", serverIdx, prevLogIndex)
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

		fmt.Printf("[Server %v]: Append Entry Input: %v\n", serverIdx, input)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		trial := 1
		for {
			fmt.Printf("[Server %v]: calls AppendEntries: trial %v\n", serverIdx, trial)
			trial++
			output, err := client.AppendEntries(ctx, input)
			fmt.Printf("[Server %v]: Append Entries Output: %v\n", serverIdx, output)
			if err != nil {
				if strings.Contains(err.Error(), ERR_WRONG_TERM.Error()) {
					s.isLeader = false
					fmt.Printf("[Server %v]: Not newest leader, terminate\n", s.serverId)
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
					fmt.Printf("[Server %v]: Append Entry Input (Update): %v\n", serverIdx, input)
				}
				if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
					continue
				}
			}
			if output.Success {
				fmt.Printf("[Server %v]: Append entry success\n", serverIdx)
				commitChan <- output
				return
			} else if s.isCrashed {
				fmt.Printf("[Server %v]: is crash\n", s.serverId)
				continue
			}
		}

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
	// log.Printf("Append entries function...\n")
	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}
	// log.Printf("Initial output: %v\n", output)
	if s.isCrashed {
		fmt.Printf("[Server %v]: is crash\n", s.serverId)
		return output, ERR_SERVER_CRASHED
	}
	// log.Printf("follower %v term: %v", s.serverId, s.term)
	//1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		fmt.Printf("[Server %v]: Wrong term, follower has higher term: Follower term: %v, Leader term: %v\n", s.serverId, s.term, input.Term)
		input.Term = s.term
		output.Term = s.term
		return output, ERR_WRONG_TERM
	}

	if input.Term > s.term {
		//there is a new leader
		fmt.Printf("[Server %v]: Update follower's term: Follower term %v, Leader term: %v\n", s.serverId, s.term, input.Term)
		if s.isLeader {
			s.isLeader = false
		}
		s.term = input.Term
	}
	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	//matches prevLogTerm (§5.3)
	if int(input.PrevLogIndex) > len(s.log) {
		fmt.Printf("[Server %v]: Prev log index mismatch: leader's PrevLogIndex: %v, follower's log length: %v\n", s.serverId, input.PrevLogIndex, len(s.log))
		return output, ERR_PREVLOGTERM_MISMATCH
	}

	if input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		fmt.Printf("[Server %v]: Prev log term mismatch: leader's PrevLogTerm: %v, follower's log PrevLogIndex term: %v\n", s.serverId, input.PrevLogTerm, s.log[input.PrevLogIndex].Term)
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
	fmt.Printf("[Server %v]: append entries log: %v\n", s.serverId, s.log)

	// log.Printf("Commit index: %v\n", s.commitIndex)
	// log.Printf("Leader commit: %v\n", input.LeaderCommit)

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}
	fmt.Printf("[Server %v]: Last Applied: %v\n", s.serverId, s.lastApplied)
	fmt.Printf("[Server %v]: Commit index: %v\n", s.serverId, s.commitIndex)
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	s.nextIndex[s.serverId] = int64(len(s.log))
	s.matchIndex[s.serverId] = int64(len(s.log) - 1)
	fmt.Printf("[Server %v]: next index: %v\n", s.serverId, s.nextIndex[s.serverId])
	fmt.Printf("[Server %v]: match index: %v\n", s.serverId, s.matchIndex[s.serverId])

	output.Success = true
	output.Term = input.Term
	output.ServerId = s.serverId
	output.MatchedIndex = s.matchIndex[s.serverId]

	// fmt.Printf("Server %v append entries\nlog: %v\n", s.serverId, s.log)
	// fmt.Printf("Server %v output: %v\n", s.serverId, output)
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// check crash
	if s.isCrashed {
		fmt.Printf("[Server %v]: is crash\n", s.serverId)
		return nil, ERR_SERVER_CRASHED
	}
	// need lock ?
	s.isLeader = true
	s.term = s.term + 1
	fmt.Printf("[Server %v]: is leader now. Term: %v\n", s.serverId, s.term)
	// update next match
	// log.Printf("Number of servers: %v \n", len(s.ipList))
	for i, _ := range s.ipList {
		s.nextIndex[i] = int64(len(s.log))
	}
	s.matchIndex[s.serverId] = int64(len(s.log) - 1)
	fmt.Printf("[Server %v]: pending commit: %v\n", s.serverId, s.pendingCommits)
	fmt.Printf("Next Index: %v\n", s.nextIndex)
	fmt.Printf("Match Index: %v\n", s.matchIndex)

	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Printf("[Server %v]: start sends heartbeat...\n", s.serverId)
	fmt.Printf("[Server %v]: log before: %v\n", s.serverId, s.log)
	// fmt.Printf("Next index: %v\n", s.nextIndex)
	if s.isCrashed {
		fmt.Printf("[Server %v]: is crash\n", s.serverId)
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	if s.isLeader == false {
		fmt.Printf("[Server %v]: is not a leader\n", s.serverId)
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
		fmt.Printf("[Server %v]: connects successfully\n", i)

		prevLogIndex := s.nextIndex[i] - 1
		// log.Printf("Prev log index: %v\n", prevLogIndex)
		fmt.Printf("[Server %v]: Prev log index: %v\n", i, prevLogIndex)
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = int(s.log[prevLogIndex].Term)
		}
		// log.Printf("Prev log term: %v\n", prevLogTerm)

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  int64(prevLogTerm),
			Entries:      s.log[s.nextIndex[i]:],
			LeaderCommit: s.commitIndex,
		}
		// log.Printf("Append Entry Input: %v\n", input)
		fmt.Printf("[Server %v]: Append Entry Input: %v\n", i, input)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		trial := 1
		for {
			fmt.Printf("[Server %v]: calls AppendEntries: trial %v\n", i, trial)
			trial++
			output, err := client.AppendEntries(ctx, input)
			fmt.Printf("[Server %v]: Append Entries Output: %v\n", i, output)
			if err != nil {
				if strings.Contains(err.Error(), ERR_WRONG_TERM.Error()) {
					s.isLeader = false
					fmt.Printf("[Server %v]: Not newest leader, terminate\n", s.serverId)
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
					fmt.Printf("[Server %v]: Append Entry Input (Update): %v\n", i, input)
				}
				if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
					break
				}
			}
			if output.Success {
				fmt.Printf("[Server %v]: Append entry success\n", i)
				break
			} else if s.isCrashed {
				fmt.Printf("[Server %v]: is crash\n", s.serverId)
			}
		}
		internal, _ := client.GetInternalState(ctx, &emptypb.Empty{})
		fmt.Printf("[Server %v]: log after: %v\n", i, internal.Log)

	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()
	fmt.Printf("[Server %v]: is crashed\n", s.serverId)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()
	fmt.Printf("[Server %v]: is restored\n", s.serverId)

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
