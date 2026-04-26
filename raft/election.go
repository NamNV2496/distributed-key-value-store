package raft

import (
	"context"
	"time"
)

// VoteResponse represents the response from a vote request
type VoteResponse struct {
	Term        int64
	VoteGranted bool
}

func (rn *RaftNode) campaign(preVote bool) {
	rn.mu.Lock()

	// If we somehow become leader already (e.g., concurrent campaign), bail out
	if rn.role == LeaderRole {
		rn.mu.Unlock()
		return
	}

	var campaignTerm int64
	if preVote {
		// Pre-Vote uses term + 1 in request but doesn't persist or increment yet
		campaignTerm = rn.state.CurrentTerm() + 1
	} else {
		// Become candidate - increment term, vote for self
		rn.state.SetCurrentTerm(rn.state.CurrentTerm() + 1)
		rn.state.SetVotedFor(rn.nodeID)
		campaignTerm = rn.state.CurrentTerm()
	}

	// Snapshot what we need from the locked state before releasing
	var lastLogIndex, lastLogTerm int64
	lastEntry := rn.log.LastEntry()
	if lastEntry != nil {
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	} else {
		lastLogIndex = -1
		lastLogTerm = 0
	}

	peers := make([]string, 0, len(rn.peers))
	for id := range rn.peers {
		peers = append(peers, id)
	}
	rn.mu.Unlock()

	// Single node cluster - win immediately without any RPCs.
	if len(peers) == 0 {
		rn.mu.Lock()
		if preVote {
			rn.mu.Unlock()
			rn.campaign(false) // prev-vote passed, now to the real election
		} else {
			rn.role = LeaderRole
			rn.leaderId = rn.nodeID
			// Initialize leader state
			for peerID := range rn.peers {
				if peerID != rn.nodeID {
					rn.nextIndex[peerID] = int64(rn.log.Len())
					rn.matchIndex[peerID] = -1
				}
			}
			if rn.onStateChange != nil {
				rn.onStateChange(rn.role)
			}
			rn.mu.Unlock()
			// Start sending heartbeats
			go rn.sendHeartbeats()
		}
		return
	}
	voteCh := make(chan VoteResponse, len(peers))
	for _, peerID := range peers {
		go func(peerID string) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			req := RequestVoteArgs{
				Term:         campaignTerm,
				CandidateId:  rn.nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			var resp VoteResponse

			client, ok := rn.clientMap[peerID]
			if !ok {
				resp = VoteResponse{VoteGranted: false}
				voteCh <- resp
				return
			}

			if preVote {
				// For pre-vote, we still use RequestVote but with term+1
				// The server should handle this via handlePreVote logic
				reply, err := client.RequestVote(ctx, &req)
				if err != nil {
					resp = VoteResponse{VoteGranted: false}
				} else {
					resp = VoteResponse{Term: reply.Term, VoteGranted: reply.VoteGranted}
				}
			} else {
				reply, err := client.RequestVote(ctx, &req)
				if err != nil {
					resp = VoteResponse{VoteGranted: false}
				} else {
					resp = VoteResponse{Term: reply.Term, VoteGranted: reply.VoteGranted}
				}
			}
			voteCh <- resp
		}(peerID)
	}

	// Tally results. Start at 1 - we always vote for ourselves.
	votes := 1
	needed := rn.quorum()

	for range peers {
		resp := <-voteCh
		rn.mu.Lock()
		if preVote && rn.role != CandidateRole {
			rn.mu.Unlock()
			return
		}
		if !preVote && (rn.role != CandidateRole || rn.state.CurrentTerm() != campaignTerm) {
			rn.mu.Unlock()
			return
		}
		if resp.Term > rn.state.CurrentTerm() {
			rn.state.SetCurrentTerm(resp.Term)
			rn.role = FollowerRole
			rn.leaderId = ""
			if rn.onStateChange != nil {
				rn.onStateChange(rn.role)
			}
			rn.mu.Unlock()
			return
		}

		rn.mu.Unlock()

		if resp.VoteGranted {
			votes++
		}

		if votes >= needed {
			// Quorum reached - we won.
			rn.mu.Lock()
			if preVote {
				rn.mu.Unlock()
				rn.campaign(false)
			} else {
				rn.role = LeaderRole
				rn.leaderId = rn.nodeID
				for peerID := range rn.peers {
					if peerID != rn.nodeID {
						rn.nextIndex[peerID] = int64(rn.log.Len())
						rn.matchIndex[peerID] = -1
					}
				}
				if rn.onStateChange != nil {
					rn.onStateChange(rn.role)
				}
				rn.mu.Unlock()
				go rn.sendHeartbeats()
			}
			return
		}
	}

	// Failed to reach quorum. Step back to follower and wait for the next timeout.
	rn.mu.Lock()
	if rn.role == CandidateRole {
		rn.role = FollowerRole
		rn.leaderId = ""
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
	}
	rn.mu.Unlock()
}

func (rn *RaftNode) isLogUpToDate(candidateLastTerm, candidateLastIndex int64) bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	var myLastTerm, myLastIndex int64
	lastEntry := rn.log.LastEntry()
	if lastEntry != nil {
		myLastTerm = lastEntry.Term
		myLastIndex = lastEntry.Index
	} else {
		myLastIndex = -1
		myLastTerm = 0
	}

	// In case of different terms, whoever has higher term is up-to-date
	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	// Same term: whoever has more entries is up-to-date
	return candidateLastIndex >= myLastIndex
}

// Returns the minimum votes needed to win: floor(N/2) + 1.
func (rn *RaftNode) quorum() int {
	return (len(rn.peers)+1)/2 + 1
}
