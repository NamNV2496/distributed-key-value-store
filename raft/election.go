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

	if rn.role == LeaderRole {
		rn.mu.Unlock()
		return
	}

	// Single-node cluster: become leader immediately without entering Candidate state.
	if len(rn.clientMap) == 0 {
		if preVote {
			rn.mu.Unlock()
			rn.campaign(false)
			return
		}
		rn.state.SetCurrentTerm(rn.state.CurrentTerm() + 1)
		rn.state.SetVotedFor(rn.nodeID)
		rn.role = LeaderRole
		rn.leaderId = rn.nodeID
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
		rn.mu.Unlock()
		go rn.sendHeartbeats()
		return
	}

	var campaignTerm int64
	if preVote {
		campaignTerm = rn.state.CurrentTerm() + 1
	} else {
		rn.state.SetCurrentTerm(rn.state.CurrentTerm() + 1)
		rn.state.SetVotedFor(rn.nodeID)
		rn.role = CandidateRole
		rn.leaderId = ""
		campaignTerm = rn.state.CurrentTerm()
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
	}

	var lastLogIndex, lastLogTerm int64
	lastEntry := rn.log.LastEntry()
	if lastEntry != nil {
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	} else {
		lastLogIndex = -1
		lastLogTerm = 0
	}

	// Build peer list from clientMap so self is never included.
	peers := make([]string, 0, len(rn.clientMap))
	for id := range rn.clientMap {
		peers = append(peers, id)
	}
	rn.mu.Unlock()

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

			reply, err := rn.clientMap[peerID].RequestVote(ctx, &req)
			if err != nil {
				voteCh <- VoteResponse{VoteGranted: false}
				return
			}
			voteCh <- VoteResponse{Term: reply.Term, VoteGranted: reply.VoteGranted}
		}(peerID)
	}

	// Tally results. Start at 1 — we always vote for ourselves.
	votes := 1
	needed := rn.quorum()

	for range peers {
		resp := <-voteCh
		rn.mu.Lock()

		// Bail if role changed out from under us (e.g. another leader appeared).
		if preVote && rn.role == LeaderRole {
			rn.mu.Unlock()
			return
		}
		if !preVote && (rn.role != CandidateRole || rn.state.CurrentTerm() != campaignTerm) {
			rn.mu.Unlock()
			return
		}

		// Step down if we see a higher term.
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
			rn.mu.Lock()
			if preVote {
				rn.mu.Unlock()
				rn.campaign(false)
			} else {
				rn.role = LeaderRole
				rn.leaderId = rn.nodeID
				for peerID := range rn.clientMap {
					rn.nextIndex[peerID] = int64(rn.log.Len())
					rn.matchIndex[peerID] = -1
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

	// Failed to reach quorum — step back to follower.
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

	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIndex >= myLastIndex
}

// quorum returns the minimum votes needed to win: floor(N/2) + 1.
// Uses clientMap (peers excluding self) so the cluster size is always correct.
func (rn *RaftNode) quorum() int {
	return (len(rn.clientMap)+1)/2 + 1
}
