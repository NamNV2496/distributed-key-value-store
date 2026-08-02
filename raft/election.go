package raft

import (
	"context"
	"errors"
	"log"
)

// VoteResponse represents the response from a vote request
type VoteResponse struct {
	Term        int64
	VoteGranted bool
}

func (rn *RaftNode) campaignFindLeaderNode(preVote bool) {
	rn.mu.Lock()

	if rn.role == LeaderRole {
		rn.mu.Unlock()
		return
	}

	// One campaign at a time. Persisting a vote is a durable write, so a
	// campaign can easily outlive an election timeout; without this guard the
	// timer would launch a second overlapping election, bump the term again,
	// and the two would invalidate each other indefinitely.
	if rn.campaigning {
		rn.mu.Unlock()
		return
	}
	rn.campaigning = true
	defer func() {
		rn.mu.Lock()
		rn.campaigning = false
		rn.mu.Unlock()
	}()

	// Single-node cluster: become leader immediately without entering Candidate state.
	if len(rn.clientMap) == 0 {
		if preVote {
			rn.mu.Unlock()
			rn.clearCampaigning()
			rn.campaignFindLeaderNode(false)
			return
		}
		rn.state.SetTermAndVotedFor(rn.state.CurrentTerm()+1, rn.nodeID)
		rn.role = CandidateRole
		term := rn.state.CurrentTerm()
		rn.mu.Unlock()
		rn.becomeLeader(term)
		return
	}

	var campaignTerm int64
	if preVote {
		campaignTerm = rn.state.CurrentTerm() + 1
	} else {
		rn.state.SetTermAndVotedFor(rn.state.CurrentTerm()+1, rn.nodeID)
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

	// Snapshot the clients while we still hold the lock. The goroutines below
	// must not touch rn.clientMap: reading a map without the lock, from N
	// goroutines, while AddPeer may be writing to it, is a data race.
	peers := make([]RaftRPCClient, 0, len(rn.clientMap))
	for _, client := range rn.clientMap {
		peers = append(peers, client)
	}
	rn.mu.Unlock()

	voteCh := make(chan VoteResponse, len(peers))
	for _, client := range peers {
		go func(client RaftRPCClient) {
			ctx, cancel := context.WithTimeout(context.Background(), rn.electionTimeout)
			defer cancel()

			req := RequestVoteArgs{
				Term:         campaignTerm,
				CandidateId:  rn.nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				PreVote:      preVote,
			}

			reply, err := client.RequestVote(ctx, &req)
			if err != nil {
				voteCh <- VoteResponse{VoteGranted: false}
				return
			}
			voteCh <- VoteResponse{Term: reply.Term, VoteGranted: reply.VoteGranted}
		}(client)
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
			if preVote {
				rn.clearCampaigning()
				rn.campaignFindLeaderNode(false)
				return
			}
			rn.becomeLeader(campaignTerm)
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

// clearCampaigning releases the in-flight guard early, for the pre-vote path
// that immediately recurses into the real election. The deferred release still
// runs afterwards and is harmless.
func (rn *RaftNode) clearCampaigning() {
	rn.mu.Lock()
	rn.campaigning = false
	rn.mu.Unlock()
}

func (rn *RaftNode) quorum() int {
	return (len(rn.clientMap)+1)/2 + 1
}

// becomeLeader promotes this node and starts its heartbeat loop.
//
// It appends a no-op entry of the new term. Raft only ever commits an entry by
// counting replicas of an entry from the CURRENT term (§5.4.2), so without a
// no-op a new leader cannot commit entries inherited from previous terms until
// a client happens to write — leaving those entries replicated but not
// committed, and invisible to reads. Committing the no-op commits everything
// before it.
func (rn *RaftNode) becomeLeader(term int64) {
	rn.mu.Lock()
	// Re-validate under the lock. The tally loop released rn.mu between
	// counting the winning vote and getting here, and in that window we may
	// have seen a higher term and stepped down. Promoting anyway would install
	// a leader for a term it no longer owns — a second leader.
	if rn.role != CandidateRole || rn.state.CurrentTerm() != term {
		rn.mu.Unlock()
		return
	}
	rn.role = LeaderRole
	rn.leaderId = rn.nodeID
	nextIdx := int64(rn.log.Len())
	for peerID := range rn.clientMap {
		rn.nextIndex[peerID] = nextIdx
		rn.matchIndex[peerID] = -1
	}
	if rn.onStateChange != nil {
		rn.onStateChange(rn.role)
	}
	rn.mu.Unlock()

	go rn.sendHeartbeats()

	if _, err := rn.Propose(NoOpCommand, nil); err != nil && !errors.Is(err, ErrNotLeader) {
		log.Printf("[%s] failed to append no-op entry on election: %v", rn.nodeID, err)
	}
}
