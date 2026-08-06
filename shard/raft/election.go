package raft

import (
	"context"
	"errors"
	"log"
)

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
	if _, votes := rn.voters[rn.nodeID]; !votes {
		rn.mu.Unlock()
		return
	}
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

	if len(rn.votingPeers()) == 0 {
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

	peers := rn.votingPeers()
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

	votes := 1
	needed := rn.quorum()

	for range peers {
		resp := <-voteCh
		rn.mu.Lock()

		if preVote && rn.role == LeaderRole {
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
			if preVote {
				rn.clearCampaigning()
				rn.campaignFindLeaderNode(false)
				return
			}
			rn.becomeLeader(campaignTerm)
			return
		}
	}

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

func (rn *RaftNode) clearCampaigning() {
	rn.mu.Lock()
	rn.campaigning = false
	rn.mu.Unlock()
}

func (rn *RaftNode) quorum() int {
	return len(rn.voters)/2 + 1
}

func (rn *RaftNode) votingPeers() []RaftRPCClient {
	out := make([]RaftRPCClient, 0, len(rn.clientMap))
	for peerID, client := range rn.clientMap {
		if _, votes := rn.voters[peerID]; votes {
			out = append(out, client)
		}
	}
	return out
}

func (rn *RaftNode) becomeLeader(term int64) {
	rn.mu.Lock()
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

	if _, _, err := rn.Propose(NoOpCommand, nil); err != nil && !errors.Is(err, ErrNotLeader) {
		log.Printf("[%s] failed to append no-op entry on election: %v", rn.nodeID, err)
	}
}
