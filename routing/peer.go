package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
)

const TopologyVersionHeader = "X-Topology-Version"
const MaxBodyBytes = 16 << 20 // 16MB

type CommandEnvelope struct {
	NodeID  string `json:"node_id"`
	Command string `json:"command,omitempty"`
	Status  string `json:"status"`
	Result  any    `json:"result,omitempty"`
	Error   string `json:"error,omitempty"`
}

type PeerClient struct {
	selfID  string
	topo    *cluster.Store
	http    *http.Client
	install func(*cluster.Topology) (bool, error)
}

func NewPeerClient(
	selfID string,
	topo *cluster.Store,
	client *http.Client,
	install func(*cluster.Topology) (bool, error),
) *PeerClient {
	return &PeerClient{selfID: selfID, topo: topo, http: client, install: install}
}

func (p *PeerClient) Version() int64 {
	if topo := p.topo.Get(); topo != nil {
		return topo.Version
	}
	return 0
}

func ShardCommandURL(base, shardID string) string {
	return base + "/shards/" + shardID + "/raft/command"
}

func KVURL(base, _ string) string { return base + "/kv" }

func (p *PeerClient) PostToShardMember(
	ctx context.Context, shardID string, body []byte, prefer string, url func(base, shardID string) string,
) (any, string, error) {
	topo := p.topo.Get()
	if topo == nil {
		return nil, "", errors.New("no topology yet; cannot route")
	}
	shard, ok := topo.Shards[shardID]
	if !ok {
		return nil, "", fmt.Errorf("unknown shard %q", shardID)
	}

	var errs []error
	for _, nodeID := range orderCandidates(shard.NodeIDs(), prefer, p.selfID) {
		result, err := p.postCommand(ctx, url(shard.Members[nodeID], shardID), body)
		if err == nil {
			return result, nodeID, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", nodeID, err))
		if ctx.Err() != nil {
			break
		}
	}
	if len(errs) == 0 {
		return nil, "", fmt.Errorf("shard %q has no member to serve this other than %s", shardID, p.selfID)
	}
	return nil, "", fmt.Errorf("shard %q unreachable: %w", shardID, errors.Join(errs...))
}

func orderCandidates(members []string, prefer, skip string) []string {
	out := make([]string, 0, len(members))
	for _, id := range members {
		if id == prefer && id != skip {
			out = append(out, id)
		}
	}
	for _, id := range members {
		if id == skip || id == prefer {
			continue
		}
		out = append(out, id)
	}
	return out
}

func (p *PeerClient) postCommand(ctx context.Context, url string, body []byte) (any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(TopologyVersionHeader, strconv.FormatInt(p.Version(), 10))

	resp, err := p.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	p.noticeTopologyVersion(resp)

	raw, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes))
	if err != nil {
		return nil, err
	}
	var env CommandEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(raw))
		}
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if env.Error != "" {
		return nil, errors.New(env.Error)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s", resp.Status)
	}
	return env.Result, nil
}

func (p *PeerClient) noticeTopologyVersion(resp *http.Response) {
	header := resp.Header.Get(TopologyVersionHeader)
	if header == "" {
		return
	}
	theirs, err := strconv.ParseInt(header, 10, 64)
	if err != nil || theirs <= p.Version() {
		return
	}
	base := resp.Request.URL.Scheme + "://" + resp.Request.URL.Host
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		topo, err := p.FetchTopology(ctx, base)
		if err != nil {
			log.Printf("[%s] could not pull topology v%d from %s: %v", p.selfID, theirs, base, err)
			return
		}
		if installed, err := p.install(topo); err != nil {
			log.Printf("[%s] rejected topology from %s: %v", p.selfID, base, err)
		} else if installed {
			log.Printf("[%s] adopted topology v%d from %s", p.selfID, topo.Version, base)
		}
	}()
}

func (p *PeerClient) FetchTopology(ctx context.Context, baseURL string) (*cluster.Topology, error) {
	return FetchTopologyFrom(ctx, p.http, baseURL)
}

func FetchTopologyFrom(ctx context.Context, client *http.Client, baseURL string) (*cluster.Topology, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/cluster/topology", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s", resp.Status)
	}
	var topo cluster.Topology
	if err := json.NewDecoder(io.LimitReader(resp.Body, MaxBodyBytes)).Decode(&topo); err != nil {
		return nil, err
	}
	return &topo, topo.Validate()
}

// CleanSeeds normalises seed URLs, dropping blanks and trailing slashes.
func CleanSeeds(seeds []string) []string {
	out := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		seed = strings.TrimRight(strings.TrimSpace(seed), "/")
		if seed != "" {
			out = append(out, seed)
		}
	}
	return out
}
