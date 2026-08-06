package cluster

func NodeAddresses(topos ...*Topology) map[string]string {
	out := make(map[string]string)
	for _, t := range topos {
		if t == nil {
			continue
		}
		for _, shard := range t.Shards {
			for nodeID, addr := range shard.Members {
				out[nodeID] = addr
			}
		}
	}
	return out
}
