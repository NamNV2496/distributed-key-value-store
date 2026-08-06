package routing

import (
	"encoding/json"
	"log"
	"net/http"
)

type KVResponse struct {
	NodeID          string `json:"node_id"`
	Shard           string `json:"shard,omitempty"`
	Slot            int    `json:"slot"`
	Command         string `json:"command,omitempty"`
	Status          string `json:"status"`
	Result          any    `json:"result,omitempty"`
	Error           string `json:"error,omitempty"`
	TopologyVersion int64  `json:"topology_version"`

	ServedBy string `json:"served_by,omitempty"`

	Moved *MovedInfo `json:"moved,omitempty"`
}

type MovedInfo struct {
	Slot  int               `json:"slot"`
	Shard string            `json:"shard"`
	Nodes map[string]string `json:"nodes"`
}

func (r KVResponse) WithError(msg string) KVResponse {
	r.Status = "error"
	r.Error = msg
	return r
}

func WriteJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

func ReadOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "the dashboard port is read-only; use the cluster port", http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
}
