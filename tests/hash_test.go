package tests

import (
	"fmt"
	"math"
	"testing"

	"github.com/kusu/memcache-go/cluster"
)

func TestRingEmpty(t *testing.T) {
	r := cluster.NewRing(0, nil)
	if !r.IsEmpty() {
		t.Fatal("new ring should be empty")
	}
	if r.Get("anything") != "" {
		t.Fatal("empty ring should return empty node")
	}
}

func TestRingDeterministic(t *testing.T) {
	r := cluster.NewRing(0, nil)
	r.Add("node1", "node2", "node3")
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if r.Get(key) != r.Get(key) {
			t.Fatalf("non-deterministic mapping for %s", key)
		}
	}
}

func TestRingDistribution(t *testing.T) {
	r := cluster.NewRing(0, nil)
	nodes := []string{"node1", "node2", "node3"}
	r.Add(nodes...)

	counts := map[string]int{}
	const n = 30000
	for i := 0; i < n; i++ {
		counts[r.Get(fmt.Sprintf("key-%d", i))]++
	}
	// Each node should get roughly n/3; allow +/-25% skew.
	expected := float64(n) / float64(len(nodes))
	for _, node := range nodes {
		got := float64(counts[node])
		if math.Abs(got-expected)/expected > 0.25 {
			t.Fatalf("node %s got %d keys, expected ~%.0f (>25%% skew)", node, counts[node], expected)
		}
	}
}

func TestRingRemovalStability(t *testing.T) {
	r := cluster.NewRing(0, nil)
	r.Add("node1", "node2", "node3")

	before := map[string]string{}
	const n = 10000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key] = r.Get(key)
	}

	r.Remove("node3")

	moved := 0
	for key, oldNode := range before {
		newNode := r.Get(key)
		if oldNode != newNode {
			moved++
		}
	}
	// With consistent hashing, only keys that were on node3 (~1/3) should move.
	fraction := float64(moved) / float64(n)
	if fraction > 0.5 {
		t.Fatalf("too many keys moved after removal: %.2f%%", fraction*100)
	}
}

func TestRingGetN(t *testing.T) {
	r := cluster.NewRing(0, nil)
	r.Add("node1", "node2", "node3")
	got := r.GetN("some-key", 2)
	if len(got) != 2 {
		t.Fatalf("GetN returned %d nodes, want 2", len(got))
	}
	if got[0] == got[1] {
		t.Fatal("GetN returned duplicate nodes")
	}
}
