package shardmaster

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTargetAllocation(t *testing.T) {
	testInputs := []struct {
		numShards int
		numGroups int
	}{
		{9, 2},
		{9, 5},
		{10, 3},
	}

	testOutputs := [][]int{
		{5, 4},
		{2, 2, 2, 2, 1},
		{4, 3, 3},
	}

	for i := 0; i < len(testInputs); i++ {
		actualOutput := getTargetAllocation(testInputs[i].numShards, testInputs[i].numGroups)
		assert.Equal(t, len(testOutputs[i]), len(actualOutput), "Expected length not equal to actual length")
		for j, size := range actualOutput {
			assert.Equal(t, testOutputs[i][j], size, "Expected group size not equal to actual size")
		}
	}
}

func TestReallocation(t *testing.T) {
	configs := []Config{
		{
			Num:    0,
			Shards: [NShards]int{},
			Groups: map[int][]string{1: []string{"server0"}},
		},
		{
			Num:    1,
			Shards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			Groups: map[int][]string{
				1: []string{"server0"},
				2: []string{"server1"},
				3: []string{"server2"},
			},
		},
		{
			Num:    2,
			Shards: [NShards]int{2, 2, 2, 3, 3, 3, 1, 1, 1, 1},
			Groups: map[int][]string{
				1: []string{"server0"},
				2: []string{"server1"},
				3: []string{"server2"},
				4: []string{"server3"},
			},
		},
		{
			Num:    3,
			Shards: [NShards]int{2, 2, 2, 4, 3, 3, 4, 1, 1, 1},
			Groups: map[int][]string{
				1: []string{"server0"},
				2: []string{"server1"},
				3: []string{"server2"},
			},
		},
		{
			Num:    4,
			Shards: [NShards]int{2, 2, 2, 1, 3, 3, 3, 1, 1, 1},
			Groups: map[int][]string{
				1: []string{"server0"},
			},
		},
		{
			Num:    5,
			Shards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			Groups: make(map[int][]string),
		},
		{
			Num:    6,
			Shards: [NShards]int{},
			Groups: make(map[int][]string),
		},
	}
	newGroupsInputs := [][]int{
		{1},
		{2, 3},
		{4},
		nil,
		nil,
		nil,
	}
	removedGroupsInputs := [][]int{
		nil,
		nil,
		nil,
		{4},
		{2, 3},
		{1},
	}
	for i := range newGroupsInputs {
		reallocateShards(&configs[i], newGroupsInputs[i], removedGroupsInputs[i])
		for j, gid := range configs[i].Shards {
			assert.Equal(t, configs[i+1].Shards[j], gid, fmt.Sprintf("Group number for shards at index [%d] mismatch", j))
		}
	}
}
