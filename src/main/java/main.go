package main

import (
	"fmt"
	"sort"
)

func getRepeatElems(nums []int) []int {
	sort.Ints(nums)
	n := len(nums)
	res := []int{}

	l, r := 0, 0
	for r < n {
		for nums[r] == nums[l] {
			r++
		}
		if r-l > 1 {
			res = append(res, nums[l])
			l = r
		}
	}

	return res
}

func get(nums []int) []int {
	cnt := map[int]int{}
	added := map[int]int{}
	var res []int
	for _, num := range nums {
		if _, ok := cnt[num]; ok {
			if _, add := added[num]; !add {
				res = append(res, num)
				added[num]++
			}
		} else {
			cnt[num]++
		}
	}
	return res
}

type Node struct {
	key, val string
}

type KV struct {
	// memo map[string]string
	// sortedKeys []string  // sort by key
	head, tail *Node
	memo       map[string]*Node
}

// insert: O(logn)
// query: O(1)

func (kv KV) get(key string) (string, error) {
	return "", nil
}

func (kv KV) put(key, value string) {
	return
}

func (kv KV) delete(key string) error {
	return nil
}

/*
	    {
		    {key1, val1},
		    {key2, val2},
	    }
*/
func (kv KV) list(key string, n int) [][2]int {

}

func main() {
	fmt.Println(getRepeatElems([]int{1, 3, 1, 2, 3, 4}))
}
