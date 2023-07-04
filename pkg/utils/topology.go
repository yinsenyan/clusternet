package utils

import (
	"fmt"
	"sort"
	"strings"
)

func ClusterReplicaSum(replicas map[string][]int32) (result []int32) {
	for _, replicas := range replicas {
		for index, replica := range replicas {
			if len(result) <= index {
				result = append(result, 0)
			}
			result[index] += replica
		}
	}
	return
}

func MapSortToString(s map[string]string) string {
	var domain []string
	for k, v := range s {
		if IsTopologyKey(k) {
			domain = append(domain, fmt.Sprintf("%s=%s", k, v))
		}
	}
	sort.Strings(domain)
	return strings.Join(domain, ",")
}

func IsZoneKey(key string) bool {
	hostname := strings.Split(key, "/")
	if len(hostname) == 0 {
		return false
	}
	domain := strings.Split(hostname[0], ".")
	if len(domain) < 4 {
		return false
	}
	if domain[len(domain)-3] == "topology" && domain[len(domain)-4] == "zone" {
		return true
	}
	return false
}

func IsRegionKey(key string) bool {
	hostname := strings.Split(key, "/")
	if len(hostname) == 0 {
		return false
	}
	domain := strings.Split(hostname[0], ".")
	if len(domain) < 4 {
		return false
	}
	if domain[len(domain)-3] == "topology" && domain[len(domain)-4] == "region" {
		return true
	}
	return false
}

func IsTopologyKey(key string) bool {
	hostname := strings.Split(key, "/")
	if len(hostname) == 0 {
		return false
	}
	domain := strings.Split(hostname[0], ".")
	if len(domain) < 3 {
		return false
	}
	if domain[len(domain)-3] == "topology" {
		return true
	}
	return false
}

func Int32Ptr(i int32) *int32 {
	return &i
}

func AvailableCluster(max []int32, avg int32) int {
	var count int
	for _, v := range max {
		if v > avg {
			count++
		}
	}
	return count
}

// EscapeJSONPointerValue handle escape json value
func EscapeJSONPointerValue(in string) string {
	step := strings.ReplaceAll(in, "~", "~0")
	return strings.ReplaceAll(step, "/", "~1")
}
