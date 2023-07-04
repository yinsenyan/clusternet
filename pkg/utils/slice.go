/*
Copyright 2021 The Clusternet Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Copied from k8s.io/kubernetes/pkg/utils/slice/slice.go
// and make some modifications

// CopyStrings copies the contents of the specified string slice
// into a new slice.
func CopyStrings(s []string) []string {
	if s == nil {
		return nil
	}
	c := make([]string, len(s))
	copy(c, s)
	return c
}

// SortStrings sorts the specified string slice in place. It returns the same
// slice that was provided in order to facilitate method chaining.
func SortStrings(s []string) []string {
	sort.Strings(s)
	return s
}

// ContainsString checks if a given slice of strings contains the provided string.
func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func ContainesMap(slice []map[string]string, m map[string]string) bool {
	for _, item := range slice {
		if reflect.DeepEqual(item, m) {
			return true
		}
	}
	return false
}

// ContainsPrefix checks if a given slice of strings start with the provided string.
func ContainsPrefix(slice []string, s string) bool {
	for _, item := range slice {
		if strings.HasPrefix(s, item) {
			return true
		}
	}
	return false
}

// RemoveString returns a newly created []string that contains all items from slice that
// are not equal to s.
func RemoveString(slice []string, s string) []string {
	newSlice := make([]string, 0)
	for _, item := range slice {
		if item == s {
			continue
		}
		newSlice = append(newSlice, item)
	}
	if len(newSlice) == 0 {
		// Sanitize for unit tests so we don't need to distinguish empty array
		// and nil.
		newSlice = nil
	}
	return newSlice
}

func RemoveStringByIndex(slice []string, index int) (result []string) {
	for i, v := range slice {
		if i == index {
			continue
		}
		result = append(result, v)
	}
	return
}

func RemoveInt32(slice []int32, index int) (result []int32) {
	for i, v := range slice {
		if i == index {
			continue
		}
		result = append(result, v)
	}
	return
}

func RemoveMap(slice []map[string]string, m map[string]string) []map[string]string {
	var result []map[string]string
	for _, v := range slice {
		if !reflect.DeepEqual(v, m) {
			result = append(result, v)
		}
	}
	return result
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func MaxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func SumArrayInt32(array []int32) (sum int32) {
	for _, v := range array {
		sum += v
	}
	return
}

func MinIndex(array []int32) int {
	if len(array) == 1 {
		return 0
	}
	var result int
	var tmp int32 = array[0]
	for index, value := range array {
		if value < tmp {
			tmp = value
			result = index
		}
	}
	return result
}

func SumMapInt32s(m map[string][]int32) (sum []int32) {
	for _, array := range m {
		for i, v := range array {
			sum[i] += v
		}
	}
	return
}

func SumMapInt(m map[string]int32) (sum int32) {
	for _, v := range m {
		sum += v
	}
	return sum
}

func MergeMapMin(a, b map[string]int32) map[string]int32 {
	for k, v := range a {
		if value, isok := b[k]; !isok || value > v {
			b[k] = v
		}
	}

	return b
}

func RemoveDuplicateElement(input []string) (output []string) {
	tmp := make(map[string]bool)
	for _, v := range input {
		tmp[v] = true
	}
	for key := range tmp {
		output = append(output, key)
	}
	return
}

func CompareMap(s []string, m map[string]string) bool {
	d := make(map[string]string)
	for _, label := range s {
		l := strings.Split(label, "=")
		if len(l) == 2 {
			d[l[0]] = d[l[1]]
		}
	}
	return reflect.DeepEqual(d, m)
}

func MapToString(m map[string]string) string {
	b := new(bytes.Buffer)
	for k, v := range m {
		fmt.Fprintf(b, "%s=%s,", k, v)
	}
	return b.String()
}

func StringsToMap(s []string) map[string]string {
	result := make(map[string]string)
	for _, str := range s {
		data := strings.Split(str, "=")
		if len(data) != 2 {
			continue
		} else {
			result[data[0]] = data[1]
		}
	}
	return result
}

func GetIndex(array []string, s string) int {
	for i, v := range array {
		if s == v {
			return i
		}
	}
	return -1
}

func GetMapsIndex(array []map[string]string, m map[string]string) int {
	for i, v := range array {
		if reflect.DeepEqual(v, m) {
			return i
		}
	}
	return -1
}
