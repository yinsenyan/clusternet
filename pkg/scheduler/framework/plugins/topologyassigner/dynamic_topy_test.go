package topologyassigner

import (
	"testing"
)

func Test_Dynamic_One(t *testing.T) {
	t.Run("a", func(t *testing.T) {
		result := dynamicDivideReplicas(10, []int32{10, 10, 10, 10})
		t.Error("got result is : ", result)
	})
}
