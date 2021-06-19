package pkg

import "container/heap"

type minHeap []uint64

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(uint64))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Heap struct {
	min minHeap
}

func (h *Heap) Push(ack uint64) {
	heap.Push(&h.min, ack)
}

func (h *Heap) Pop() (uint64, bool) {
	min, ok := heap.Pop(&h.min).(uint64)
	return min, ok
}

func (h *Heap) Peek() (uint64, bool) {
	if h.Len() != 0 {
		return h.min[0], true
	}
	return 0, false
}

func (h *Heap) Len() int {
	return len(h.min)
}
