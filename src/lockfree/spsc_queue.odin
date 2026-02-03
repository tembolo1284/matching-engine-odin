package lockfree

import "core:sync"

// =============================================================================
// Single-Producer Single-Consumer Lock-Free Queue
// =============================================================================
// Design matches C matching-engine lockfree_queue.h:
// - Fixed-size ring buffer (power of 2 for fast masking)
// - Cache-line padding to prevent false sharing
// - Lock-free using atomic operations
// - Batch dequeue to amortize atomic overhead
// =============================================================================

CACHE_LINE_SIZE :: 64
DEFAULT_QUEUE_CAPACITY :: 65536

// =============================================================================
// SPSC Queue Structure
// =============================================================================

Producer_Stats :: struct {
	total_enqueues:  u64,
	failed_enqueues: u64,
	peak_size:       u64,
}

Consumer_Stats :: struct {
	total_dequeues: u64,
	batch_dequeues: u64,
}

// Generic SPSC queue with compile-time capacity
// $T = element type, $N = capacity (must be power of 2)
SPSC_Queue :: struct($T: typeid, $N: u32) where N > 0, (N & (N - 1)) == 0 {
	// Head index (consumer side) - own cache line
	head:      u64,
	_pad_head: [CACHE_LINE_SIZE - 8]u8,
	
	// Tail index (producer side) - own cache line
	tail:      u64,
	_pad_tail: [CACHE_LINE_SIZE - 8]u8,
	
	// Producer statistics - own cache line
	prod_stats: Producer_Stats,
	_pad_prod:  [CACHE_LINE_SIZE - size_of(Producer_Stats)]u8,
	
	// Consumer statistics - own cache line
	cons_stats: Consumer_Stats,
	_pad_cons:  [CACHE_LINE_SIZE - size_of(Consumer_Stats)]u8,
	
	// Ring buffer storage
	buffer: [N]T,
}

// =============================================================================
// Initialization
// =============================================================================

spsc_init :: proc(q: ^SPSC_Queue($T, $N)) {
	sync.atomic_store(&q.head, u64(0))
	sync.atomic_store(&q.tail, u64(0))
	q.prod_stats = Producer_Stats{}
	q.cons_stats = Consumer_Stats{}
}

// =============================================================================
// Producer Operations
// =============================================================================

spsc_enqueue :: proc(q: ^SPSC_Queue($T, $N), item: ^T) -> bool {
	mask := u64(N - 1)
	
	current_tail := sync.atomic_load(&q.tail)
	next_tail := (current_tail + 1) & mask
	
	current_head := sync.atomic_load(&q.head)
	
	if next_tail == current_head {
		q.prod_stats.failed_enqueues += 1
		return false
	}
	
	q.buffer[current_tail] = item^
	
	sync.atomic_store(&q.tail, next_tail)
	
	q.prod_stats.total_enqueues += 1
	
	current_size := (next_tail - current_head) & mask
	if current_size > q.prod_stats.peak_size {
		q.prod_stats.peak_size = current_size
	}
	
	return true
}

// =============================================================================
// Consumer Operations
// =============================================================================

spsc_dequeue :: proc(q: ^SPSC_Queue($T, $N), item: ^T) -> bool {
	mask := u64(N - 1)
	
	current_head := sync.atomic_load(&q.head)
	current_tail := sync.atomic_load(&q.tail)
	
	if current_head == current_tail {
		return false
	}
	
	item^ = q.buffer[current_head]
	
	sync.atomic_store(&q.head, (current_head + 1) & mask)
	
	q.cons_stats.total_dequeues += 1
	
	return true
}

spsc_dequeue_batch :: proc(q: ^SPSC_Queue($T, $N), items: []T, max_items: u32) -> u32 {
	if max_items == 0 || len(items) == 0 {
		return 0
	}
	
	mask := u64(N - 1)
	
	head := sync.atomic_load(&q.head)
	tail := sync.atomic_load(&q.tail)
	
	available := u32((tail - head) & mask)
	items_len := u32(len(items))
	to_dequeue := available
	if max_items < to_dequeue {
		to_dequeue = max_items
	}
	if items_len < to_dequeue {
		to_dequeue = items_len
	}
	
	if to_dequeue == 0 {
		return 0
	}
	
	for i: u32 = 0; i < to_dequeue; i += 1 {
		items[i] = q.buffer[(head + u64(i)) & mask]
	}
	
	sync.atomic_store(&q.head, (head + u64(to_dequeue)) & mask)
	
	q.cons_stats.total_dequeues += u64(to_dequeue)
	q.cons_stats.batch_dequeues += 1
	
	return to_dequeue
}

// =============================================================================
// Query Operations
// =============================================================================

spsc_is_empty :: proc(q: ^SPSC_Queue($T, $N)) -> bool {
	head := sync.atomic_load(&q.head)
	tail := sync.atomic_load(&q.tail)
	return head == tail
}

spsc_size :: proc(q: ^SPSC_Queue($T, $N)) -> u32 {
	mask := u64(N - 1)
	head := sync.atomic_load(&q.head)
	tail := sync.atomic_load(&q.tail)
	return u32((tail - head) & mask)
}

spsc_capacity :: proc(q: ^SPSC_Queue($T, $N)) -> u32 {
	return N - 1
}

spsc_get_stats :: proc(q: ^SPSC_Queue($T, $N)) -> (total_enq: u64, total_deq: u64, failed_enq: u64, peak: u64) {
	return q.prod_stats.total_enqueues,
	       q.cons_stats.total_dequeues,
	       q.prod_stats.failed_enqueues,
	       q.prod_stats.peak_size
}
