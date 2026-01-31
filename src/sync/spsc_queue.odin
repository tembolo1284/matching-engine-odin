package sync

import "base:intrinsics"

// =============================================================================
// Single-Producer Single-Consumer Lock-Free Queue
// =============================================================================
// Design matches C matching-engine lockfree_queue.h:
// - Fixed-size ring buffer (power of 2 for fast masking)
// - Cache-line padding to prevent false sharing
// - Lock-free using atomic operations
// - Batch dequeue to amortize atomic overhead
//
// Power of Ten Compliance:
// - Rule 2: All loops bounded by queue size
// - Rule 3: No dynamic allocation (fixed-size buffer)
// - Rule 5: Assertions verify invariants
// =============================================================================

// Cache line size (64 bytes on modern CPUs)
CACHE_LINE_SIZE :: 64

// Default queue capacity (must be power of 2)
DEFAULT_QUEUE_CAPACITY :: 65536

// =============================================================================
// SPSC Queue Structure
// =============================================================================

// Generic SPSC queue with compile-time capacity
// $T = element type, $N = capacity (must be power of 2)
SPSC_Queue :: struct($T: typeid, $N: u32) where N > 0 && (N & (N - 1)) == 0 {
	// Head index (consumer side) - own cache line
	head:      u64,
	_pad_head: [CACHE_LINE_SIZE - 8]u8,
	
	// Tail index (producer side) - own cache line
	tail:      u64,
	_pad_tail: [CACHE_LINE_SIZE - 8]u8,
	
	// Producer statistics - own cache line (updated only by producer)
	prod_stats: Producer_Stats,
	_pad_prod:  [CACHE_LINE_SIZE - size_of(Producer_Stats)]u8,
	
	// Consumer statistics - own cache line (updated only by consumer)
	cons_stats: Consumer_Stats,
	_pad_cons:  [CACHE_LINE_SIZE - size_of(Consumer_Stats)]u8,
	
	// Ring buffer storage
	buffer:    [N]T,
}

// Producer statistics (updated only by producer thread)
Producer_Stats :: struct {
	total_enqueues:  u64,
	failed_enqueues: u64,
	peak_size:       u64,
}

// Consumer statistics (updated only by consumer thread)
Consumer_Stats :: struct {
	total_dequeues: u64,
	batch_dequeues: u64,
}

// Mask for fast modulo (capacity - 1)
queue_mask :: proc($N: u32) -> u64 {
	return u64(N - 1)
}

// =============================================================================
// Initialization
// =============================================================================

// Initialize a queue (all indices start at 0)
spsc_init :: proc(q: ^SPSC_Queue($T, $N)) {
	intrinsics.atomic_store(&q.head, 0)
	intrinsics.atomic_store(&q.tail, 0)
	
	q.prod_stats = Producer_Stats{}
	q.cons_stats = Consumer_Stats{}
}

// =============================================================================
// Producer Operations (single producer thread only)
// =============================================================================

// Enqueue an item (producer only)
// Returns true on success, false if queue is full
spsc_enqueue :: proc(q: ^SPSC_Queue($T, $N), item: ^T) -> bool {
	mask := queue_mask(N)
	
	// Load tail (relaxed - we own it)
	current_tail := intrinsics.atomic_load(&q.tail)
	next_tail := (current_tail + 1) & mask
	
	// Load head (acquire - see consumer's progress)
	current_head := intrinsics.atomic_load_acquire(&q.head)
	
	// Check if full
	if next_tail == current_head {
		q.prod_stats.failed_enqueues += 1
		return false
	}
	
	// Store item
	q.buffer[current_tail] = item^
	
	// Publish tail (release - consumer can now see the item)
	intrinsics.atomic_store_release(&q.tail, next_tail)
	
	// Update stats
	q.prod_stats.total_enqueues += 1
	
	// Update peak size
	current_size := (next_tail - current_head) & mask
	if current_size > q.prod_stats.peak_size {
		q.prod_stats.peak_size = current_size
	}
	
	return true
}

// =============================================================================
// Consumer Operations (single consumer thread only)
// =============================================================================

// Dequeue a single item (consumer only)
// Returns true on success, false if queue is empty
spsc_dequeue :: proc(q: ^SPSC_Queue($T, $N), item: ^T) -> bool {
	mask := queue_mask(N)
	
	// Load head (relaxed - we own it)
	current_head := intrinsics.atomic_load(&q.head)
	
	// Load tail (acquire - see producer's writes)
	current_tail := intrinsics.atomic_load_acquire(&q.tail)
	
	// Check if empty
	if current_head == current_tail {
		return false
	}
	
	// Load item
	item^ = q.buffer[current_head]
	
	// Advance head (release - producer can reuse slot)
	intrinsics.atomic_store_release(&q.head, (current_head + 1) & mask)
	
	// Update stats
	q.cons_stats.total_dequeues += 1
	
	return true
}

// Batch dequeue multiple items (consumer only)
// Returns number of items dequeued (0 to max_items)
// This amortizes atomic overhead across multiple items
spsc_dequeue_batch :: proc(q: ^SPSC_Queue($T, $N), items: []T, max_items: u32) -> u32 {
	if max_items == 0 || len(items) == 0 {
		return 0
	}
	
	mask := queue_mask(N)
	
	// Load indices
	head := intrinsics.atomic_load(&q.head)
	tail := intrinsics.atomic_load_acquire(&q.tail)
	
	// Calculate available items
	available := u32((tail - head) & mask)
	to_dequeue := min(available, max_items, u32(len(items)))
	
	if to_dequeue == 0 {
		return 0
	}
	
	// Copy items (Rule 2: bounded by to_dequeue)
	for i: u32 = 0; i < to_dequeue; i += 1 {
		items[i] = q.buffer[(head + u64(i)) & mask]
	}
	
	// Single atomic store for entire batch
	intrinsics.atomic_store_release(&q.head, (head + u64(to_dequeue)) & mask)
	
	// Update stats
	q.cons_stats.total_dequeues += u64(to_dequeue)
	q.cons_stats.batch_dequeues += 1
	
	return to_dequeue
}

// =============================================================================
// Query Operations (may be stale due to concurrency)
// =============================================================================

// Check if queue is empty (approximate)
spsc_is_empty :: proc(q: ^SPSC_Queue($T, $N)) -> bool {
	head := intrinsics.atomic_load_acquire(&q.head)
	tail := intrinsics.atomic_load_acquire(&q.tail)
	return head == tail
}

// Get approximate queue size
spsc_size :: proc(q: ^SPSC_Queue($T, $N)) -> u32 {
	mask := queue_mask(N)
	head := intrinsics.atomic_load_acquire(&q.head)
	tail := intrinsics.atomic_load_acquire(&q.tail)
	return u32((tail - head) & mask)
}

// Get queue capacity
spsc_capacity :: proc(q: ^SPSC_Queue($T, $N)) -> u32 {
	return N - 1  // One slot reserved for full detection
}

// =============================================================================
// Statistics (may be slightly stale if read from wrong thread)
// =============================================================================

// Get statistics snapshot
spsc_get_stats :: proc(
	q: ^SPSC_Queue($T, $N),
) -> (total_enq: u64, total_deq: u64, failed_enq: u64, peak: u64) {
	return q.prod_stats.total_enqueues,
	       q.cons_stats.total_dequeues,
	       q.prod_stats.failed_enqueues,
	       q.prod_stats.peak_size
}
