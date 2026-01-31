package memory

import "../types"
import "../util"

// =============================================================================
// Fixed-Size Object Pool
// =============================================================================
// Rule 3: Do not use dynamic memory allocation after initialization
//
// All objects are pre-allocated at startup. The pool provides O(1) alloc/free
// using a free list. No heap allocation on the hot path.
// =============================================================================

// Pool handle - index into the pool (0 is reserved as null)
Handle :: distinct u32

NULL_HANDLE :: Handle(0)

// Pool of fixed-size objects
// $T is the object type, $N is the capacity
Pool :: struct($T: typeid, $N: u32) where N > 0 {
	// Storage for objects (index 0 unused - reserved for null handle)
	storage:    [N + 1]T,
	
	// Free list - each entry points to next free slot
	// When allocated, the slot is removed from free list
	free_list:  [N + 1]Handle,
	
	// Head of free list (next available slot)
	free_head:  Handle,
	
	// Statistics
	allocated:  u32,
	high_water: u32,
}

// Initialize the pool - must be called before use
// Rule 3: This is the ONLY place we "allocate" - at init time
pool_init :: proc(pool: ^Pool($T, $N)) -> types.Error {
	// Assertion: pool pointer valid (Rule 5)
	if !util.ensure(pool != nil, "pool pointer is nil") {
		return .Internal
	}
	
	// Build free list: each slot points to the next
	// Slot 0 is reserved (NULL_HANDLE)
	for i: u32 = 1; i <= N; i += 1 {
		pool.free_list[i] = Handle(i + 1)
	}
	pool.free_list[N] = NULL_HANDLE  // End of list
	
	pool.free_head = Handle(1)       // First free slot
	pool.allocated = 0
	pool.high_water = 0
	
	// Assertion: pool initialized correctly (Rule 5)
	if !util.ensure(pool.free_head != NULL_HANDLE, "pool init failed") {
		return .Internal
	}
	
	return .None
}

// Allocate an object from the pool
// Returns NULL_HANDLE if pool is exhausted
pool_alloc :: proc(pool: ^Pool($T, $N)) -> (Handle, types.Error) {
	// Assertion: pool valid (Rule 5)
	if !util.ensure(pool != nil, "pool pointer is nil") {
		return NULL_HANDLE, .Internal
	}
	
	// Check if pool is exhausted
	if pool.free_head == NULL_HANDLE {
		return NULL_HANDLE, .Pool_Exhausted
	}
	
	// Pop from free list
	handle := pool.free_head
	pool.free_head = pool.free_list[handle]
	
	// Clear the allocated slot
	pool.storage[handle] = {}
	
	// Update stats
	pool.allocated += 1
	if pool.allocated > pool.high_water {
		pool.high_water = pool.allocated
	}
	
	// Assertion: valid handle returned (Rule 5)
	if !util.ensure(handle != NULL_HANDLE, "allocated null handle") {
		return NULL_HANDLE, .Internal
	}
	
	return handle, .None
}

// Free an object back to the pool
pool_free :: proc(pool: ^Pool($T, $N), handle: Handle) -> types.Error {
	// Assertion: pool valid (Rule 5)
	if !util.ensure(pool != nil, "pool pointer is nil") {
		return .Internal
	}
	
	// Assertion: handle valid (Rule 5)
	if !util.ensure(handle != NULL_HANDLE, "freeing null handle") {
		return .Pool_Invalid_Handle
	}
	if !util.ensure(u32(handle) <= N, "handle out of range") {
		return .Pool_Invalid_Handle
	}
	
	// Push onto free list
	pool.free_list[handle] = pool.free_head
	pool.free_head = handle
	pool.allocated -= 1
	
	return .None
}

// Get pointer to object by handle
// Returns nil for invalid handles
pool_get :: #force_inline proc(pool: ^Pool($T, $N), handle: Handle) -> ^T {
	if handle == NULL_HANDLE || u32(handle) > N {
		return nil
	}
	return &pool.storage[handle]
}

// Get pointer to object (unchecked - for hot path after validation)
pool_get_unchecked :: #force_inline proc(pool: ^Pool($T, $N), handle: Handle) -> ^T {
	return &pool.storage[handle]
}

// Check if handle is valid
pool_is_valid :: #force_inline proc(pool: ^Pool($T, $N), handle: Handle) -> bool {
	return handle != NULL_HANDLE && u32(handle) <= N
}

// Get current allocation count
pool_count :: #force_inline proc(pool: ^Pool($T, $N)) -> u32 {
	return pool.allocated
}

// Get remaining capacity
pool_remaining :: #force_inline proc(pool: ^Pool($T, $N)) -> u32 {
	return N - pool.allocated
}

// Check if pool is full
pool_is_full :: #force_inline proc(pool: ^Pool($T, $N)) -> bool {
	return pool.free_head == NULL_HANDLE
}

// Check if pool is empty
pool_is_empty :: #force_inline proc(pool: ^Pool($T, $N)) -> bool {
	return pool.allocated == 0
}
