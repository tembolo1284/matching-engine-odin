package core

import "../types"
import "../memory"
import "../protocol"
import "../util"

// =============================================================================
// Price Level
// =============================================================================
// A price level holds all orders at a single price point.
// Orders are stored in a doubly-linked list for O(1) insertion/removal.
// FIFO ordering: oldest order at head, newest at tail.
//
// Rule 2: All loops bounded by MAX_ORDERS_PER_LEVEL
// Rule 3: No dynamic allocation - uses pool handles
// Rule 5: Assertions on all operations
// =============================================================================

// Maximum orders per price level (Rule 2: bounded loops)
MAX_ORDERS_PER_LEVEL :: protocol.MAX_ORDERS_PER_LEVEL

// Price level structure
Price_Level :: struct {
	price:       u32,                // Price for this level
	side:        protocol.Side,      // Buy or Sell
	
	// Doubly-linked list of orders (pool handles, not pointers)
	head:        memory.Handle,      // First order (oldest, highest priority)
	tail:        memory.Handle,      // Last order (newest, lowest priority)
	
	// Aggregate quantities
	order_count: u32,                // Number of orders at this level
	total_qty:   u32,                // Sum of all remaining quantities
}

// =============================================================================
// Price Level Operations
// =============================================================================

// Initialize a price level
level_init :: proc(level: ^Price_Level, price: u32, side: protocol.Side) {
	level.price       = price
	level.side        = side
	level.head        = memory.NULL_HANDLE
	level.tail        = memory.NULL_HANDLE
	level.order_count = 0
	level.total_qty   = 0
}

// Add an order to the tail of the level (FIFO: new orders go to back)
// Order pool is passed in to access order data
level_add_order :: proc(
	level: ^Price_Level,
	order_handle: memory.Handle,
	pool: ^memory.Pool(Order, $N),
) -> types.Error {
	// Rule 5: Validate inputs
	if !util.ensure(level != nil, "level is nil") {
		return .Internal
	}
	if !util.ensure(order_handle != memory.NULL_HANDLE, "null order handle") {
		return .Pool_Invalid_Handle
	}
	
	// Rule 2: Check bounded limit
	if level.order_count >= MAX_ORDERS_PER_LEVEL {
		return .Price_Level_Full
	}
	
	order := memory.pool_get(pool, order_handle)
	if !util.ensure(order != nil, "invalid order handle") {
		return .Pool_Invalid_Handle
	}
	
	// Rule 5: Verify order price matches level
	if !util.ensure(order.price == level.price, "order price mismatch") {
		return .Order_Invalid_Price
	}
	
	// Link order to tail of list
	order.prev = level.tail
	order.next = memory.NULL_HANDLE
	
	if level.tail != memory.NULL_HANDLE {
		// List not empty - link previous tail to new order
		tail_order := memory.pool_get(pool, level.tail)
		if tail_order != nil {
			tail_order.next = order_handle
		}
	} else {
		// List was empty - this is also the head
		level.head = order_handle
	}
	
	level.tail = order_handle
	level.order_count += 1
	level.total_qty += order.remaining
	
	return .None
}

// Remove an order from the level (can be anywhere in the list)
level_remove_order :: proc(
	level: ^Price_Level,
	order_handle: memory.Handle,
	pool: ^memory.Pool(Order, $N),
) -> types.Error {
	// Rule 5: Validate inputs
	if !util.ensure(level != nil, "level is nil") {
		return .Internal
	}
	if !util.ensure(order_handle != memory.NULL_HANDLE, "null order handle") {
		return .Pool_Invalid_Handle
	}
	
	order := memory.pool_get(pool, order_handle)
	if !util.ensure(order != nil, "invalid order handle") {
		return .Pool_Invalid_Handle
	}
	
	// Unlink from previous
	if order.prev != memory.NULL_HANDLE {
		prev_order := memory.pool_get(pool, order.prev)
		if prev_order != nil {
			prev_order.next = order.next
		}
	} else {
		// This was the head
		level.head = order.next
	}
	
	// Unlink from next
	if order.next != memory.NULL_HANDLE {
		next_order := memory.pool_get(pool, order.next)
		if next_order != nil {
			next_order.prev = order.prev
		}
	} else {
		// This was the tail
		level.tail = order.prev
	}
	
	// Update aggregates
	level.order_count -= 1
	if order.remaining <= level.total_qty {
		level.total_qty -= order.remaining
	} else {
		level.total_qty = 0  // Safety: don't underflow
	}
	
	// Clear order's links
	order.prev = memory.NULL_HANDLE
	order.next = memory.NULL_HANDLE
	
	return .None
}

// Update total quantity after a partial fill
// Call this after modifying order.remaining directly
level_update_qty :: proc(level: ^Price_Level, old_qty: u32, new_qty: u32) {
	if old_qty >= new_qty {
		delta := old_qty - new_qty
		if delta <= level.total_qty {
			level.total_qty -= delta
		} else {
			level.total_qty = 0
		}
	}
}

// Get the first (highest priority) order at this level
level_peek_head :: #force_inline proc(level: ^Price_Level) -> memory.Handle {
	return level.head
}

// Check if level is empty
level_is_empty :: #force_inline proc(level: ^Price_Level) -> bool {
	return level.head == memory.NULL_HANDLE
}

// Get order count
level_count :: #force_inline proc(level: ^Price_Level) -> u32 {
	return level.order_count
}

// Get total quantity at this level
level_total_quantity :: #force_inline proc(level: ^Price_Level) -> u32 {
	return level.total_qty
}

// =============================================================================
// Iteration (for matching)
// =============================================================================

// Iterator state for walking through orders at a price level
Level_Iterator :: struct {
	current: memory.Handle,
	count:   u32,            // Orders visited (for Rule 2 bound)
}

// Create iterator starting at head
level_iter_begin :: proc(level: ^Price_Level) -> Level_Iterator {
	return Level_Iterator{
		current = level.head,
		count   = 0,
	}
}

// Check if iterator has more orders
level_iter_valid :: #force_inline proc(iter: ^Level_Iterator) -> bool {
	// Rule 2: Bounded iteration
	return iter.current != memory.NULL_HANDLE && iter.count < MAX_ORDERS_PER_LEVEL
}

// Get current order and advance iterator
level_iter_next :: proc(
	iter: ^Level_Iterator,
	pool: ^memory.Pool(Order, $N),
) -> ^Order {
	if !level_iter_valid(iter) {
		return nil
	}
	
	order := memory.pool_get(pool, iter.current)
	if order == nil {
		iter.current = memory.NULL_HANDLE
		return nil
	}
	
	iter.current = order.next
	iter.count += 1
	
	return order
}

// Get current handle without advancing
level_iter_current :: #force_inline proc(iter: ^Level_Iterator) -> memory.Handle {
	return iter.current
}

// Advance iterator without returning order
level_iter_advance :: proc(
	iter: ^Level_Iterator,
	pool: ^memory.Pool(Order, $N),
) {
	if !level_iter_valid(iter) {
		return
	}
	
	order := memory.pool_get(pool, iter.current)
	if order != nil {
		iter.current = order.next
	} else {
		iter.current = memory.NULL_HANDLE
	}
	iter.count += 1
}
