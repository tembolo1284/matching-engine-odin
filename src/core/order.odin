package core

import "../types"
import "../memory"
import "../protocol"

// =============================================================================
// Order Structure - Cache-Line Aligned
// =============================================================================
// From the LinkedIn post: "All that lost PnL came down to orders sitting too
// close together in memory."
//
// Each Order is aligned to 64 bytes (cache line) to prevent false sharing
// when multiple cores update adjacent orders.
// =============================================================================

CACHE_LINE_SIZE :: 64

// Order represents a single order in the book
// Aligned to cache line boundary to prevent false sharing
Order :: struct #align(CACHE_LINE_SIZE) {
	// Identifiers (12 bytes)
	user_id:       u32,                   // Client user ID
	user_order_id: u32,                   // Client-assigned order ID
	server_id:     u32,                   // Server-assigned internal ID
	
	// Symbol (8 bytes)
	symbol:        protocol.Symbol,
	
	// Pricing (8 bytes) - using u32 to match wire format
	price:         u32,                   // Price (integer, e.g., cents)
	quantity:      u32,                   // Original quantity
	
	// State (8 bytes)
	remaining:     u32,                   // Remaining quantity
	timestamp:     u32,                   // Entry timestamp (for time priority)
	
	// Classification (4 bytes)
	side:          protocol.Side,         // Buy/Sell (wire format)
	order_type:    types.Order_Type,      // Limit/Market/IOC/FOK
	status:        types.Status,          // Current status
	_pad1:         u8,                    // Explicit padding
	
	// Linked list pointers for price level (8 bytes)
	// These are pool handles, not raw pointers (Rule 9)
	next:          memory.Handle,         // Next order at same price
	prev:          memory.Handle,         // Previous order at same price
	
	// Padding to 64 bytes (16 bytes)
	_pad2:         [16]u8,
}

// Compile-time assertion: Order must be exactly 64 bytes
#assert(size_of(Order) == CACHE_LINE_SIZE)

// =============================================================================
// Order Operations
// =============================================================================

// Initialize a new order
order_init :: proc(
	order:         ^Order,
	user_id:       u32,
	user_order_id: u32,
	server_id:     u32,
	symbol:        protocol.Symbol,
	price:         u32,
	quantity:      u32,
	side:          protocol.Side,
	order_type:    types.Order_Type,
	timestamp:     u32,
) {
	order.user_id       = user_id
	order.user_order_id = user_order_id
	order.server_id     = server_id
	order.symbol        = symbol
	order.price         = price
	order.quantity      = quantity
	order.remaining     = quantity
	order.timestamp     = timestamp
	order.side          = side
	order.order_type    = order_type
	order.status        = .New
	order.next          = memory.NULL_HANDLE
	order.prev          = memory.NULL_HANDLE
}

// Fill an order (partially or fully)
// Returns the filled quantity
order_fill :: #force_inline proc(order: ^Order, fill_qty: u32) -> u32 {
	actual_fill := min(fill_qty, order.remaining)
	order.remaining -= actual_fill
	
	if order.remaining == 0 {
		order.status = .Filled
	} else {
		order.status = .Partial
	}
	
	return actual_fill
}

// Cancel an order
order_cancel :: #force_inline proc(order: ^Order) {
	order.status = .Cancelled
}

// Check if order can match at given price
// Buy orders match at or below their limit
// Sell orders match at or above their limit
order_can_match :: #force_inline proc(order: ^Order, match_price: u32) -> bool {
	if order.side == .Buy {
		return order.price >= match_price
	} else {
		return order.price <= match_price
	}
}

// Check if order is still active (can be matched)
order_is_active :: #force_inline proc(order: ^Order) -> bool {
	return types.is_active(order.status) && order.remaining > 0
}

// Check if order is fully filled
order_is_filled :: #force_inline proc(order: ^Order) -> bool {
	return order.status == .Filled || order.remaining == 0
}

// Get the unfilled quantity
order_leaves_qty :: #force_inline proc(order: ^Order) -> u32 {
	return order.remaining
}
