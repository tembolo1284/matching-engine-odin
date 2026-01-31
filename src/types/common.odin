package types

// =============================================================================
// Common Types - Foundation for the matching engine
// =============================================================================
// Rule 6: Smallest possible scope - these are shared across modules
// Rule 9: No hidden pointers in typedefs
// =============================================================================

// Fixed-point price representation
// Stored as integer with 8 decimal places (1e8 scaling)
// Example: $123.45 stored as 12_345_000_000
Price :: distinct i64

// Quantity in base units (e.g., shares, contracts)
Quantity :: distinct u64

// Unique order identifier - monotonically increasing
Order_Id :: distinct u64

// Sequence number for protocol messages
Sequence :: distinct u64

// Nanosecond timestamp from monotonic clock
Timestamp :: distinct u64

// Symbol identifier - fixed 8 bytes, null-padded
Symbol :: distinct [8]u8

// =============================================================================
// Constants
// =============================================================================

PRICE_SCALE :: 100_000_000  // 1e8 - 8 decimal places
NULL_ORDER_ID :: Order_Id(0)
NULL_PRICE :: Price(0)
MAX_PRICE :: Price(max(i64))
MIN_PRICE :: Price(min(i64))

// =============================================================================
// Price Utilities (inline, no function call overhead on hot path)
// =============================================================================

// Convert float to fixed-point price
// Only use at boundaries (parsing), never on hot path
price_from_float :: #force_inline proc(f: f64) -> Price {
	return Price(i64(f * f64(PRICE_SCALE)))
}

// Convert fixed-point to float
// Only use for display/logging, never on hot path
price_to_float :: #force_inline proc(p: Price) -> f64 {
	return f64(p) / f64(PRICE_SCALE)
}

// Compare prices - returns -1, 0, or 1
price_cmp :: #force_inline proc(a: Price, b: Price) -> i32 {
	if i64(a) < i64(b) {
		return -1
	}
	if i64(a) > i64(b) {
		return 1
	}
	return 0
}
