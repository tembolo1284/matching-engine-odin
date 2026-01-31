package protocol

import "../types"

// =============================================================================
// Binary Protocol Messages
// =============================================================================
// Wire-compatible with C/Zig/Rust matching engine implementations
//
// Wire format:
//   Byte 0:     Magic (0x4D = 'M')
//   Byte 1:     Message type (ASCII char)
//   Byte 2+:    Payload (type-specific, BIG-ENDIAN)
//
// All structs are #packed for exact wire format
// Rule 9: No hidden pointers - all data is inline
// =============================================================================

// Symbol type: 8 bytes, null-padded
Symbol :: distinct [MAX_SYMBOL_LENGTH]u8

EMPTY_SYMBOL :: Symbol{}

// =============================================================================
// Side enum (wire: 'B' = 0x42, 'S' = 0x53)
// =============================================================================

Side :: enum u8 {
	Buy  = 'B',  // 0x42
	Sell = 'S',  // 0x53
}

// =============================================================================
// Reject Reason (wire values 1-10)
// =============================================================================

Reject_Reason :: enum u8 {
	Unknown_Symbol     = 1,
	Invalid_Quantity   = 2,
	Invalid_Price      = 3,
	Order_Not_Found    = 4,
	Duplicate_Order_Id = 5,
	Pool_Exhausted     = 6,
	Unauthorized       = 7,
	Throttled          = 8,
	Book_Full          = 9,
	Invalid_Order_Id   = 10,
}

// =============================================================================
// Client -> Server Messages
// =============================================================================

// New Order (27 bytes total)
// Wire: magic(1) + type(1) + user_id(4) + symbol(8) + price(4) + qty(4) + side(1) + order_id(4)
New_Order :: struct #packed {
	user_id:       u32,        // 4 bytes - big-endian
	symbol:        Symbol,     // 8 bytes
	price:         u32,        // 4 bytes - big-endian
	quantity:      u32,        // 4 bytes - big-endian
	side:          Side,       // 1 byte
	user_order_id: u32,        // 4 bytes - big-endian
}

#assert(size_of(New_Order) == NEW_ORDER_WIRE_SIZE - HEADER_SIZE)

// Cancel Order (10 bytes total)
// Wire: magic(1) + type(1) + user_id(4) + order_id(4)
// Note: No symbol - looked up server-side from order tracking
Cancel_Order :: struct #packed {
	user_id:       u32,        // 4 bytes - big-endian
	user_order_id: u32,        // 4 bytes - big-endian
}

#assert(size_of(Cancel_Order) == CANCEL_WIRE_SIZE - HEADER_SIZE)

// Flush (2 bytes total - header only)
// Wire: magic(1) + type(1)
Flush :: struct #packed {
	// No payload
}

// =============================================================================
// Server -> Client Messages
// =============================================================================

// Ack (18 bytes total)
// Wire: magic(1) + type(1) + symbol(8) + user_id(4) + order_id(4)
Ack :: struct #packed {
	symbol:        Symbol,     // 8 bytes
	user_id:       u32,        // 4 bytes - big-endian
	user_order_id: u32,        // 4 bytes - big-endian
}

#assert(size_of(Ack) == ACK_WIRE_SIZE - HEADER_SIZE)

// Cancel Ack (18 bytes total)
// Wire: magic(1) + type(1) + symbol(8) + user_id(4) + order_id(4)
Cancel_Ack :: struct #packed {
	symbol:        Symbol,     // 8 bytes
	user_id:       u32,        // 4 bytes - big-endian
	user_order_id: u32,        // 4 bytes - big-endian
}

#assert(size_of(Cancel_Ack) == CANCEL_ACK_WIRE_SIZE - HEADER_SIZE)

// Trade (34 bytes total)
// Wire: magic(1) + type(1) + symbol(8) + buy_uid(4) + buy_oid(4) + sell_uid(4) + sell_oid(4) + price(4) + qty(4)
Trade :: struct #packed {
	symbol:        Symbol,     // 8 bytes
	buy_user_id:   u32,        // 4 bytes - big-endian
	buy_order_id:  u32,        // 4 bytes - big-endian
	sell_user_id:  u32,        // 4 bytes - big-endian
	sell_order_id: u32,        // 4 bytes - big-endian
	price:         u32,        // 4 bytes - big-endian
	quantity:      u32,        // 4 bytes - big-endian
}

#assert(size_of(Trade) == TRADE_WIRE_SIZE - HEADER_SIZE)

// Top of Book (20 bytes total)
// Wire: magic(1) + type(1) + symbol(8) + side(1) + price(4) + qty(4) + pad(1)
Top_Of_Book :: struct #packed {
	symbol:   Symbol,          // 8 bytes
	side:     Side,            // 1 byte
	price:    u32,             // 4 bytes - big-endian
	quantity: u32,             // 4 bytes - big-endian
	_pad:     u8,              // 1 byte padding
}

#assert(size_of(Top_Of_Book) == TOP_OF_BOOK_WIRE_SIZE - HEADER_SIZE)

// Reject (19 bytes total)
// Wire: magic(1) + type(1) + symbol(8) + user_id(4) + order_id(4) + reason(1)
Reject :: struct #packed {
	symbol:        Symbol,         // 8 bytes
	user_id:       u32,            // 4 bytes - big-endian
	user_order_id: u32,            // 4 bytes - big-endian
	reason:        Reject_Reason,  // 1 byte
}

#assert(size_of(Reject) == REJECT_WIRE_SIZE - HEADER_SIZE)

// =============================================================================
// Helper Functions
// =============================================================================

// Create a symbol from a string (null-padded)
make_symbol :: proc(s: string) -> Symbol {
	result := Symbol{}
	copy_len := min(len(s), MAX_SYMBOL_LENGTH)
	for i := 0; i < copy_len; i += 1 {
		result[i] = s[i]
	}
	return result
}

// Check if symbol is empty (all zeros)
symbol_is_empty :: proc(sym: ^Symbol) -> bool {
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		if sym[i] != 0 {
			return false
		}
	}
	return true
}

// Compare two symbols
symbol_equal :: proc(a: ^Symbol, b: ^Symbol) -> bool {
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Validate magic byte
validate_magic :: proc(byte: u8) -> types.Error {
	if byte != MAGIC {
		return .Invalid_Magic
	}
	return .None
}

// Parse side from wire byte (security: validate before use)
parse_side :: proc(byte: u8) -> (Side, bool) {
	switch byte {
	case 'B':
		return .Buy, true
	case 'S':
		return .Sell, true
	case:
		return .Buy, false  // Invalid
	}
}

// Parse reject reason from wire byte (security: validate before use)
parse_reject_reason :: proc(byte: u8) -> (Reject_Reason, bool) {
	if byte >= 1 && byte <= 10 {
		return Reject_Reason(byte), true
	}
	return .Unknown_Symbol, false  // Invalid
}
