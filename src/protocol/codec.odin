package protocol

import "../types"
import "../util"

// =============================================================================
// Binary Protocol Codec
// =============================================================================
// Wire-compatible with C/Zig/Rust matching engine implementations
//
// All multi-byte integers are BIG-ENDIAN (network byte order)
// Security: All enum values validated before use
// Rule 7: All buffer bounds checked before access
// =============================================================================

// =============================================================================
// Decode Results
// =============================================================================

// Result of decoding an input message
Input_Decode_Result :: struct {
	msg_type:       u8,           // MSG_NEW_ORDER, MSG_CANCEL, MSG_FLUSH
	new_order:      New_Order,    // Valid if msg_type == MSG_NEW_ORDER
	cancel:         Cancel_Order, // Valid if msg_type == MSG_CANCEL
	bytes_consumed: int,
}

// Result of decoding an output message
Output_Decode_Result :: struct {
	msg_type:       u8,           // MSG_ACK, MSG_TRADE, etc.
	ack:            Ack,
	cancel_ack:     Cancel_Ack,
	trade:          Trade,
	top_of_book:    Top_Of_Book,
	reject:         Reject,
	bytes_consumed: int,
}

// =============================================================================
// Big-Endian Helpers
// =============================================================================

// Read u32 from big-endian bytes
read_u32_big :: #force_inline proc(data: []u8) -> u32 {
	return u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3])
}

// Write u32 to big-endian bytes
write_u32_big :: #force_inline proc(buf: []u8, value: u32) {
	buf[0] = u8(value >> 24)
	buf[1] = u8(value >> 16)
	buf[2] = u8(value >> 8)
	buf[3] = u8(value)
}

// =============================================================================
// Input Message Encoding (Client -> Server)
// =============================================================================

// Encode a new order message
encode_new_order :: proc(order: ^New_Order, buf: []u8) -> (int, types.Error) {
	// Rule 5: Validate inputs
	if !util.ensure(order != nil, "order is nil") {
		return 0, .Internal
	}
	if !util.ensure(order.quantity > 0, "quantity must be > 0") {
		return 0, .Order_Invalid_Quantity
	}
	
	// Rule 7: Check buffer bounds
	if len(buf) < NEW_ORDER_WIRE_SIZE {
		return 0, .Message_Too_Large
	}
	
	pos := 0
	
	// Header
	buf[pos] = MAGIC
	pos += 1
	buf[pos] = MSG_NEW_ORDER
	pos += 1
	
	// user_id (big-endian)
	write_u32_big(buf[pos:], order.user_id)
	pos += 4
	
	// symbol (8 bytes)
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		buf[pos + i] = order.symbol[i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	// price (big-endian)
	write_u32_big(buf[pos:], order.price)
	pos += 4
	
	// quantity (big-endian)
	write_u32_big(buf[pos:], order.quantity)
	pos += 4
	
	// side
	buf[pos] = u8(order.side)
	pos += 1
	
	// user_order_id (big-endian)
	write_u32_big(buf[pos:], order.user_order_id)
	pos += 4
	
	// Rule 5: Verify output
	if !util.ensure(pos == NEW_ORDER_WIRE_SIZE, "encode size mismatch") {
		return 0, .Internal
	}
	
	return pos, .None
}

// Encode a cancel order message
encode_cancel :: proc(cancel: ^Cancel_Order, buf: []u8) -> (int, types.Error) {
	// Rule 5: Validate inputs
	if !util.ensure(cancel != nil, "cancel is nil") {
		return 0, .Internal
	}
	
	// Rule 7: Check buffer bounds
	if len(buf) < CANCEL_WIRE_SIZE {
		return 0, .Message_Too_Large
	}
	
	pos := 0
	
	// Header
	buf[pos] = MAGIC
	pos += 1
	buf[pos] = MSG_CANCEL
	pos += 1
	
	// user_id (big-endian)
	write_u32_big(buf[pos:], cancel.user_id)
	pos += 4
	
	// user_order_id (big-endian)
	write_u32_big(buf[pos:], cancel.user_order_id)
	pos += 4
	
	// Rule 5: Verify output
	if !util.ensure(pos == CANCEL_WIRE_SIZE, "encode size mismatch") {
		return 0, .Internal
	}
	
	return pos, .None
}

// Encode a flush message
encode_flush :: proc(buf: []u8) -> (int, types.Error) {
	if len(buf) < FLUSH_WIRE_SIZE {
		return 0, .Message_Too_Large
	}
	
	buf[0] = MAGIC
	buf[1] = MSG_FLUSH
	
	return FLUSH_WIRE_SIZE, .None
}

// =============================================================================
// Input Message Decoding (Client -> Server)
// =============================================================================

// Decode an input message from wire format
decode_input :: proc(data: []u8) -> (Input_Decode_Result, types.Error) {
	result := Input_Decode_Result{}
	
	// Rule 7: Check minimum size
	if len(data) < HEADER_SIZE {
		return result, .Incomplete_Read
	}
	
	// Validate magic
	if data[0] != MAGIC {
		return result, .Invalid_Magic
	}
	
	msg_type := data[1]
	result.msg_type = msg_type
	
	switch msg_type {
	case MSG_NEW_ORDER:
		if len(data) < NEW_ORDER_WIRE_SIZE {
			return result, .Incomplete_Read
		}
		
		order, err := decode_new_order(data)
		if err != .None {
			return result, err
		}
		result.new_order = order
		result.bytes_consumed = NEW_ORDER_WIRE_SIZE
		
	case MSG_CANCEL:
		if len(data) < CANCEL_WIRE_SIZE {
			return result, .Incomplete_Read
		}
		
		result.cancel = decode_cancel(data)
		result.bytes_consumed = CANCEL_WIRE_SIZE
		
	case MSG_FLUSH:
		result.bytes_consumed = FLUSH_WIRE_SIZE
		
	case:
		return result, .Invalid_Message_Type
	}
	
	return result, .None
}

// Decode new order from wire
decode_new_order :: proc(data: []u8) -> (New_Order, types.Error) {
	order := New_Order{}
	
	// Rule 5: Verify preconditions
	if !util.ensure(len(data) >= NEW_ORDER_WIRE_SIZE, "buffer too small") {
		return order, .Incomplete_Read
	}
	
	pos := HEADER_SIZE
	
	// user_id
	order.user_id = read_u32_big(data[pos:])
	pos += 4
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		order.symbol[i] = data[pos + i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	// price
	order.price = read_u32_big(data[pos:])
	pos += 4
	
	// quantity
	order.quantity = read_u32_big(data[pos:])
	pos += 4
	
	// side - SECURITY: validate before use
	side, valid := parse_side(data[pos])
	if !valid {
		return order, .Invalid_Message
	}
	order.side = side
	pos += 1
	
	// user_order_id
	order.user_order_id = read_u32_big(data[pos:])
	
	return order, .None
}

// Decode cancel from wire
decode_cancel :: proc(data: []u8) -> Cancel_Order {
	cancel := Cancel_Order{}
	
	pos := HEADER_SIZE
	
	cancel.user_id = read_u32_big(data[pos:])
	pos += 4
	
	cancel.user_order_id = read_u32_big(data[pos:])
	
	return cancel
}

// =============================================================================
// Output Message Encoding (Server -> Client)
// =============================================================================

// Encode an ack message
encode_ack :: proc(ack: ^Ack, buf: []u8) -> (int, types.Error) {
	if len(buf) < ACK_WIRE_SIZE {
		return 0, .Message_Too_Large
	}
	
	pos := 0
	
	buf[pos] = MAGIC
	pos += 1
	buf[pos] = MSG_ACK
	pos += 1
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		buf[pos + i] = ack.symbol[i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	// user_id
	write_u32_big(buf[pos:], ack.user_id)
	pos += 4
	
	// user_order_id
	write_u32_big(buf[pos:], ack.user_order_id)
	pos += 4
	
	return pos, .None
}

// Encode a cancel ack message
encode_cancel_ack :: proc(ack: ^Cancel_Ack, buf: []u8) -> (int, types.Error) {
	if len(buf) < CANCEL_ACK_WIRE_SIZE {
		return 0, .Message_Too_Large
	}
	
	pos := 0
	
	buf[pos] = MAGIC
	pos += 1
	buf[pos] = MSG_CANCEL_ACK
	pos += 1
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		buf[pos + i] = ack.symbol[i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	// user_id
	write_u32_big(buf[pos:], ack.user_id)
	pos += 4
	
	// user_order_id
	write_u32_big(buf[pos:], ack.user_order_id)
	pos += 4
	
	return pos, .None
}

// Encode a trade message
encode_trade :: proc(trade: ^Trade, buf: []u8) -> (int, types.Error) {
	// Rule 5: Validate trade data
	if !util.ensure(trade.quantity > 0, "trade quantity must be > 0") {
		return 0, .Order_Invalid_Quantity
	}
	if !util.ensure(trade.price > 0, "trade price must be > 0") {
		return 0, .Order_Invalid_Price
	}
	
	if len(buf) < TRADE_WIRE_SIZE {
		return 0, .Message_Too_Large
	}
	
	pos := 0
	
	buf[pos] = MAGIC
	pos += 1
	buf[pos] = MSG_TRADE
	pos += 1
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		buf[pos + i] = trade.symbol[i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	// buy_user_id
	write_u32_big(buf[pos:], trade.buy_user_id)
	pos += 4
	
	// buy_order_id
	write_u32_big(buf[pos:], trade.buy_order_id)
	pos += 4
	
	// sell_user_id
	write_u32_big(buf[pos:], trade.sell_user_id)
	pos += 4
	
	// sell_order_id
	write_u32_big(buf[pos:], trade.sell_order_id)
	pos += 4
	
	// price
	write_u32_big(buf[pos:], trade.price)
	pos += 4
	
	// quantity
	write_u32_big(buf[pos:], trade.quantity)
	pos += 4
	
	return pos, .None
}

// Encode a top of book message
encode_top_of_book :: proc(tob: ^Top_Of_Book, buf: []u8) -> (int, types.Error) {
	if len(buf) < TOP_OF_BOOK_WIRE_SIZE {
		return 0, .Message_Too_Large
	}
	
	pos := 0
	
	buf[pos] = MAGIC
	pos += 1
	buf[pos] = MSG_TOP_OF_BOOK
	pos += 1
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		buf[pos + i] = tob.symbol[i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	// side
	buf[pos] = u8(tob.side)
	pos += 1
	
	// price
	write_u32_big(buf[pos:], tob.price)
	pos += 4
	
	// quantity
	write_u32_big(buf[pos:], tob.quantity)
	pos += 4
	
	// padding byte
	buf[pos] = 0
	pos += 1
	
	return pos, .None
}

// Encode a reject message
encode_reject :: proc(reject: ^Reject, buf: []u8) -> (int, types.Error) {
	if len(buf) < REJECT_WIRE_SIZE {
		return 0, .Message_Too_Large
	}
	
	pos := 0
	
	buf[pos] = MAGIC
	pos += 1
	buf[pos] = MSG_REJECT
	pos += 1
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		buf[pos + i] = reject.symbol[i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	// user_id
	write_u32_big(buf[pos:], reject.user_id)
	pos += 4
	
	// user_order_id
	write_u32_big(buf[pos:], reject.user_order_id)
	pos += 4
	
	// reason
	buf[pos] = u8(reject.reason)
	pos += 1
	
	return pos, .None
}

// =============================================================================
// Output Message Decoding (Server -> Client)
// =============================================================================

// Decode an output message from wire format
decode_output :: proc(data: []u8) -> (Output_Decode_Result, types.Error) {
	result := Output_Decode_Result{}
	
	// Rule 7: Check minimum size
	if len(data) < HEADER_SIZE {
		return result, .Incomplete_Read
	}
	
	// Validate magic
	if data[0] != MAGIC {
		return result, .Invalid_Magic
	}
	
	msg_type := data[1]
	result.msg_type = msg_type
	
	switch msg_type {
	case MSG_ACK:
		if len(data) < ACK_WIRE_SIZE {
			return result, .Incomplete_Read
		}
		result.ack = decode_ack(data)
		result.bytes_consumed = ACK_WIRE_SIZE
		
	case MSG_CANCEL_ACK:
		if len(data) < CANCEL_ACK_WIRE_SIZE {
			return result, .Incomplete_Read
		}
		result.cancel_ack = decode_cancel_ack(data)
		result.bytes_consumed = CANCEL_ACK_WIRE_SIZE
		
	case MSG_TRADE:
		if len(data) < TRADE_WIRE_SIZE {
			return result, .Incomplete_Read
		}
		result.trade = decode_trade(data)
		result.bytes_consumed = TRADE_WIRE_SIZE
		
	case MSG_TOP_OF_BOOK:
		if len(data) < TOP_OF_BOOK_WIRE_SIZE {
			return result, .Incomplete_Read
		}
		tob, err := decode_top_of_book(data)
		if err != .None {
			return result, err
		}
		result.top_of_book = tob
		result.bytes_consumed = TOP_OF_BOOK_WIRE_SIZE
		
	case MSG_REJECT:
		if len(data) < REJECT_WIRE_SIZE {
			return result, .Incomplete_Read
		}
		reject, err := decode_reject(data)
		if err != .None {
			return result, err
		}
		result.reject = reject
		result.bytes_consumed = REJECT_WIRE_SIZE
		
	case:
		return result, .Invalid_Message_Type
	}
	
	return result, .None
}

// Decode ack from wire
decode_ack :: proc(data: []u8) -> Ack {
	ack := Ack{}
	pos := HEADER_SIZE
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		ack.symbol[i] = data[pos + i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	ack.user_id = read_u32_big(data[pos:])
	pos += 4
	
	ack.user_order_id = read_u32_big(data[pos:])
	
	return ack
}

// Decode cancel ack from wire
decode_cancel_ack :: proc(data: []u8) -> Cancel_Ack {
	ack := Cancel_Ack{}
	pos := HEADER_SIZE
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		ack.symbol[i] = data[pos + i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	ack.user_id = read_u32_big(data[pos:])
	pos += 4
	
	ack.user_order_id = read_u32_big(data[pos:])
	
	return ack
}

// Decode trade from wire
decode_trade :: proc(data: []u8) -> Trade {
	trade := Trade{}
	pos := HEADER_SIZE
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		trade.symbol[i] = data[pos + i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	trade.buy_user_id = read_u32_big(data[pos:])
	pos += 4
	
	trade.buy_order_id = read_u32_big(data[pos:])
	pos += 4
	
	trade.sell_user_id = read_u32_big(data[pos:])
	pos += 4
	
	trade.sell_order_id = read_u32_big(data[pos:])
	pos += 4
	
	trade.price = read_u32_big(data[pos:])
	pos += 4
	
	trade.quantity = read_u32_big(data[pos:])
	
	return trade
}

// Decode top of book from wire
decode_top_of_book :: proc(data: []u8) -> (Top_Of_Book, types.Error) {
	tob := Top_Of_Book{}
	pos := HEADER_SIZE
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		tob.symbol[i] = data[pos + i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	// side - SECURITY: validate before use
	side, valid := parse_side(data[pos])
	if !valid {
		return tob, .Invalid_Message
	}
	tob.side = side
	pos += 1
	
	tob.price = read_u32_big(data[pos:])
	pos += 4
	
	tob.quantity = read_u32_big(data[pos:])
	
	return tob, .None
}

// Decode reject from wire
decode_reject :: proc(data: []u8) -> (Reject, types.Error) {
	reject := Reject{}
	pos := HEADER_SIZE
	
	// symbol
	for i := 0; i < MAX_SYMBOL_LENGTH; i += 1 {
		reject.symbol[i] = data[pos + i]
	}
	pos += MAX_SYMBOL_LENGTH
	
	reject.user_id = read_u32_big(data[pos:])
	pos += 4
	
	reject.user_order_id = read_u32_big(data[pos:])
	pos += 4
	
	// reason - SECURITY: validate before use
	reason, valid := parse_reject_reason(data[pos])
	if !valid {
		return reject, .Invalid_Message
	}
	reject.reason = reason
	
	return reject, .None
}

// =============================================================================
// Message Size Lookup
// =============================================================================

// Get expected wire size for a message type
get_message_size :: proc(msg_type: u8) -> int {
	switch msg_type {
	case MSG_NEW_ORDER:   return NEW_ORDER_WIRE_SIZE
	case MSG_CANCEL:      return CANCEL_WIRE_SIZE
	case MSG_FLUSH:       return FLUSH_WIRE_SIZE
	case MSG_ACK:         return ACK_WIRE_SIZE
	case MSG_CANCEL_ACK:  return CANCEL_ACK_WIRE_SIZE
	case MSG_TRADE:       return TRADE_WIRE_SIZE
	case MSG_TOP_OF_BOOK: return TOP_OF_BOOK_WIRE_SIZE
	case MSG_REJECT:      return REJECT_WIRE_SIZE
	case:                 return -1
	}
}
