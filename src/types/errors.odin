package types

// =============================================================================
// Error Handling
// =============================================================================
// Rule 7: Return values must be checked
// No exceptions - explicit error returns everywhere
// =============================================================================

// Engine-level errors
Error :: enum u8 {
	None = 0,

	// Memory errors
	Pool_Exhausted,
	Pool_Invalid_Handle,
	Pool_Double_Free,

	// Order errors
	Order_Not_Found,
	Order_Already_Exists,
	Order_Invalid_State,
	Order_Invalid_Price,
	Order_Invalid_Quantity,

	// Orderbook errors
	Book_Full,
	Book_Invalid_Symbol,
	Price_Level_Full,

	// Network errors
	Connection_Closed,
	Connection_Timeout,
	Invalid_Message,
	Message_Too_Large,
	Incomplete_Read,
	Incomplete_Write,

	// Protocol errors
	Invalid_Magic,
	Invalid_Version,
	Invalid_Message_Type,
	Invalid_Sequence,
	Checksum_Mismatch,

	// System errors
	Would_Block,
	Interrupted,
	Internal,
}

// Check if error indicates success
is_ok :: #force_inline proc(e: Error) -> bool {
	return e == .None
}

// Check if error indicates failure
is_err :: #force_inline proc(e: Error) -> bool {
	return e != .None
}

// Convert error to string for logging (not on hot path)
error_string :: proc(e: Error) -> string {
	switch e {
	case .None:                 return "success"
	case .Pool_Exhausted:       return "pool exhausted"
	case .Pool_Invalid_Handle:  return "invalid pool handle"
	case .Pool_Double_Free:     return "double free detected"
	case .Order_Not_Found:      return "order not found"
	case .Order_Already_Exists: return "order already exists"
	case .Order_Invalid_State:  return "invalid order state"
	case .Order_Invalid_Price:  return "invalid price"
	case .Order_Invalid_Quantity: return "invalid quantity"
	case .Book_Full:            return "orderbook full"
	case .Book_Invalid_Symbol:  return "invalid symbol"
	case .Price_Level_Full:     return "price level full"
	case .Connection_Closed:    return "connection closed"
	case .Connection_Timeout:   return "connection timeout"
	case .Invalid_Message:      return "invalid message"
	case .Message_Too_Large:    return "message too large"
	case .Incomplete_Read:      return "incomplete read"
	case .Incomplete_Write:     return "incomplete write"
	case .Invalid_Magic:        return "invalid magic bytes"
	case .Invalid_Version:      return "invalid protocol version"
	case .Invalid_Message_Type: return "invalid message type"
	case .Invalid_Sequence:     return "invalid sequence number"
	case .Checksum_Mismatch:    return "checksum mismatch"
	case .Would_Block:          return "would block"
	case .Interrupted:          return "interrupted"
	case .Internal:             return "internal error"
	}
	return "unknown error"
}
