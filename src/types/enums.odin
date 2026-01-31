package types

// =============================================================================
// Enumerations
// =============================================================================
// Internal enums for engine state (not wire format)
// Wire-format enums are in protocol/messages.odin
// =============================================================================

// Order type - internal use
Order_Type :: enum u8 {
	Limit             = 0,
	Market            = 1,
	Immediate_Or_Cancel = 2,  // IOC: fill what you can, cancel rest
	Fill_Or_Kill      = 3,    // FOK: fill entirely or cancel entirely
}

// Order status - internal tracking
Status :: enum u8 {
	New       = 0,
	Partial   = 1,
	Filled    = 2,
	Cancelled = 3,
	Rejected  = 4,
}

// Protocol message types - internal enum
// (Wire format uses ASCII bytes directly: 'N', 'C', 'A', etc.)
Message_Type :: enum u8 {
	// Client -> Server
	New_Order       = 'N',
	Cancel_Order    = 'C',
	Flush           = 'F',

	// Server -> Client
	Ack             = 'A',
	Cancel_Ack      = 'X',
	Trade           = 'T',
	Top_Of_Book     = 'B',
	Reject          = 'R',
}

// =============================================================================
// Utility functions
// =============================================================================

is_active :: #force_inline proc(status: Status) -> bool {
	return status == .New || status == .Partial
}

is_terminal :: #force_inline proc(status: Status) -> bool {
	return status == .Filled || status == .Cancelled || status == .Rejected
}
