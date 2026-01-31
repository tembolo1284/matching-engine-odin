package threading

import "../protocol"
import "../sync"

// =============================================================================
// Queue Type Definitions
// =============================================================================
// Defines the message envelope types that flow through the system:
//
// Input Path:  Client Handler → Input Queue → Processor
// Output Path: Processor → Output Queue → Router → Per-Client Queue → Client Handler
//
// Envelope wraps message with routing metadata (client_id, sequence)
// =============================================================================

// =============================================================================
// Queue Capacities (must be power of 2)
// =============================================================================

// Main input/output queue capacity
QUEUE_CAPACITY :: 65536

// Per-client output queue capacity (larger to handle bursts)
CLIENT_OUTPUT_QUEUE_CAPACITY :: 524288

// Batch sizes for dequeue operations
PROCESSOR_BATCH_SIZE :: 64
ROUTER_BATCH_SIZE :: 64

// =============================================================================
// Input Envelope (Client Handler → Processor)
// =============================================================================

// Input message types
Input_Msg_Type :: enum u8 {
	New_Order = 0,
	Cancel    = 1,
	Flush     = 2,
}

// Input message union
Input_Msg :: struct {
	msg_type:  Input_Msg_Type,
	new_order: protocol.New_Order,    // Valid if msg_type == .New_Order
	cancel:    protocol.Cancel_Order, // Valid if msg_type == .Cancel
}

// Input envelope - wraps message with client routing info
Input_Envelope :: struct {
	msg:       Input_Msg,
	client_id: u32,        // Which client sent this
	timestamp: u64,        // Receipt timestamp (for latency tracking)
}

// =============================================================================
// Output Envelope (Processor → Router)
// =============================================================================

// Output message types
Output_Msg_Type :: enum u8 {
	Ack         = 0,
	Cancel_Ack  = 1,
	Trade       = 2,
	Top_Of_Book = 3,
	Reject      = 4,
}

// Output message union
Output_Msg :: struct {
	msg_type:    Output_Msg_Type,
	ack:         protocol.Ack,          // Valid if msg_type == .Ack
	cancel_ack:  protocol.Cancel_Ack,   // Valid if msg_type == .Cancel_Ack
	trade:       protocol.Trade,        // Valid if msg_type == .Trade
	top_of_book: protocol.Top_Of_Book,  // Valid if msg_type == .Top_Of_Book
	reject:      protocol.Reject,       // Valid if msg_type == .Reject
}

// Output envelope - wraps message with routing info
Output_Envelope :: struct {
	msg:       Output_Msg,
	client_id: u32,        // Target client (0 = broadcast)
	sequence:  u64,        // Sequence number for ordering
}

// =============================================================================
// Queue Type Aliases
// =============================================================================

// Per-client input queue (client handler produces, processor consumes)
Input_Queue :: sync.SPSC_Queue(Input_Envelope, QUEUE_CAPACITY)

// Processor output queue (processor produces, router consumes)
Output_Queue :: sync.SPSC_Queue(Output_Envelope, QUEUE_CAPACITY)

// Per-client output queue (router produces, client handler consumes)
Client_Output_Queue :: sync.SPSC_Queue(Output_Msg, CLIENT_OUTPUT_QUEUE_CAPACITY)

// =============================================================================
// Envelope Constructors
// =============================================================================

// Create input envelope for new order
make_input_new_order :: proc(order: ^protocol.New_Order, client_id: u32) -> Input_Envelope {
	return Input_Envelope{
		msg = Input_Msg{
			msg_type  = .New_Order,
			new_order = order^,
		},
		client_id = client_id,
		timestamp = 0,  // TODO: fill with actual timestamp
	}
}

// Create input envelope for cancel
make_input_cancel :: proc(cancel: ^protocol.Cancel_Order, client_id: u32) -> Input_Envelope {
	return Input_Envelope{
		msg = Input_Msg{
			msg_type = .Cancel,
			cancel   = cancel^,
		},
		client_id = client_id,
		timestamp = 0,
	}
}

// Create input envelope for flush
make_input_flush :: proc(client_id: u32) -> Input_Envelope {
	return Input_Envelope{
		msg = Input_Msg{
			msg_type = .Flush,
		},
		client_id = client_id,
		timestamp = 0,
	}
}

// Create output envelope for ack
make_output_ack :: proc(ack: ^protocol.Ack, client_id: u32, seq: u64) -> Output_Envelope {
	return Output_Envelope{
		msg = Output_Msg{
			msg_type = .Ack,
			ack      = ack^,
		},
		client_id = client_id,
		sequence  = seq,
	}
}

// Create output envelope for cancel ack
make_output_cancel_ack :: proc(ack: ^protocol.Cancel_Ack, client_id: u32, seq: u64) -> Output_Envelope {
	return Output_Envelope{
		msg = Output_Msg{
			msg_type   = .Cancel_Ack,
			cancel_ack = ack^,
		},
		client_id = client_id,
		sequence  = seq,
	}
}

// Create output envelope for trade
make_output_trade :: proc(trade: ^protocol.Trade, client_id: u32, seq: u64) -> Output_Envelope {
	return Output_Envelope{
		msg = Output_Msg{
			msg_type = .Trade,
			trade    = trade^,
		},
		client_id = client_id,
		sequence  = seq,
	}
}

// Create output envelope for top of book
make_output_top_of_book :: proc(tob: ^protocol.Top_Of_Book, client_id: u32, seq: u64) -> Output_Envelope {
	return Output_Envelope{
		msg = Output_Msg{
			msg_type    = .Top_Of_Book,
			top_of_book = tob^,
		},
		client_id = client_id,
		sequence  = seq,
	}
}

// Create output envelope for reject
make_output_reject :: proc(reject: ^protocol.Reject, client_id: u32, seq: u64) -> Output_Envelope {
	return Output_Envelope{
		msg = Output_Msg{
			msg_type = .Reject,
			reject   = reject^,
		},
		client_id = client_id,
		sequence  = seq,
	}
}
