package engine

import "../protocol"
import "../lockfree"

// =============================================================================
// Queue Configuration
// =============================================================================

QUEUE_CAPACITY :: 65536
CLIENT_OUTPUT_QUEUE_CAPACITY :: 524288
PROCESSOR_BATCH_SIZE :: 64
ROUTER_BATCH_SIZE :: 64

// =============================================================================
// Input Messages (Client → Processor)
// =============================================================================

Input_Msg_Type :: enum u8 {
	New_Order,
	Cancel,
	Flush,
}

Input_Msg :: struct {
	msg_type:  Input_Msg_Type,
	new_order: protocol.New_Order,
	cancel:    protocol.Cancel_Order,
}

Input_Envelope :: struct {
	msg:       Input_Msg,
	client_id: u32,
	timestamp: u64,
}

// =============================================================================
// Output Messages (Processor → Client)
// =============================================================================

Output_Msg_Type :: enum u8 {
	Ack,
	Cancel_Ack,
	Trade,
	Top_Of_Book,
	Reject,
}

Output_Msg :: struct {
	msg_type:    Output_Msg_Type,
	ack:         protocol.Ack,
	cancel_ack:  protocol.Cancel_Ack,
	trade:       protocol.Trade,
	top_of_book: protocol.Top_Of_Book,
	reject:      protocol.Reject,
}

Output_Envelope :: struct {
	msg:       Output_Msg,
	client_id: u32,
	sequence:  u64,
}

// =============================================================================
// Queue Types
// =============================================================================

Input_Queue :: lockfree.SPSC_Queue(Input_Envelope, QUEUE_CAPACITY)
Output_Queue :: lockfree.SPSC_Queue(Output_Envelope, QUEUE_CAPACITY)
Client_Output_Queue :: lockfree.SPSC_Queue(Output_Msg, CLIENT_OUTPUT_QUEUE_CAPACITY)
