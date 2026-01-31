package threading

// =============================================================================
// Output Router Thread
// =============================================================================
// - Dequeues from processor's output queue
// - Routes each message to appropriate client's output queue
// - Handles broadcast messages (client_id = 0)
// =============================================================================

Router :: struct {
	config:           Router_Config,
	client_registry:  ^net.Client_Registry,
	input_queues:     [MAX_OUTPUT_QUEUES]^Output_Queue,
	num_input_queues: u32,
	shutdown_flag:    ^bool,
	
	messages_routed:  u64,
	messages_dropped: u64,
}

router_thread :: proc(arg: rawptr) {
	// Round-robin across input queues
	// Route to per-client output queues
	// Handle broadcasts
}
