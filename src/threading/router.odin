package threading

import "core:fmt"
import "core:time"
import "../net"
import spsc "../sync"

// =============================================================================
// Output Router Thread
// =============================================================================
// Dedicated thread for routing output messages to clients.
//
// Architecture:
// - Dequeues from processor's output queue
// - Routes each message to appropriate client's output queue
// - Handles broadcast messages (client_id = 0)
// - Round-robin for fairness when multiple clients
//
// Power of Ten Compliance:
// - Rule 2: All loops bounded
// - Rule 3: No dynamic allocation in main loop
// =============================================================================

// Configuration
ROUTER_SLEEP_NS :: 1000  // 1 microsecond

// Maximum output queues (for dual-processor support)
MAX_OUTPUT_QUEUES :: 2

// =============================================================================
// Router State
// =============================================================================

Router_Config :: struct {
	tcp_mode: bool,   // true = route to TCP clients
}

Router :: struct {
	config:           Router_Config,
	client_registry:  ^net.Client_Registry,
	
	// Input queues from processors (1 or 2)
	input_queues:     [MAX_OUTPUT_QUEUES]^Output_Queue,
	num_input_queues: u32,
	
	shutdown_flag:    ^bool,
	
	// Statistics
	messages_routed:  u64,
	messages_dropped: u64,
	messages_from_processor: [MAX_OUTPUT_QUEUES]u64,
}

// =============================================================================
// Initialization
// =============================================================================

router_init :: proc(
	router: ^Router,
	config: Router_Config,
	client_registry: ^net.Client_Registry,
	output_queue: ^Output_Queue,
	shutdown_flag: ^bool,
) {
	router.config = config
	router.client_registry = client_registry
	router.input_queues[0] = output_queue
	router.input_queues[1] = nil
	router.num_input_queues = 1
	router.shutdown_flag = shutdown_flag
	
	router.messages_routed = 0
	router.messages_dropped = 0
	router.messages_from_processor[0] = 0
	router.messages_from_processor[1] = 0
}

// Initialize with dual processors
router_init_dual :: proc(
	router: ^Router,
	config: Router_Config,
	client_registry: ^net.Client_Registry,
	output_queue_0: ^Output_Queue,
	output_queue_1: ^Output_Queue,
	shutdown_flag: ^bool,
) {
	router.config = config
	router.client_registry = client_registry
	router.input_queues[0] = output_queue_0
	router.input_queues[1] = output_queue_1
	router.num_input_queues = 2
	router.shutdown_flag = shutdown_flag
	
	router.messages_routed = 0
	router.messages_dropped = 0
	router.messages_from_processor[0] = 0
	router.messages_from_processor[1] = 0
}

// =============================================================================
// Message Routing
// =============================================================================

// Route a single message to appropriate client(s)
@(private)
route_message :: proc(router: ^Router, envelope: ^Output_Envelope) -> bool {
	client_id := envelope.client_id
	msg := &envelope.msg
	
	if client_id == 0 {
		// Broadcast to all clients
		return broadcast_message(router, msg)
	} else {
		// Route to specific client
		return route_to_client(router, msg, client_id)
	}
}

// Route to a specific client
@(private)
route_to_client :: proc(router: ^Router, msg: ^Output_Msg, client_id: u32) -> bool {
	client := net.registry_get_client(router.client_registry, client_id)
	if client == nil {
		return false  // Client disconnected
	}
	
	if net.client_enqueue_output(client, msg) {
		return true
	}
	
	// Queue full - message dropped
	return false
}

// Broadcast to all connected clients
@(private)
broadcast_message :: proc(router: ^Router, msg: ^Output_Msg) -> bool {
	clients := net.registry_get_all_clients(router.client_registry)
	success := false
	
	for i := 0; i < len(clients); i += 1 {
		client := &clients[i]
		if client.state == .Inactive {
			continue
		}
		
		if net.client_enqueue_output(client, msg) {
			success = true
		}
	}
	
	return success
}

// Process a batch from a single queue
@(private)
process_queue_batch :: proc(
	router: ^Router,
	queue: ^Output_Queue,
	queue_index: u32,
	batch: []Output_Envelope,
) -> u32 {
	// Batch dequeue
	count := spsc.spsc_dequeue_batch(queue, batch, ROUTER_BATCH_SIZE)
	
	// Route each message
	for i: u32 = 0; i < count; i += 1 {
		envelope := &batch[i]
		
		if route_message(router, envelope) {
			router.messages_routed += 1
		} else {
			router.messages_dropped += 1
		}
		
		router.messages_from_processor[queue_index] += 1
	}
	
	return count
}

// =============================================================================
// Main Router Loop
// =============================================================================

// Router thread entry point
router_thread :: proc(arg: rawptr) {
	router := cast(^Router)arg
	
	fmt.printfln("[Router] Starting (mode: %s, queues: %d)",
		router.config.tcp_mode ? "TCP" : "UDP",
		router.num_input_queues)
	
	// Pre-allocated batch buffer (Rule 3)
	batch: [ROUTER_BATCH_SIZE]Output_Envelope
	
	// Main loop
	for !router.shutdown_flag^ {
		total_processed: u32 = 0
		
		// Round-robin across all input queues
		for q: u32 = 0; q < router.num_input_queues && q < MAX_OUTPUT_QUEUES; q += 1 {
			queue := router.input_queues[q]
			if queue == nil {
				continue
			}
			
			processed := process_queue_batch(router, queue, q, batch[:])
			total_processed += processed
		}
		
		// Sleep if no messages
		if total_processed == 0 {
			time.sleep(time.Duration(ROUTER_SLEEP_NS))
		}
	}
	
	// Drain remaining messages
	fmt.println("[Router] Draining remaining messages...")
	drain_remaining(router)
	
	fmt.println("[Router] Shutting down")
	router_print_stats(router)
}

// Drain remaining messages during shutdown
@(private)
drain_remaining :: proc(router: ^Router) {
	batch: [ROUTER_BATCH_SIZE]Output_Envelope
	
	MAX_DRAIN_ITERATIONS :: 100
	
	for iteration := 0; iteration < MAX_DRAIN_ITERATIONS; iteration += 1 {
		has_messages := false
		
		for q: u32 = 0; q < router.num_input_queues && q < MAX_OUTPUT_QUEUES; q += 1 {
			queue := router.input_queues[q]
			if queue == nil {
				continue
			}
			
			count := spsc.spsc_dequeue_batch(queue, batch[:], ROUTER_BATCH_SIZE)
			if count > 0 {
				has_messages = true
				
				for i: u32 = 0; i < count; i += 1 {
					if route_message(router, &batch[i]) {
						router.messages_routed += 1
					} else {
						router.messages_dropped += 1
					}
				}
			}
		}
		
		if !has_messages {
			break
		}
	}
}

// Print router statistics
router_print_stats :: proc(router: ^Router) {
	fmt.println("\n=== Router Statistics ===")
	fmt.printfln("Messages routed:       %d", router.messages_routed)
	fmt.printfln("Messages dropped:      %d", router.messages_dropped)
	
	if router.num_input_queues == 2 {
		fmt.printfln("From Processor 0:      %d", router.messages_from_processor[0])
		fmt.printfln("From Processor 1:      %d", router.messages_from_processor[1])
	}
}
