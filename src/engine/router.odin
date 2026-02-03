package engine

import "core:fmt"
import "core:time"
import "core:thread"
import "../lockfree"

// =============================================================================
// Router Thread - Output Fan-out
// =============================================================================

ROUTER_SLEEP_NS :: 1000
MAX_OUTPUT_QUEUES :: 2

Router_Config :: struct {
	tcp_mode: bool,
}

Router :: struct {
	config:           Router_Config,
	client_registry:  ^Client_Registry,
	input_queues:     [MAX_OUTPUT_QUEUES]^Output_Queue,
	num_input_queues: u32,
	shutdown_flag:    ^bool,
	
	messages_routed:  u64,
	messages_dropped: u64,
}

router_init :: proc(
	router: ^Router,
	config: Router_Config,
	client_registry: ^Client_Registry,
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
}

// =============================================================================
// Message Routing
// =============================================================================

@(private)
msg_type_name :: proc(msg_type: Output_Msg_Type) -> string {
	switch msg_type {
	case .Ack:         return "ACK"
	case .Cancel_Ack:  return "CANCEL_ACK"
	case .Trade:       return "TRADE"
	case .Top_Of_Book: return "TOB"
	case .Reject:      return "REJECT"
	}
	return "UNKNOWN"
}

@(private)
route_to_client :: proc(router: ^Router, msg: ^Output_Msg, client_id: u32) -> bool {
	client := registry_get_client(router.client_registry, client_id)
	if client == nil {
		fmt.printfln("[Router] Client %d not found, dropping %s", client_id, msg_type_name(msg.msg_type))
		return false
	}
	
	if client_enqueue_output(client, msg) {
		fmt.printfln("[Router] Routed %s to client %d", msg_type_name(msg.msg_type), client_id)
		return true
	} else {
		fmt.printfln("[Router] Queue full for client %d, dropping %s", client_id, msg_type_name(msg.msg_type))
		return false
	}
}

@(private)
broadcast_message :: proc(router: ^Router, msg: ^Output_Msg) -> bool {
	clients := registry_get_all_clients(router.client_registry)
	success := false
	
	for i := 0; i < len(clients); i += 1 {
		client := &clients[i]
		if client.state == .Inactive {
			continue
		}
		if client_enqueue_output(client, msg) {
			fmt.printfln("[Router] Broadcast %s to client %d", msg_type_name(msg.msg_type), client.client_id)
			success = true
		}
	}
	return success
}

@(private)
route_message :: proc(router: ^Router, envelope: ^Output_Envelope) -> bool {
	if envelope.client_id == 0 {
		return broadcast_message(router, &envelope.msg)
	} else {
		return route_to_client(router, &envelope.msg, envelope.client_id)
	}
}

@(private)
process_queue_batch :: proc(router: ^Router, queue: ^Output_Queue, batch: []Output_Envelope) -> u32 {
	count := lockfree.spsc_dequeue_batch(queue, batch, ROUTER_BATCH_SIZE)
	
	for i: u32 = 0; i < count; i += 1 {
		if route_message(router, &batch[i]) {
			router.messages_routed += 1
		} else {
			router.messages_dropped += 1
		}
	}
	
	return count
}

// =============================================================================
// Main Loop
// =============================================================================

router_thread_proc :: proc(t: ^thread.Thread) {
	router := cast(^Router)t.data
	
	fmt.println("[Router] Starting")
	
	batch: [ROUTER_BATCH_SIZE]Output_Envelope
	
	for !router.shutdown_flag^ {
		total_processed: u32 = 0
		
		for q: u32 = 0; q < router.num_input_queues; q += 1 {
			queue := router.input_queues[q]
			if queue == nil {
				continue
			}
			total_processed += process_queue_batch(router, queue, batch[:])
		}
		
		if total_processed == 0 {
			time.sleep(time.Duration(ROUTER_SLEEP_NS))
		}
	}
	
	// Drain remaining
	fmt.println("[Router] Draining...")
	for iteration := 0; iteration < 100; iteration += 1 {
		has_messages := false
		for q: u32 = 0; q < router.num_input_queues; q += 1 {
			queue := router.input_queues[q]
			if queue == nil {
				continue
			}
			count := process_queue_batch(router, queue, batch[:])
			if count > 0 {
				has_messages = true
			}
		}
		if !has_messages {
			break
		}
	}
	
	fmt.println("[Router] Stopped")
	router_print_stats(router)
}

router_print_stats :: proc(router: ^Router) {
	fmt.println("\n=== Router Statistics ===")
	fmt.printfln("Messages routed:  %d", router.messages_routed)
	fmt.printfln("Messages dropped: %d", router.messages_dropped)
}
