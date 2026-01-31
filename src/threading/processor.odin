package threading

import "core:fmt"
import "core:time"
import "core:intrinsics"
import "../types"
import "../protocol"
import "../core"
import "../net"
import spsc "../sync"

// =============================================================================
// Processor Thread
// =============================================================================
// Dedicated thread for order matching - the HOT PATH.
//
// Architecture:
// - Round-robins across per-client input queues
// - Batch dequeues for efficiency
// - Processes through matching engine
// - Enqueues results to output queue for router
//
// No I/O syscalls on this thread - pure computation.
//
// Power of Ten Compliance:
// - Rule 2: All loops bounded
// - Rule 3: No dynamic allocation in main loop
// - Rule 5: Assertions on critical paths
// =============================================================================

// Configuration
PROCESSOR_SPIN_ITERATIONS :: 1000
PROCESSOR_SLEEP_NS :: 1000  // 1 microsecond

// =============================================================================
// Processor State
// =============================================================================

Processor_Config :: struct {
	processor_id: u32,
	spin_wait:    bool,    // true = spin, false = sleep when idle
}

Processor_Stats :: struct {
	messages_processed: u64,
	batches_processed:  u64,
	output_messages:    u64,
	trades_processed:   u64,
	empty_polls:        u64,
	output_queue_full:  u64,
}

Processor :: struct {
	config:          Processor_Config,
	engine:          ^core.Order_Book,
	client_registry: ^net.Client_Registry,
	output_queue:    ^Output_Queue,
	shutdown_flag:   ^bool,
	
	// State
	running:         bool,
	output_sequence: u64,
	
	// Statistics
	stats:           Processor_Stats,
	
	// Flush state (for continuing multi-batch flushes)
	flush_client_id: u32,
}

// =============================================================================
// Initialization
// =============================================================================

processor_init :: proc(
	processor: ^Processor,
	config: Processor_Config,
	engine: ^core.Order_Book,
	client_registry: ^net.Client_Registry,
	output_queue: ^Output_Queue,
	shutdown_flag: ^bool,
) {
	processor.config = config
	processor.engine = engine
	processor.client_registry = client_registry
	processor.output_queue = output_queue
	processor.shutdown_flag = shutdown_flag
	
	processor.running = false
	processor.output_sequence = 0
	processor.flush_client_id = 0
	
	processor.stats = Processor_Stats{}
}

// =============================================================================
// Output Helpers
// =============================================================================

// Enqueue output with error tracking
@(private)
enqueue_output :: proc(
	processor: ^Processor,
	msg: ^Output_Msg,
	client_id: u32,
) -> bool {
	seq := processor.output_sequence
	processor.output_sequence += 1
	
	envelope := Output_Envelope{
		msg       = msg^,
		client_id = client_id,
		sequence  = seq,
	}
	
	if spsc.spsc_enqueue(processor.output_queue, &envelope) {
		processor.stats.output_messages += 1
		return true
	} else {
		processor.stats.output_queue_full += 1
		return false
	}
}

// Drain output buffer from matching engine to output queue
@(private)
drain_outputs :: proc(
	processor: ^Processor,
	trade: ^core.Trade_Result,
	client_id: u32,
) {
	// Trade: route to BOTH buyer and seller
	processor.stats.trades_processed += 1
	
	// Build trade message
	trade_msg := Output_Msg{
		msg_type = .Trade,
		trade = protocol.Trade{
			symbol        = processor.engine.symbol,
			buy_user_id   = trade.buy_user_id,
			buy_order_id  = trade.buy_order_id,
			sell_user_id  = trade.sell_user_id,
			sell_order_id = trade.sell_order_id,
			price         = trade.price,
			quantity      = trade.quantity,
		},
	}
	
	// Send to buyer
	enqueue_output(processor, &trade_msg, trade.buy_user_id)
	
	// Send to seller (if different client)
	if trade.buy_user_id != trade.sell_user_id {
		enqueue_output(processor, &trade_msg, trade.sell_user_id)
	}
}

// =============================================================================
// Message Processing
// =============================================================================

// Process a single input message
@(private)
process_message :: proc(
	processor: ^Processor,
	envelope: ^Input_Envelope,
) {
	client_id := envelope.client_id
	msg := &envelope.msg
	
	switch msg.msg_type {
	case .New_Order:
		process_new_order(processor, &msg.new_order, client_id)
		
	case .Cancel:
		process_cancel(processor, &msg.cancel, client_id)
		
	case .Flush:
		processor.flush_client_id = client_id
		// Flush would cancel all orders - simplified for now
	}
	
	processor.stats.messages_processed += 1
}

// Process new order
@(private)
process_new_order :: proc(
	processor: ^Processor,
	order: ^protocol.New_Order,
	client_id: u32,
) {
	// Set up trade callback context
	Trade_Context :: struct {
		processor: ^Processor,
		client_id: u32,
	}
	ctx := Trade_Context{processor, client_id}
	
	// Temporarily set trade callback
	old_callback := processor.engine.on_trade
	old_data := processor.engine.trade_user_data
	
	core.book_set_trade_callback(processor.engine, proc(trade: ^core.Trade_Result, user_data: rawptr) {
		tc := cast(^Trade_Context)user_data
		drain_outputs(tc.processor, trade, tc.client_id)
	}, &ctx)
	
	// Submit order
	_, err := core.book_add_order(
		processor.engine,
		order.user_id,
		order.user_order_id,
		order.price,
		order.quantity,
		order.side,
	)
	
	// Restore callback
	processor.engine.on_trade = old_callback
	processor.engine.trade_user_data = old_data
	
	// Send ack or reject
	if err == .None {
		ack := Output_Msg{
			msg_type = .Ack,
			ack = protocol.Ack{
				symbol        = processor.engine.symbol,
				user_id       = order.user_id,
				user_order_id = order.user_order_id,
			},
		}
		enqueue_output(processor, &ack, client_id)
	} else {
		reject := Output_Msg{
			msg_type = .Reject,
			reject = protocol.Reject{
				symbol        = processor.engine.symbol,
				user_id       = order.user_id,
				user_order_id = order.user_order_id,
				reason        = error_to_reject_reason(err),
			},
		}
		enqueue_output(processor, &reject, client_id)
	}
	
	// Send top of book updates
	send_top_of_book(processor, client_id)
}

// Process cancel
@(private)
process_cancel :: proc(
	processor: ^Processor,
	cancel: ^protocol.Cancel_Order,
	client_id: u32,
) {
	err := core.book_cancel_order(
		processor.engine,
		cancel.user_id,
		cancel.user_order_id,
	)
	
	if err == .None {
		ack := Output_Msg{
			msg_type = .Cancel_Ack,
			cancel_ack = protocol.Cancel_Ack{
				symbol        = processor.engine.symbol,
				user_id       = cancel.user_id,
				user_order_id = cancel.user_order_id,
			},
		}
		enqueue_output(processor, &ack, client_id)
	} else {
		reject := Output_Msg{
			msg_type = .Reject,
			reject = protocol.Reject{
				symbol        = processor.engine.symbol,
				user_id       = cancel.user_id,
				user_order_id = cancel.user_order_id,
				reason        = error_to_reject_reason(err),
			},
		}
		enqueue_output(processor, &reject, client_id)
	}
	
	send_top_of_book(processor, client_id)
}

// Send top of book updates
@(private)
send_top_of_book :: proc(processor: ^Processor, client_id: u32) {
	// Bid side
	bid_price := core.book_best_bid(processor.engine)
	bid_qty := core.book_best_bid_qty(processor.engine)
	
	if bid_price > 0 {
		tob := Output_Msg{
			msg_type = .Top_Of_Book,
			top_of_book = protocol.Top_Of_Book{
				symbol   = processor.engine.symbol,
				side     = .Buy,
				price    = bid_price,
				quantity = bid_qty,
			},
		}
		enqueue_output(processor, &tob, 0)  // 0 = broadcast
	}
	
	// Ask side
	ask_price := core.book_best_ask(processor.engine)
	ask_qty := core.book_best_ask_qty(processor.engine)
	
	if ask_price > 0 {
		tob := Output_Msg{
			msg_type = .Top_Of_Book,
			top_of_book = protocol.Top_Of_Book{
				symbol   = processor.engine.symbol,
				side     = .Sell,
				price    = ask_price,
				quantity = ask_qty,
			},
		}
		enqueue_output(processor, &tob, 0)  // 0 = broadcast
	}
}

// Map error to reject reason
@(private)
error_to_reject_reason :: proc(err: types.Error) -> protocol.Reject_Reason {
	switch err {
	case .Order_Not_Found:        return .Order_Not_Found
	case .Order_Already_Exists:   return .Duplicate_Order_Id
	case .Order_Invalid_Price:    return .Invalid_Price
	case .Order_Invalid_Quantity: return .Invalid_Quantity
	case .Pool_Exhausted:         return .Pool_Exhausted
	case .Book_Full:              return .Book_Full
	case .Book_Invalid_Symbol:    return .Unknown_Symbol
	case:                         return .Invalid_Order_Id
	}
}

// =============================================================================
// Main Processing Loop
// =============================================================================

// Processor thread entry point
processor_thread :: proc(arg: rawptr) {
	processor := cast(^Processor)arg
	
	fmt.printfln("[Processor %d] Starting (mode: %s)",
		processor.config.processor_id,
		processor.config.spin_wait ? "spin-wait" : "sleep")
	
	processor.running = true
	
	// Pre-allocated batch buffer (Rule 3: no allocation in loop)
	input_batch: [PROCESSOR_BATCH_SIZE]Input_Envelope
	
	// Spin counter for hybrid wait
	spin_count := 0
	
	// Stats flush interval
	STATS_FLUSH_INTERVAL :: 1000
	since_last_flush: u64 = 0
	
	// Get all clients array for round-robin
	clients := net.registry_get_all_clients(processor.client_registry)
	
	// Main loop
	for !processor.shutdown_flag^ {
		total_processed: u32 = 0
		
		// Round-robin across all client input queues
		for i := 0; i < len(clients); i += 1 {
			client := &clients[i]
			
			// Skip inactive clients
			if client.state == .Inactive {
				continue
			}
			
			// Batch dequeue from this client's input queue
			count := net.client_dequeue_input_batch(
				client,
				input_batch[:],
				PROCESSOR_BATCH_SIZE,
			)
			
			if count == 0 {
				continue
			}
			
			// Process each message in batch
			for j: u32 = 0; j < count; j += 1 {
				process_message(processor, &input_batch[j])
			}
			
			total_processed += count
			processor.stats.batches_processed += 1
		}
		
		// Handle empty poll
		if total_processed == 0 {
			processor.stats.empty_polls += 1
			
			if processor.config.spin_wait {
				spin_count += 1
				if spin_count >= PROCESSOR_SPIN_ITERATIONS {
					// Yield after spinning
					intrinsics.cpu_relax()
					spin_count = 0
				}
				// CPU pause hint
				intrinsics.cpu_relax()
			} else {
				// Sleep
				time.sleep(time.Duration(PROCESSOR_SLEEP_NS))
			}
		} else {
			spin_count = 0
		}
		
		// Periodic stats logging
		since_last_flush += u64(total_processed)
		if since_last_flush >= STATS_FLUSH_INTERVAL {
			since_last_flush = 0
			// Stats are updated continuously, no flush needed
		}
	}
	
	processor.running = false
	
	fmt.printfln("[Processor %d] Shutting down", processor.config.processor_id)
	processor_print_stats(processor)
}

// Print processor statistics
processor_print_stats :: proc(processor: ^Processor) {
	stats := &processor.stats
	
	avg_batch: f64 = 0
	if stats.batches_processed > 0 {
		avg_batch = f64(stats.messages_processed) / f64(stats.batches_processed)
	}
	
	outputs_per_msg: f64 = 0
	if stats.messages_processed > 0 {
		outputs_per_msg = f64(stats.output_messages) / f64(stats.messages_processed)
	}
	
	fmt.println("\n=== Processor Statistics ===")
	fmt.printfln("Messages processed:    %d", stats.messages_processed)
	fmt.printfln("Batches processed:     %d", stats.batches_processed)
	fmt.printfln("Average batch size:    %.1f", avg_batch)
	fmt.printfln("Output messages:       %d", stats.output_messages)
	fmt.printfln("Outputs per message:   %.2f", outputs_per_msg)
	fmt.printfln("Trades processed:      %d", stats.trades_processed)
	fmt.printfln("Empty polls:           %d", stats.empty_polls)
	fmt.printfln("Output queue full:     %d", stats.output_queue_full)
}
