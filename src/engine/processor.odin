package engine

import "core:fmt"
import "core:time"
import "core:thread"
import "../types"
import "../protocol"
import "../core"
import "../lockfree"

// =============================================================================
// Processor Thread - Matching Engine Hot Path
// =============================================================================

PROCESSOR_SPIN_ITERATIONS :: 1000
PROCESSOR_SLEEP_NS :: 1000

Processor_Config :: struct {
	processor_id: u32,
	spin_wait:    bool,
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
	client_registry: ^Client_Registry,
	output_queue:    ^Output_Queue,
	shutdown_flag:   ^bool,
	
	running:         bool,
	output_sequence: u64,
	stats:           Processor_Stats,
	flush_client_id: u32,
}

processor_init :: proc(
	processor: ^Processor,
	config: Processor_Config,
	engine: ^core.Order_Book,
	client_registry: ^Client_Registry,
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

@(private)
enqueue_output :: proc(processor: ^Processor, msg: ^Output_Msg, client_id: u32) -> bool {
	seq := processor.output_sequence
	processor.output_sequence += 1
	
	envelope := Output_Envelope{
		msg       = msg^,
		client_id = client_id,
		sequence  = seq,
	}
	
	if lockfree.spsc_enqueue(processor.output_queue, &envelope) {
		processor.stats.output_messages += 1
		return true
	} else {
		processor.stats.output_queue_full += 1
		return false
	}
}

@(private)
send_trade :: proc(processor: ^Processor, trade: ^core.Trade_Result) {
	processor.stats.trades_processed += 1
	
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
	
	enqueue_output(processor, &trade_msg, trade.buy_user_id)
	if trade.buy_user_id != trade.sell_user_id {
		enqueue_output(processor, &trade_msg, trade.sell_user_id)
	}
}

@(private)
send_top_of_book :: proc(processor: ^Processor) {
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
		enqueue_output(processor, &tob, 0)
	}
	
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
		enqueue_output(processor, &tob, 0)
	}
}

@(private)
error_to_reject_reason :: proc(err: types.Error) -> protocol.Reject_Reason {
	#partial switch err {
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
// Message Processing
// =============================================================================

@(private)
process_new_order :: proc(processor: ^Processor, order: ^protocol.New_Order, client_id: u32) {
	_, err := core.book_add_order(
		processor.engine,
		order.user_id,
		order.user_order_id,
		order.price,
		order.quantity,
		order.side,
	)
	
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
	
	send_top_of_book(processor)
}

@(private)
process_cancel :: proc(processor: ^Processor, cancel: ^protocol.Cancel_Order, client_id: u32) {
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
	
	send_top_of_book(processor)
}

@(private)
process_message :: proc(processor: ^Processor, envelope: ^Input_Envelope) {
	client_id := envelope.client_id
	msg := &envelope.msg
	
	switch msg.msg_type {
	case .New_Order:
		process_new_order(processor, &msg.new_order, client_id)
	case .Cancel:
		process_cancel(processor, &msg.cancel, client_id)
	case .Flush:
		processor.flush_client_id = client_id
	}
	
	processor.stats.messages_processed += 1
}

// =============================================================================
// Main Loop
// =============================================================================

processor_thread_proc :: proc(t: ^thread.Thread) {
	processor := cast(^Processor)t.data
	
	fmt.printfln("[Processor %d] Starting", processor.config.processor_id)
	processor.running = true
	
	input_batch: [PROCESSOR_BATCH_SIZE]Input_Envelope
	spin_count := 0
	clients := registry_get_all_clients(processor.client_registry)
	
	for !processor.shutdown_flag^ {
		total_processed: u32 = 0
		
		for i := 0; i < len(clients); i += 1 {
			client := &clients[i]
			if client.state == .Inactive {
				continue
			}
			
			count := client_dequeue_input_batch(client, input_batch[:], PROCESSOR_BATCH_SIZE)
			if count == 0 {
				continue
			}
			
			for j: u32 = 0; j < count; j += 1 {
				process_message(processor, &input_batch[j])
			}
			
			total_processed += count
			processor.stats.batches_processed += 1
		}
		
		if total_processed == 0 {
			processor.stats.empty_polls += 1
			if processor.config.spin_wait {
				spin_count += 1
				if spin_count >= PROCESSOR_SPIN_ITERATIONS {
					thread.yield()
					spin_count = 0
				}
			} else {
				time.sleep(time.Duration(PROCESSOR_SLEEP_NS))
			}
		} else {
			spin_count = 0
		}
	}
	
	processor.running = false
	fmt.printfln("[Processor %d] Stopped", processor.config.processor_id)
	processor_print_stats(processor)
}

processor_print_stats :: proc(processor: ^Processor) {
	stats := &processor.stats
	fmt.println("\n=== Processor Statistics ===")
	fmt.printfln("Messages processed: %d", stats.messages_processed)
	fmt.printfln("Batches processed:  %d", stats.batches_processed)
	fmt.printfln("Trades processed:   %d", stats.trades_processed)
	fmt.printfln("Output messages:    %d", stats.output_messages)
	fmt.printfln("Empty polls:        %d", stats.empty_polls)
	fmt.printfln("Output queue full:  %d", stats.output_queue_full)
}
