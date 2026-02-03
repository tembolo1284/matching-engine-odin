package engine

import "core:fmt"
import "core:net"
import "core:thread"
import "core:time"
import "../types"
import "../protocol"

// =============================================================================
// TCP Listener
// =============================================================================

Listener_Config :: struct {
	port:       u16,
	quiet_mode: bool,
}

Listener :: struct {
	config:               Listener_Config,
	listen_socket:        net.TCP_Socket,
	client_registry:      ^Client_Registry,
	shutdown_flag:        ^bool,
	connections_accepted: u64,
	connections_rejected: u64,
}

listener_init :: proc(
	listener: ^Listener,
	config: Listener_Config,
	client_registry: ^Client_Registry,
	shutdown_flag: ^bool,
) -> types.Error {
	listener.config = config
	listener.client_registry = client_registry
	listener.shutdown_flag = shutdown_flag
	listener.connections_accepted = 0
	listener.connections_rejected = 0
	
	endpoint := net.Endpoint{
		address = net.IP4_Any,
		port = int(config.port),
	}
	
	socket, err := net.listen_tcp(endpoint)
	if err != nil {
		fmt.eprintfln("[Listener] Failed to listen on port %d: %v", config.port, err)
		return .Internal
	}
	
	listener.listen_socket = socket
	return .None
}

// =============================================================================
// Client Handler Context
// =============================================================================

Client_Handler_Context :: struct {
	client_registry: ^Client_Registry,
	client_id:       u32,
	shutdown_flag:   ^bool,
	quiet_mode:      bool,
}

// =============================================================================
// Listener Thread
// =============================================================================

listener_thread_proc :: proc(t: ^thread.Thread) {
	listener := cast(^Listener)t.data
	
	fmt.printfln("[Listener] Started on port %d", listener.config.port)
	
	// Set a timeout so accept doesn't block forever
	net.set_option(listener.listen_socket, .Receive_Timeout, time.Duration(500 * time.Millisecond))
	
	for !listener.shutdown_flag^ {
		client_socket, client_endpoint, err := net.accept_tcp(listener.listen_socket)
		
		if err != nil {
			if listener.shutdown_flag^ {
				break
			}
			continue
		}
		
		net.set_option(client_socket, .TCP_Nodelay, true)
		
		client_id := registry_add_client(listener.client_registry, client_socket)
		if client_id == 0 {
			fmt.eprintln("[Listener] At capacity, rejecting client")
			net.close(client_socket)
			listener.connections_rejected += 1
			continue
		}
		
		if !listener.config.quiet_mode {
			fmt.printfln("[Listener] Client %d connected from %v", client_id, client_endpoint)
		}
		
		listener.connections_accepted += 1
		
		ctx := new(Client_Handler_Context)
		ctx.client_registry = listener.client_registry
		ctx.client_id = client_id
		ctx.shutdown_flag = listener.shutdown_flag
		ctx.quiet_mode = listener.config.quiet_mode
		
		handler := thread.create(client_handler_thread_proc)
		if handler == nil {
			fmt.eprintfln("[Listener] Failed to create handler for client %d", client_id)
			free(ctx)
			registry_remove_client(listener.client_registry, client_id)
			continue
		}
		
		handler.data = ctx
		thread.start(handler)
	}
	
	fmt.println("[Listener] Stopped")
	fmt.printfln("[Listener] Accepted: %d, Rejected: %d",
		listener.connections_accepted, listener.connections_rejected)
}

// =============================================================================
// Client Handler Thread
// =============================================================================

FRAME_HEADER_SIZE :: 4

client_handler_thread_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Client_Handler_Context)t.data
	if ctx == nil {
		fmt.eprintln("[Handler] ERROR: nil context")
		return
	}
	
	client_id := ctx.client_id
	registry := ctx.client_registry
	quiet_mode := ctx.quiet_mode
	shutdown_flag := ctx.shutdown_flag
	
	free(ctx)
	
	client := registry_get_client(registry, client_id)
	if client == nil {
		fmt.eprintfln("[Handler %d] Client not found", client_id)
		return
	}
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Started", client_id)
	}
	
	// Send welcome
	welcome_buf: [6]u8
	welcome_buf[0] = 0x00
	welcome_buf[1] = 0x00
	welcome_buf[2] = 0x00
	welcome_buf[3] = 0x02
	welcome_buf[4] = protocol.MAGIC
	welcome_buf[5] = protocol.MSG_FLUSH
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Sending welcome", client_id)
	}
	
	_, welcome_err := net.send_tcp(client.socket, welcome_buf[:])
	if welcome_err != nil {
		fmt.eprintfln("[Handler %d] Failed to send welcome: %v", client_id, welcome_err)
		registry_remove_client(registry, client_id)
		return
	}
	
	recv_buffer: [4096]u8
	buffer_used := 0
	send_buffer: [4096]u8
	last_message_time := time.now()
	
	// Main loop
	for !shutdown_flag^ && client.state == .Connected {
		// Always try to send pending output first
		sent := send_pending_output(client, send_buffer[:], quiet_mode)
		if sent > 0 {
			last_message_time = time.now()
		}
		
		// Try to receive with timeout
		net.set_option(client.socket, .Receive_Timeout, time.Duration(50 * time.Millisecond))
		bytes, recv_err := net.recv_tcp(client.socket, recv_buffer[buffer_used:])
		
		if recv_err != nil {
			// Could be timeout or actual error
			// Check if we have pending output to send
			if client_has_pending_output(client) {
				continue
			}
			
			// Check how long since last activity
			elapsed := time.duration_seconds(time.diff(last_message_time, time.now()))
			if elapsed > 2.0 {
				// Timeout with no activity - client probably gone
				if !quiet_mode {
					fmt.printfln("[Handler %d] Timeout, disconnecting", client_id)
				}
				break
			}
			continue
		}
		
		if bytes == 0 {
			// Client closed connection gracefully
			// But we need to wait for any final output (like FLUSH responses)
			if !quiet_mode {
				fmt.printfln("[Handler %d] Client closed connection, draining output...", client_id)
			}
			
			// Drain loop - wait for processor/router and send remaining output
			for drain := 0; drain < 200; drain += 1 {
				time.sleep(10 * time.Millisecond)
				drain_sent := send_pending_output(client, send_buffer[:], quiet_mode)
				if drain_sent == 0 && drain > 50 {
					// No output for a while, probably done
					break
				}
			}
			break
		}
		
		// Got data
		buffer_used += bytes
		client_add_bytes_received(client, u64(bytes))
		last_message_time = time.now()
		
		if !quiet_mode {
			fmt.printfln("[Handler %d] Received %d bytes (buffer: %d)", client_id, bytes, buffer_used)
		}
		
		// Process messages
		processed := process_framed_messages(client, recv_buffer[:buffer_used], quiet_mode)
		
		// Compact buffer
		if processed > 0 && processed < buffer_used {
			for i := 0; i < buffer_used - processed; i += 1 {
				recv_buffer[i] = recv_buffer[processed + i]
			}
		}
		if processed > 0 {
			buffer_used -= processed
		}
	}
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Disconnected (recv=%d, sent=%d)",
			client_id, client.messages_received, client.messages_sent)
	}
	
	registry_remove_client(registry, client_id)
}

// =============================================================================
// Framing Helpers
// =============================================================================

@(private)
read_frame_length :: proc(data: []u8) -> u32 {
	return u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3])
}

@(private)
process_framed_messages :: proc(client: ^Client, data: []u8, quiet_mode: bool) -> int {
	processed := 0
	
	for processed < len(data) {
		remaining := data[processed:]
		
		if len(remaining) < FRAME_HEADER_SIZE {
			break
		}
		
		msg_len := int(read_frame_length(remaining))
		
		if !quiet_mode {
			fmt.printfln("[Handler %d] Frame length: %d", client.client_id, msg_len)
		}
		
		if msg_len <= 0 || msg_len > 1024 {
			processed += 1
			continue
		}
		
		total_frame_size := FRAME_HEADER_SIZE + msg_len
		if len(remaining) < total_frame_size {
			break
		}
		
		msg_data := remaining[FRAME_HEADER_SIZE:total_frame_size]
		
		if !quiet_mode {
			fmt.printf("[Handler %d] Message payload: ", client.client_id)
			for i := 0; i < len(msg_data); i += 1 {
				fmt.printf("%02X ", msg_data[i])
			}
			fmt.println("")
		}
		
		process_binary_message(client, msg_data, quiet_mode)
		processed += total_frame_size
	}
	
	return processed
}

@(private)
process_binary_message :: proc(client: ^Client, data: []u8, quiet_mode: bool) {
	if len(data) < protocol.HEADER_SIZE {
		return
	}
	
	if data[0] != protocol.MAGIC {
		return
	}
	
	msg_type := data[1]
	expected_size := protocol.get_message_size(msg_type)
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Msg type: '%c' (0x%02X), expected: %d, actual: %d", 
			client.client_id, msg_type, msg_type, expected_size, len(data))
	}
	
	if expected_size < 0 || len(data) < expected_size {
		return
	}
	
	result, err := protocol.decode_input(data[:expected_size])
	if err == .None {
		if !quiet_mode {
			switch result.msg_type {
			case protocol.MSG_NEW_ORDER:
				fmt.printfln("[Handler %d] Decoded NEW_ORDER: user=%d, order=%d, price=%d, qty=%d, side=%c",
					client.client_id,
					result.new_order.user_id,
					result.new_order.user_order_id,
					result.new_order.price,
					result.new_order.quantity,
					u8(result.new_order.side))
			case protocol.MSG_CANCEL:
				fmt.printfln("[Handler %d] Decoded CANCEL: user=%d, order=%d",
					client.client_id, result.cancel.user_id, result.cancel.user_order_id)
			case protocol.MSG_FLUSH:
				fmt.printfln("[Handler %d] Decoded FLUSH", client.client_id)
			}
		}
		
		envelope := create_input_envelope(&result, client.client_id)
		if client_enqueue_input(client, &envelope) {
			client_inc_received(client)
			if !quiet_mode {
				fmt.printfln("[Handler %d] Enqueued message", client.client_id)
			}
		}
	}
}

@(private)
create_input_envelope :: proc(result: ^protocol.Input_Decode_Result, client_id: u32) -> Input_Envelope {
	envelope := Input_Envelope{
		client_id = client_id,
		timestamp = 0,
	}
	
	switch result.msg_type {
	case protocol.MSG_NEW_ORDER:
		envelope.msg.msg_type = .New_Order
		envelope.msg.new_order = result.new_order
	case protocol.MSG_CANCEL:
		envelope.msg.msg_type = .Cancel
		envelope.msg.cancel = result.cancel
	case protocol.MSG_FLUSH:
		envelope.msg.msg_type = .Flush
	}
	
	return envelope
}

// =============================================================================
// Send Helpers
// =============================================================================

@(private)
send_pending_output :: proc(client: ^Client, send_buffer: []u8, quiet_mode: bool) -> int {
	sent_count := 0
	
	for i := 0; i < 100; i += 1 {
		msg: Output_Msg
		if !client_dequeue_output(client, &msg) {
			break
		}
		
		bytes_written := encode_output_msg(&msg, send_buffer[FRAME_HEADER_SIZE:])
		if bytes_written <= 0 {
			continue
		}
		
		send_buffer[0] = u8(bytes_written >> 24)
		send_buffer[1] = u8(bytes_written >> 16)
		send_buffer[2] = u8(bytes_written >> 8)
		send_buffer[3] = u8(bytes_written)
		
		total_frame_size := FRAME_HEADER_SIZE + bytes_written
		
		if !quiet_mode {
			fmt.printfln("[Handler %d] Sending %s (%d bytes)", 
				client.client_id, msg_type_str(msg.msg_type), total_frame_size)
		}
		
		total_sent := 0
		for total_sent < total_frame_size {
			sent, err := net.send_tcp(client.socket, send_buffer[total_sent:total_frame_size])
			if err != nil {
				return sent_count
			}
			total_sent += sent
		}
		
		client_add_bytes_sent(client, u64(total_frame_size))
		client_inc_sent(client)
		sent_count += 1
	}
	
	return sent_count
}

@(private)
msg_type_str :: proc(t: Output_Msg_Type) -> string {
	switch t {
	case .Ack:         return "ACK"
	case .Cancel_Ack:  return "CANCEL_ACK"
	case .Trade:       return "TRADE"
	case .Top_Of_Book: return "TOB"
	case .Reject:      return "REJECT"
	}
	return "?"
}

@(private)
encode_output_msg :: proc(msg: ^Output_Msg, buffer: []u8) -> int {
	switch msg.msg_type {
	case .Ack:
		bytes, err := protocol.encode_ack(&msg.ack, buffer)
		return bytes if err == .None else 0
	case .Cancel_Ack:
		bytes, err := protocol.encode_cancel_ack(&msg.cancel_ack, buffer)
		return bytes if err == .None else 0
	case .Trade:
		bytes, err := protocol.encode_trade(&msg.trade, buffer)
		return bytes if err == .None else 0
	case .Top_Of_Book:
		bytes, err := protocol.encode_top_of_book(&msg.top_of_book, buffer)
		return bytes if err == .None else 0
	case .Reject:
		bytes, err := protocol.encode_reject(&msg.reject, buffer)
		return bytes if err == .None else 0
	}
	return 0
}
