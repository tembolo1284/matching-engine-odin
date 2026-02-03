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
	
	for !listener.shutdown_flag^ {
		client_socket, client_endpoint, err := net.accept_tcp(listener.listen_socket)
		
		if err != nil {
			if listener.shutdown_flag^ {
				break
			}
			time.sleep(10 * time.Millisecond)
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
		
		// Spawn handler
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
	
	net.close(listener.listen_socket)
	fmt.println("[Listener] Stopped")
	fmt.printfln("[Listener] Accepted: %d, Rejected: %d",
		listener.connections_accepted, listener.connections_rejected)
}

// =============================================================================
// Client Handler Thread
// =============================================================================

// Length prefix size (4 bytes, big-endian)
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
	
	// Free context early - we've copied what we need
	free(ctx)
	
	client := registry_get_client(registry, client_id)
	if client == nil {
		fmt.eprintfln("[Handler %d] Client not found", client_id)
		return
	}
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Started", client_id)
	}
	
	// Send welcome/probe response with length prefix
	// Frame: length(4) + magic(1) + type(1) = 6 bytes total
	welcome_buf: [6]u8
	welcome_buf[0] = 0x00  // Length = 2 (big-endian)
	welcome_buf[1] = 0x00
	welcome_buf[2] = 0x00
	welcome_buf[3] = 0x02
	welcome_buf[4] = protocol.MAGIC      // 0x4D 'M'
	welcome_buf[5] = protocol.MSG_FLUSH  // 0x46 'F'
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Sending welcome with frame header", client_id)
	}
	
	sent_bytes, welcome_err := net.send_tcp(client.socket, welcome_buf[:])
	if welcome_err != nil {
		fmt.eprintfln("[Handler %d] Failed to send welcome: %v", client_id, welcome_err)
		registry_remove_client(registry, client_id)
		return
	}
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Sent welcome (%d bytes)", client_id, sent_bytes)
	}
	
	recv_buffer: [4096]u8
	buffer_used := 0
	send_buffer: [4096]u8
	
	for !shutdown_flag^ && client.state == .Connected {
		// Receive
		bytes_read := receive_data(client, recv_buffer[buffer_used:])
		
		if bytes_read < 0 {
			break
		}
		
		if bytes_read > 0 {
			buffer_used += bytes_read
			client_add_bytes_received(client, u64(bytes_read))
			
			if !quiet_mode {
				fmt.printfln("[Handler %d] Received %d bytes (buffer: %d)", 
					client_id, bytes_read, buffer_used)
			}
			
			// Process length-prefixed binary messages
			processed := process_framed_messages(client, recv_buffer[:buffer_used], quiet_mode)
			
			// Compact buffer
			if processed > 0 {
				if processed < buffer_used {
					for i := 0; i < buffer_used - processed; i += 1 {
						recv_buffer[i] = recv_buffer[processed + i]
					}
				}
				buffer_used -= processed
			}
		}
		
		// Send pending output
		send_pending_output(client, send_buffer[:])
		
		if bytes_read == 0 && !client_has_pending_output(client) {
			time.sleep(100 * time.Microsecond)
		}
	}
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Disconnected (recv=%d, sent=%d)",
			client_id, client.messages_received, client.messages_sent)
	}
	
	registry_remove_client(registry, client_id)
}

// =============================================================================
// Receive Helpers
// =============================================================================

@(private)
receive_data :: proc(client: ^Client, buffer: []u8) -> int {
	if len(buffer) == 0 {
		return 0
	}
	
	bytes, err := net.recv_tcp(client.socket, buffer)
	
	if err != nil {
		return -1
	}
	
	if bytes == 0 {
		return -1
	}
	
	return bytes
}

// Read 4-byte big-endian length
@(private)
read_frame_length :: proc(data: []u8) -> u32 {
	return u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3])
}

// Process messages with 4-byte length prefix framing
@(private)
process_framed_messages :: proc(client: ^Client, data: []u8, quiet_mode: bool) -> int {
	processed := 0
	
	for processed < len(data) {
		remaining := data[processed:]
		
		// Need at least frame header (4 bytes)
		if len(remaining) < FRAME_HEADER_SIZE {
			break
		}
		
		// Read message length from frame header
		msg_len := int(read_frame_length(remaining))
		
		if !quiet_mode {
			fmt.printfln("[Handler %d] Frame length: %d", client.client_id, msg_len)
		}
		
		// Sanity check on length
		if msg_len <= 0 || msg_len > 1024 {
			fmt.printfln("[Handler %d] Invalid frame length: %d, skipping byte", 
				client.client_id, msg_len)
			processed += 1
			continue
		}
		
		// Wait for complete frame (header + payload)
		total_frame_size := FRAME_HEADER_SIZE + msg_len
		if len(remaining) < total_frame_size {
			if !quiet_mode {
				fmt.printfln("[Handler %d] Incomplete frame, have %d need %d", 
					client.client_id, len(remaining), total_frame_size)
			}
			break
		}
		
		// Extract message payload (skip frame header)
		msg_data := remaining[FRAME_HEADER_SIZE:total_frame_size]
		
		if !quiet_mode {
			fmt.printf("[Handler %d] Message payload: ", client.client_id)
			for i := 0; i < len(msg_data); i += 1 {
				fmt.printf("%02X ", msg_data[i])
			}
			fmt.println("")
		}
		
		// Process the binary message
		process_binary_message(client, msg_data, quiet_mode)
		
		processed += total_frame_size
	}
	
	return processed
}

// Process a single binary message (without frame header)
@(private)
process_binary_message :: proc(client: ^Client, data: []u8, quiet_mode: bool) {
	if len(data) < protocol.HEADER_SIZE {
		fmt.printfln("[Handler %d] Message too short: %d bytes", client.client_id, len(data))
		return
	}
	
	// Check magic byte
	if data[0] != protocol.MAGIC {
		fmt.printfln("[Handler %d] Bad magic: 0x%02X (expected 0x%02X)", 
			client.client_id, data[0], protocol.MAGIC)
		return
	}
	
	msg_type := data[1]
	expected_size := protocol.get_message_size(msg_type)
	
	if !quiet_mode {
		fmt.printfln("[Handler %d] Msg type: '%c' (0x%02X), expected size: %d, actual: %d", 
			client.client_id, msg_type, msg_type, expected_size, len(data))
	}
	
	if expected_size < 0 {
		fmt.printfln("[Handler %d] Unknown message type: 0x%02X", client.client_id, msg_type)
		return
	}
	
	if len(data) < expected_size {
		fmt.printfln("[Handler %d] Message too short for type", client.client_id)
		return
	}
	
	// Decode and enqueue
	result, err := protocol.decode_input(data[:expected_size])
	if err == .None {
		if !quiet_mode {
			if result.msg_type == protocol.MSG_NEW_ORDER {
				fmt.printfln("[Handler %d] Decoded NEW_ORDER: user=%d, order=%d, price=%d, qty=%d, side=%c",
					client.client_id,
					result.new_order.user_id,
					result.new_order.user_order_id,
					result.new_order.price,
					result.new_order.quantity,
					u8(result.new_order.side))
			} else if result.msg_type == protocol.MSG_CANCEL {
				fmt.printfln("[Handler %d] Decoded CANCEL: user=%d, order=%d",
					client.client_id,
					result.cancel.user_id,
					result.cancel.user_order_id)
			} else if result.msg_type == protocol.MSG_FLUSH {
				fmt.printfln("[Handler %d] Decoded FLUSH", client.client_id)
			}
		}
		
		envelope := create_input_envelope(&result, client.client_id)
		if client_enqueue_input(client, &envelope) {
			client_inc_received(client)
			if !quiet_mode {
				fmt.printfln("[Handler %d] Enqueued message", client.client_id)
			}
		} else {
			fmt.printfln("[Handler %d] Failed to enqueue message", client.client_id)
		}
	} else {
		fmt.printfln("[Handler %d] Decode error: %v", client.client_id, err)
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
send_pending_output :: proc(client: ^Client, send_buffer: []u8) {
	MAX_SEND_BATCH :: 10
	
	for i := 0; i < MAX_SEND_BATCH; i += 1 {
		msg: Output_Msg
		if !client_dequeue_output(client, &msg) {
			break
		}
		
		// Encode message starting after frame header
		bytes_written := encode_output_msg(&msg, send_buffer[FRAME_HEADER_SIZE:])
		if bytes_written <= 0 {
			continue
		}
		
		// Write frame header (4-byte big-endian length)
		send_buffer[0] = u8(bytes_written >> 24)
		send_buffer[1] = u8(bytes_written >> 16)
		send_buffer[2] = u8(bytes_written >> 8)
		send_buffer[3] = u8(bytes_written)
		
		total_frame_size := FRAME_HEADER_SIZE + bytes_written
		
		total_sent := 0
		for total_sent < total_frame_size {
			sent, err := net.send_tcp(client.socket, send_buffer[total_sent:total_frame_size])
			if err != nil {
				return
			}
			total_sent += sent
		}
		
		client_add_bytes_sent(client, u64(total_frame_size))
		client_inc_sent(client)
	}
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
