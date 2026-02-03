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

client_handler_thread_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Client_Handler_Context)t.data
	defer free(ctx)
	
	client_id := ctx.client_id
	registry := ctx.client_registry
	
	client := registry_get_client(registry, client_id)
	if client == nil {
		fmt.eprintfln("[Handler %d] Client not found", client_id)
		return
	}
	
	if !ctx.quiet_mode {
		fmt.printfln("[Handler %d] Started", client_id)
	}
	
	recv_buffer: [4096]u8
	buffer_used := 0
	send_buffer: [4096]u8
	detected_protocol := Client_Protocol.Unknown
	
	for !ctx.shutdown_flag^ && client.state == .Connected {
		// Receive
		bytes_read := receive_data(client, recv_buffer[buffer_used:])
		
		if bytes_read < 0 {
			break
		}
		
		if bytes_read > 0 {
			buffer_used += bytes_read
			client_add_bytes_received(client, u64(bytes_read))
			
			if detected_protocol == .Unknown && buffer_used >= 2 {
				detected_protocol = detect_protocol(recv_buffer[:buffer_used])
				client.protocol = detected_protocol
				if !ctx.quiet_mode {
					fmt.printfln("[Handler %d] Protocol: %s",
						client_id, detected_protocol == .Binary ? "BINARY" : "CSV")
				}
			}
			
			processed := process_received_data(client, recv_buffer[:buffer_used], detected_protocol)
			
			if processed > 0 {
				if processed < buffer_used {
					for i := 0; i < buffer_used - processed; i += 1 {
						recv_buffer[i] = recv_buffer[processed + i]
					}
				}
				buffer_used -= processed
			}
		}
		
		// Send
		send_pending_output(client, send_buffer[:])
		
		if bytes_read == 0 && !client_has_pending_output(client) {
			time.sleep(100 * time.Microsecond)
		}
	}
	
	if !ctx.quiet_mode {
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

@(private)
detect_protocol :: proc(data: []u8) -> Client_Protocol {
	if len(data) < 2 {
		return .Unknown
	}
	
	if data[0] == protocol.MAGIC {
		return .Binary
	}
	
	if data[0] == 'N' || data[0] == 'C' || data[0] == 'F' {
		return .CSV
	}
	
	return .Unknown
}

@(private)
process_received_data :: proc(client: ^Client, data: []u8, proto: Client_Protocol) -> int {
	if proto != .Binary {
		return 0
	}
	
	processed := 0
	
	for processed < len(data) {
		remaining := data[processed:]
		
		if len(remaining) < protocol.HEADER_SIZE {
			break
		}
		
		if remaining[0] != protocol.MAGIC {
			processed += 1
			continue
		}
		
		msg_size := protocol.get_message_size(remaining[1])
		if msg_size < 0 {
			processed += 1
			continue
		}
		
		if len(remaining) < msg_size {
			break
		}
		
		result, err := protocol.decode_input(remaining[:msg_size])
		if err == .None {
			envelope := create_input_envelope(&result, client.client_id)
			if client_enqueue_input(client, &envelope) {
				client_inc_received(client)
			}
		}
		
		processed += msg_size
	}
	
	return processed
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
		
		bytes_written := encode_output_msg(&msg, send_buffer)
		if bytes_written <= 0 {
			continue
		}
		
		total_sent := 0
		for total_sent < bytes_written {
			sent, err := net.send_tcp(client.socket, send_buffer[total_sent:bytes_written])
			if err != nil {
				return
			}
			total_sent += sent
		}
		
		client_add_bytes_sent(client, u64(bytes_written))
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
