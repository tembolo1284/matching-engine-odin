package engine

import "core:net"
import "core:sync"
import "core:thread"
import "../lockfree"

// =============================================================================
// Client State
// =============================================================================

MAX_CLIENTS :: 100

Client_State :: enum u8 {
	Inactive,
	Connected,
	Draining,
}

Client_Protocol :: enum u8 {
	Unknown,
	Binary,
	CSV,
}

Client :: struct {
	socket:         net.TCP_Socket,
	client_id:      u32,
	state:          Client_State,
	protocol:       Client_Protocol,
	handler_thread: ^thread.Thread,
	
	input_queue:    Input_Queue,
	output_queue:   Client_Output_Queue,
	
	messages_received: u64,
	messages_sent:     u64,
	bytes_received:    u64,
	bytes_sent:        u64,
}

// =============================================================================
// Client Registry
// =============================================================================

Client_Registry :: struct {
	clients:      [MAX_CLIENTS]Client,
	active_count: u32,
	lock:         sync.Mutex,
}

registry_init :: proc(registry: ^Client_Registry) {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	
	registry.active_count = 0
	for i := 0; i < MAX_CLIENTS; i += 1 {
		registry.clients[i].client_id = 0
		registry.clients[i].state = .Inactive
		registry.clients[i].handler_thread = nil
	}
}

registry_destroy :: proc(registry: ^Client_Registry) {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	
	for i := 0; i < MAX_CLIENTS; i += 1 {
		client := &registry.clients[i]
		if client.state != .Inactive {
			net.close(client.socket)
			client.state = .Inactive
			client.client_id = 0
		}
	}
	registry.active_count = 0
}

registry_add_client :: proc(registry: ^Client_Registry, socket: net.TCP_Socket) -> u32 {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	
	if registry.active_count >= MAX_CLIENTS {
		return 0
	}
	
	for i := 0; i < MAX_CLIENTS; i += 1 {
		client := &registry.clients[i]
		if client.state == .Inactive {
			client.socket = socket
			client.client_id = u32(i + 1)
			client.state = .Connected
			client.protocol = .Unknown
			client.handler_thread = nil
			
			lockfree.spsc_init(&client.input_queue)
			lockfree.spsc_init(&client.output_queue)
			
			client.messages_received = 0
			client.messages_sent = 0
			client.bytes_received = 0
			client.bytes_sent = 0
			
			registry.active_count += 1
			return client.client_id
		}
	}
	return 0
}

registry_remove_client :: proc(registry: ^Client_Registry, client_id: u32) {
	if client_id == 0 || client_id > MAX_CLIENTS {
		return
	}
	
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	
	client := &registry.clients[client_id - 1]
	if client.state == .Inactive {
		return
	}
	
	net.close(client.socket)
	client.state = .Inactive
	client.client_id = 0
	
	if registry.active_count > 0 {
		registry.active_count -= 1
	}
}

registry_get_client :: proc(registry: ^Client_Registry, client_id: u32) -> ^Client {
	if client_id == 0 || client_id > MAX_CLIENTS {
		return nil
	}
	client := &registry.clients[client_id - 1]
	if client.state == .Inactive {
		return nil
	}
	return client
}

registry_get_active_count :: proc(registry: ^Client_Registry) -> u32 {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	return registry.active_count
}

registry_get_all_clients :: proc(registry: ^Client_Registry) -> []Client {
	return registry.clients[:]
}

// =============================================================================
// Client Queue Operations
// =============================================================================

client_enqueue_input :: proc(client: ^Client, envelope: ^Input_Envelope) -> bool {
	if client == nil || client.state != .Connected {
		return false
	}
	return lockfree.spsc_enqueue(&client.input_queue, envelope)
}

client_dequeue_input :: proc(client: ^Client, envelope: ^Input_Envelope) -> bool {
	if client == nil {
		return false
	}
	return lockfree.spsc_dequeue(&client.input_queue, envelope)
}

client_dequeue_input_batch :: proc(client: ^Client, envelopes: []Input_Envelope, max_items: u32) -> u32 {
	if client == nil {
		return 0
	}
	return lockfree.spsc_dequeue_batch(&client.input_queue, envelopes, max_items)
}

client_enqueue_output :: proc(client: ^Client, msg: ^Output_Msg) -> bool {
	if client == nil || client.state == .Inactive {
		return false
	}
	return lockfree.spsc_enqueue(&client.output_queue, msg)
}

client_dequeue_output :: proc(client: ^Client, msg: ^Output_Msg) -> bool {
	if client == nil {
		return false
	}
	return lockfree.spsc_dequeue(&client.output_queue, msg)
}

client_has_pending_output :: proc(client: ^Client) -> bool {
	if client == nil {
		return false
	}
	return !lockfree.spsc_is_empty(&client.output_queue)
}

// =============================================================================
// Client Stats
// =============================================================================

client_inc_received :: proc(client: ^Client) {
	if client != nil {
		client.messages_received += 1
	}
}

client_inc_sent :: proc(client: ^Client) {
	if client != nil {
		client.messages_sent += 1
	}
}

client_add_bytes_received :: proc(client: ^Client, bytes: u64) {
	if client != nil {
		client.bytes_received += bytes
	}
}

client_add_bytes_sent :: proc(client: ^Client, bytes: u64) {
	if client != nil {
		client.bytes_sent += bytes
	}
}
