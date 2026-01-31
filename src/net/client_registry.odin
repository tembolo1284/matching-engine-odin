package net

import "core:net"
import "core:sync"
import "core:thread"
import "../types"
import "../protocol"
import "../threading"
import spsc "../sync"

// =============================================================================
// Client Registry
// =============================================================================
// Manages per-client connection state for multi-client TCP server.
// Each client gets:
//   - Unique client_id (1-based, 0 = invalid)
//   - Dedicated input queue (client handler → processor)
//   - Dedicated output queue (router → client handler)
//   - Socket and connection state
//
// Thread Safety:
//   - Lock protects add/remove operations only
//   - Individual client access is lock-free after lookup
//   - Queue operations are lock-free (SPSC)
// =============================================================================

// Maximum simultaneous TCP clients
MAX_CLIENTS :: 100

// =============================================================================
// Per-Client State
// =============================================================================

// Client connection state
Client_State :: enum u8 {
	Inactive   = 0,
	Connected  = 1,
	Draining   = 2,   // Sending remaining data before close
}

// Per-client structure
Client :: struct {
	// === Hot fields (frequently accessed) ===
	socket:      net.TCP_Socket,
	client_id:   u32,              // 1-based ID (0 = invalid)
	state:       Client_State,
	protocol:    Client_Protocol,
	
	// Handler thread
	handler_thread: ^thread.Thread,
	
	// === Queues ===
	// Input queue: client handler produces, processor consumes
	input_queue:  threading.Input_Queue,
	
	// Output queue: router produces, client handler consumes
	output_queue: threading.Client_Output_Queue,
	
	// === Statistics ===
	messages_received: u64,
	messages_sent:     u64,
	bytes_received:    u64,
	bytes_sent:        u64,
}

// Protocol detection
Client_Protocol :: enum u8 {
	Unknown = 0,
	Binary  = 1,
	CSV     = 2,
}

// =============================================================================
// Client Registry
// =============================================================================

Client_Registry :: struct {
	clients:      [MAX_CLIENTS]Client,
	active_count: u32,
	lock:         sync.Mutex,
}

// =============================================================================
// Initialization
// =============================================================================

// Initialize the client registry
registry_init :: proc(registry: ^Client_Registry) {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	
	registry.active_count = 0
	
	// Initialize all clients as inactive
	for i := 0; i < MAX_CLIENTS; i += 1 {
		registry.clients[i].client_id = 0
		registry.clients[i].state = .Inactive
		registry.clients[i].handler_thread = nil
	}
}

// Destroy the registry (closes all connections)
registry_destroy :: proc(registry: ^Client_Registry) {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	
	for i := 0; i < MAX_CLIENTS; i += 1 {
		client := &registry.clients[i]
		if client.state != .Inactive {
			// Close socket
			net.close(client.socket)
			client.state = .Inactive
			client.client_id = 0
		}
	}
	
	registry.active_count = 0
}

// =============================================================================
// Client Management
// =============================================================================

// Add a new client connection
// Returns client_id (1-based) on success, 0 on failure
registry_add_client :: proc(
	registry: ^Client_Registry,
	socket: net.TCP_Socket,
) -> u32 {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	
	// Check capacity
	if registry.active_count >= MAX_CLIENTS {
		return 0
	}
	
	// Find first available slot
	for i := 0; i < MAX_CLIENTS; i += 1 {
		client := &registry.clients[i]
		if client.state == .Inactive {
			// Initialize client
			client.socket = socket
			client.client_id = u32(i + 1)  // 1-based IDs
			client.state = .Connected
			client.protocol = .Unknown
			client.handler_thread = nil
			
			// Initialize queues
			spsc.spsc_init(&client.input_queue)
			spsc.spsc_init(&client.output_queue)
			
			// Reset statistics
			client.messages_received = 0
			client.messages_sent = 0
			client.bytes_received = 0
			client.bytes_sent = 0
			
			registry.active_count += 1
			
			return client.client_id
		}
	}
	
	return 0  // No free slot (shouldn't happen if active_count is accurate)
}

// Remove a client connection
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
	
	// Close socket
	net.close(client.socket)
	
	// Mark inactive
	client.state = .Inactive
	client.client_id = 0
	
	if registry.active_count > 0 {
		registry.active_count -= 1
	}
}

// Get client by ID (lock-free read after initial setup)
registry_get_client :: proc(registry: ^Client_Registry, client_id: u32) -> ^Client {
	if client_id == 0 || client_id > MAX_CLIENTS {
		return nil
	}
	
	client := &registry.clients[client_id - 1]
	
	// Volatile read of state
	if client.state == .Inactive {
		return nil
	}
	
	return client
}

// Get active client count
registry_get_active_count :: proc(registry: ^Client_Registry) -> u32 {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	return registry.active_count
}

// Set client's handler thread
registry_set_handler_thread :: proc(registry: ^Client_Registry, client_id: u32, t: ^thread.Thread) {
	client := registry_get_client(registry, client_id)
	if client != nil {
		client.handler_thread = t
	}
}

// Set client's detected protocol
registry_set_protocol :: proc(registry: ^Client_Registry, client_id: u32, proto: Client_Protocol) {
	client := registry_get_client(registry, client_id)
	if client != nil {
		client.protocol = proto
	}
}

// =============================================================================
// Queue Operations (lock-free)
// =============================================================================

// Enqueue input message to client's input queue (client handler → processor)
client_enqueue_input :: proc(client: ^Client, envelope: ^threading.Input_Envelope) -> bool {
	if client == nil || client.state != .Connected {
		return false
	}
	return spsc.spsc_enqueue(&client.input_queue, envelope)
}

// Dequeue input message from client's input queue (processor side)
client_dequeue_input :: proc(client: ^Client, envelope: ^threading.Input_Envelope) -> bool {
	if client == nil {
		return false
	}
	return spsc.spsc_dequeue(&client.input_queue, envelope)
}

// Batch dequeue from client's input queue (processor side)
client_dequeue_input_batch :: proc(client: ^Client, envelopes: []threading.Input_Envelope, max_items: u32) -> u32 {
	if client == nil {
		return 0
	}
	return spsc.spsc_dequeue_batch(&client.input_queue, envelopes, max_items)
}

// Enqueue output message to client's output queue (router → client handler)
client_enqueue_output :: proc(client: ^Client, msg: ^threading.Output_Msg) -> bool {
	if client == nil || client.state == .Inactive {
		return false
	}
	return spsc.spsc_enqueue(&client.output_queue, msg)
}

// Dequeue output message from client's output queue (client handler side)
client_dequeue_output :: proc(client: ^Client, msg: ^threading.Output_Msg) -> bool {
	if client == nil {
		return false
	}
	return spsc.spsc_dequeue(&client.output_queue, msg)
}

// Check if client has pending output
client_has_pending_output :: proc(client: ^Client) -> bool {
	if client == nil {
		return false
	}
	return !spsc.spsc_is_empty(&client.output_queue)
}

// =============================================================================
// Statistics
// =============================================================================

// Increment received message count
client_inc_received :: proc(client: ^Client) {
	if client != nil {
		client.messages_received += 1
	}
}

// Increment sent message count
client_inc_sent :: proc(client: ^Client) {
	if client != nil {
		client.messages_sent += 1
	}
}

// Add to bytes received
client_add_bytes_received :: proc(client: ^Client, bytes: u64) {
	if client != nil {
		client.bytes_received += bytes
	}
}

// Add to bytes sent
client_add_bytes_sent :: proc(client: ^Client, bytes: u64) {
	if client != nil {
		client.bytes_sent += bytes
	}
}

// =============================================================================
// Iteration (for processor round-robin)
// =============================================================================

// Get array of all client slots (for iteration)
// Caller should check client.state before using
registry_get_all_clients :: proc(registry: ^Client_Registry) -> []Client {
	return registry.clients[:]
}

// Get list of active client IDs
// Returns count of active clients written to client_ids array
registry_get_active_client_ids :: proc(
	registry: ^Client_Registry,
	client_ids: []u32,
) -> u32 {
	sync.mutex_lock(&registry.lock)
	defer sync.mutex_unlock(&registry.lock)
	
	count: u32 = 0
	max_count := u32(len(client_ids))
	
	for i := 0; i < MAX_CLIENTS && count < max_count; i += 1 {
		client := &registry.clients[i]
		if client.state != .Inactive {
			client_ids[count] = client.client_id
			count += 1
		}
	}
	
	return count
}
