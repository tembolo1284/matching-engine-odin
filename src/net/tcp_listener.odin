package net

// =============================================================================
// TCP Listener Thread
// =============================================================================
// - Listens for new TCP connections
// - Registers clients in the registry
// - Spawns dedicated handler thread per client
// =============================================================================

Listener :: struct {
	config:          Listener_Config,
	listen_socket:   net.TCP_Socket,
	client_registry: ^Client_Registry,
	shutdown_flag:   ^bool,
}

listener_thread :: proc(arg: rawptr) {
	// Accept loop
	// Register client
	// Spawn handler thread
}

client_handler_thread :: proc(arg: rawptr) {
	// Receive data → parse → enqueue to input queue
	// Dequeue from output queue → encode → send
}
