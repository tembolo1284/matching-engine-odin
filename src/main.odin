package main

import "core:fmt"
import "core:os"
import "core:thread"
import "core:time"
import "core:strconv"

import "types"
import "protocol"
import "core"
import "net"
import "threading"
import "sync"

// =============================================================================
// Matching Engine - Multi-Threaded Entry Point
// =============================================================================
// Architecture:
//   - Listener Thread: accepts TCP connections, spawns client handlers
//   - Client Handler Threads: recv + send per client (spawned by listener)
//   - Processor Thread: matching engine (hot path, no I/O)
//   - Router Thread: routes output to per-client queues
//
// Data Flow:
//   Client Handlers → (input queues) → Processor → (output queue) → Router → (per-client queues) → Client Handlers
//
// Usage: matching_engine [port]
// =============================================================================

DEFAULT_PORT :: 8080
DEFAULT_SYMBOL :: "ODIN"

// =============================================================================
// Application State
// =============================================================================

App :: struct {
	// Core components
	book:            core.Order_Book,
	client_registry: net.Client_Registry,
	
	// Queues
	output_queue:    threading.Output_Queue,
	
	// Thread contexts
	listener:        net.Listener,
	processor:       threading.Processor,
	router:          threading.Router,
	
	// Threads
	listener_thread:  ^thread.Thread,
	processor_thread: ^thread.Thread,
	router_thread:    ^thread.Thread,
	
	// Control
	shutdown_flag:   bool,
	running:         bool,
}

// Global app state
g_app: App

// =============================================================================
// Main Entry Point
// =============================================================================

main :: proc() {
	fmt.println("╔════════════════════════════════════════╗")
	fmt.println("║     Odin Matching Engine v1.0          ║")
	fmt.println("║     Multi-Threaded Architecture        ║")
	fmt.println("║     Power of Ten Compliant             ║")
	fmt.println("╚════════════════════════════════════════╝")
	fmt.println("")
	
	// Parse command line
	port := parse_port()
	
	// Initialize application
	init_err := app_init(&g_app, port)
	if init_err != .None {
		fmt.eprintfln("Failed to initialize: %s", types.error_string(init_err))
		os.exit(1)
	}
	
	fmt.printfln("Configuration:")
	fmt.printfln("  Port:     %d", port)
	fmt.printfln("  Symbol:   %s", DEFAULT_SYMBOL)
	fmt.printfln("  Clients:  max %d", net.MAX_CLIENTS)
	fmt.println("")
	
	fmt.println("Thread Architecture:")
	fmt.println("  [Listener]  → accepts connections, spawns handlers")
	fmt.println("  [Handlers]  → per-client recv/send (spawned on connect)")
	fmt.println("  [Processor] → matching engine (hot path)")
	fmt.println("  [Router]    → output fan-out to clients")
	fmt.println("")
	
	// Start threads
	start_err := app_start(&g_app)
	if start_err != .None {
		fmt.eprintfln("Failed to start: %s", types.error_string(start_err))
		os.exit(1)
	}
	
	fmt.println("Server running. Press Ctrl+C to stop.")
	fmt.println("")
	
	// Wait for shutdown signal (simple busy wait for now)
	// In production, use signal handlers
	app_wait_for_shutdown(&g_app)
	
	// Shutdown
	app_shutdown(&g_app)
	
	fmt.println("")
	fmt.println("Shutdown complete.")
}

// Parse port from command line
parse_port :: proc() -> u16 {
	args := os.args
	
	if len(args) >= 2 {
		port, ok := strconv.parse_int(args[1])
		if ok && port > 0 && port < 65536 {
			return u16(port)
		}
		fmt.eprintfln("Invalid port '%s', using default %d", args[1], DEFAULT_PORT)
	}
	
	return DEFAULT_PORT
}

// =============================================================================
// Application Lifecycle
// =============================================================================

// Initialize all components
app_init :: proc(app: ^App, port: u16) -> types.Error {
	app.shutdown_flag = false
	app.running = false
	
	// Initialize order book
	symbol := protocol.make_symbol(DEFAULT_SYMBOL)
	book_err := core.book_init(&app.book, symbol)
	if book_err != .None {
		fmt.eprintln("Failed to initialize order book")
		return book_err
	}
	
	// Initialize client registry
	net.registry_init(&app.client_registry)
	
	// Initialize output queue
	sync.spsc_init(&app.output_queue)
	
	// Initialize listener
	listener_config := net.Listener_Config{
		port       = port,
		quiet_mode = false,
	}
	listener_err := net.listener_init(&app.listener, listener_config, &app.client_registry, &app.shutdown_flag)
	if listener_err != .None {
		fmt.eprintln("Failed to initialize listener")
		return listener_err
	}
	
	// Initialize processor
	processor_config := threading.Processor_Config{
		processor_id = 0,
		spin_wait    = true,  // Spin for lowest latency
	}
	threading.processor_init(
		&app.processor,
		processor_config,
		&app.book,
		&app.client_registry,
		&app.output_queue,
		&app.shutdown_flag,
	)
	
	// Initialize router
	router_config := threading.Router_Config{
		tcp_mode = true,
	}
	threading.router_init(
		&app.router,
		router_config,
		&app.client_registry,
		&app.output_queue,
		&app.shutdown_flag,
	)
	
	return .None
}

// Start all threads
app_start :: proc(app: ^App) -> types.Error {
	// Create and start listener thread
	app.listener_thread = thread.create(net.listener_thread, &app.listener)
	if app.listener_thread == nil {
		fmt.eprintln("Failed to create listener thread")
		return .Internal
	}
	
	// Create and start processor thread
	app.processor_thread = thread.create(threading.processor_thread, &app.processor)
	if app.processor_thread == nil {
		fmt.eprintln("Failed to create processor thread")
		return .Internal
	}
	
	// Create and start router thread
	app.router_thread = thread.create(threading.router_thread, &app.router)
	if app.router_thread == nil {
		fmt.eprintln("Failed to create router thread")
		return .Internal
	}
	
	// Start threads
	thread.start(app.listener_thread)
	thread.start(app.processor_thread)
	thread.start(app.router_thread)
	
	app.running = true
	
	fmt.println("All threads started:")
	fmt.println("  ✓ Listener thread")
	fmt.println("  ✓ Processor thread")
	fmt.println("  ✓ Router thread")
	
	return .None
}

// Wait for shutdown (simple polling for now)
app_wait_for_shutdown :: proc(app: ^App) {
	// In a real implementation, use signal handlers
	// For now, just sleep and check periodically
	for app.running && !app.shutdown_flag {
		time.sleep(100 * time.Millisecond)
		
		// Print periodic status (every 10 seconds)
		// Could add stats here
	}
}

// Shutdown all components
app_shutdown :: proc(app: ^App) {
	fmt.println("")
	fmt.println("Initiating shutdown...")
	
	// Signal all threads to stop
	app.shutdown_flag = true
	app.running = false
	
	// Wait a moment for threads to notice
	time.sleep(100 * time.Millisecond)
	
	// Wait for threads to finish
	if app.listener_thread != nil {
		thread.join(app.listener_thread)
		thread.destroy(app.listener_thread)
	}
	
	if app.processor_thread != nil {
		thread.join(app.processor_thread)
		thread.destroy(app.processor_thread)
	}
	
	if app.router_thread != nil {
		thread.join(app.router_thread)
		thread.destroy(app.router_thread)
	}
	
	// Print final statistics
	print_final_stats(app)
	
	// Cleanup
	net.registry_destroy(&app.client_registry)
}

// Print final statistics
print_final_stats :: proc(app: ^App) {
	fmt.println("")
	fmt.println("╔════════════════════════════════════════╗")
	fmt.println("║          Final Statistics              ║")
	fmt.println("╚════════════════════════════════════════╝")
	
	// Order book stats
	fmt.println("")
	fmt.println("Order Book:")
	fmt.printfln("  Orders in book:  %d", core.book_order_count(&app.book))
	fmt.printfln("  Best bid:        %d", core.book_best_bid(&app.book))
	fmt.printfln("  Best ask:        %d", core.book_best_ask(&app.book))
	fmt.printfln("  Spread:          %d", core.book_spread(&app.book))
	
	// Client stats
	fmt.println("")
	fmt.printfln("Clients: %d active", net.registry_get_active_count(&app.client_registry))
	
	// Processor stats already printed by processor_print_stats
	// Router stats already printed by router_print_stats
}
