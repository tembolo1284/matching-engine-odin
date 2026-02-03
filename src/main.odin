package main

import "core:fmt"
import "core:os"
import "core:net"
import "core:thread"
import "core:time"
import "core:strconv"
import "core:c/libc"
import "base:runtime"

import "types"
import "protocol"
import "core"
import "engine"
import "lockfree"

// =============================================================================
// Matching Engine Entry Point
// =============================================================================

DEFAULT_PORT :: 1234
DEFAULT_SYMBOL :: "ODIN"

App :: struct {
	book:            core.Order_Book,
	client_registry: engine.Client_Registry,
	output_queue:    engine.Output_Queue,
	
	listener:        engine.Listener,
	processor:       engine.Processor,
	router:          engine.Router,
	
	listener_thread:  ^thread.Thread,
	processor_thread: ^thread.Thread,
	router_thread:    ^thread.Thread,
	
	shutdown_flag:   bool,
	running:         bool,
}

g_app: App

// Signal handler
signal_handler :: proc "c" (sig: i32) {
	context = runtime.default_context()
	g_app.shutdown_flag = true
	g_app.running = false
	// Close listener socket to unblock accept
	net.close(g_app.listener.listen_socket)
}

main :: proc() {
	fmt.println("╔════════════════════════════════════════╗")
	fmt.println("║     Odin Matching Engine v1.0          ║")
	fmt.println("╚════════════════════════════════════════╝")
	fmt.println("")
	
	// Install signal handlers
	libc.signal(libc.SIGINT, signal_handler)
	libc.signal(libc.SIGTERM, signal_handler)
	
	port := parse_port()
	
	init_err := app_init(&g_app, port)
	if init_err != .None {
		fmt.eprintfln("Failed to initialize: %s", types.error_string(init_err))
		os.exit(1)
	}
	
	fmt.printfln("Port:   %d", port)
	fmt.printfln("Symbol: %s", DEFAULT_SYMBOL)
	fmt.println("")
	
	start_err := app_start(&g_app)
	if start_err != .None {
		fmt.eprintfln("Failed to start: %s", types.error_string(start_err))
		os.exit(1)
	}
	
	fmt.println("Server running. Press Ctrl+C to stop.")
	fmt.println("")
	
	app_wait(&g_app)
	app_shutdown(&g_app)
	
	fmt.println("Shutdown complete.")
}

parse_port :: proc() -> u16 {
	args := os.args
	if len(args) >= 2 {
		port, ok := strconv.parse_int(args[1])
		if ok && port > 0 && port < 65536 {
			return u16(port)
		}
	}
	return DEFAULT_PORT
}

app_init :: proc(app: ^App, port: u16) -> types.Error {
	app.shutdown_flag = false
	app.running = false
	
	symbol := protocol.make_symbol(DEFAULT_SYMBOL)
	book_err := core.book_init(&app.book, symbol)
	if book_err != .None {
		return book_err
	}
	
	engine.registry_init(&app.client_registry)
	lockfree.spsc_init(&app.output_queue)
	
	listener_config := engine.Listener_Config{
		port       = port,
		quiet_mode = false,
	}
	listener_err := engine.listener_init(&app.listener, listener_config, &app.client_registry, &app.shutdown_flag)
	if listener_err != .None {
		return listener_err
	}
	
	processor_config := engine.Processor_Config{
		processor_id = 0,
		spin_wait    = false,
	}
	engine.processor_init(&app.processor, processor_config, &app.book, &app.client_registry, &app.output_queue, &app.shutdown_flag)
	
	router_config := engine.Router_Config{
		tcp_mode = true,
	}
	engine.router_init(&app.router, router_config, &app.client_registry, &app.output_queue, &app.shutdown_flag)
	
	return .None
}

app_start :: proc(app: ^App) -> types.Error {
	app.listener_thread = thread.create(engine.listener_thread_proc)
	if app.listener_thread == nil {
		return .Internal
	}
	app.listener_thread.data = &app.listener
	
	app.processor_thread = thread.create(engine.processor_thread_proc)
	if app.processor_thread == nil {
		return .Internal
	}
	app.processor_thread.data = &app.processor
	
	app.router_thread = thread.create(engine.router_thread_proc)
	if app.router_thread == nil {
		return .Internal
	}
	app.router_thread.data = &app.router
	
	thread.start(app.listener_thread)
	thread.start(app.processor_thread)
	thread.start(app.router_thread)
	
	app.running = true
	
	fmt.println("Threads started:")
	fmt.println("  ✓ Listener")
	fmt.println("  ✓ Processor")
	fmt.println("  ✓ Router")
	
	return .None
}

app_wait :: proc(app: ^App) {
	for app.running && !app.shutdown_flag {
		time.sleep(100 * time.Millisecond)
	}
}

app_shutdown :: proc(app: ^App) {
	fmt.println("")
	fmt.println("Shutting down...")
	
	// Set shutdown flag
	app.shutdown_flag = true
	app.running = false
	
	// Close listener socket to unblock accept (if not already closed by signal handler)
	net.close(app.listener.listen_socket)
	
	// Give threads time to notice
	time.sleep(100 * time.Millisecond)
	
	// Wait for threads
	fmt.println("Stopping threads...")
	
	if app.processor_thread != nil {
		thread.join(app.processor_thread)
		thread.destroy(app.processor_thread)
		app.processor_thread = nil
		fmt.println("  ✓ Processor stopped")
	}
	
	if app.router_thread != nil {
		thread.join(app.router_thread)
		thread.destroy(app.router_thread)
		app.router_thread = nil
		fmt.println("  ✓ Router stopped")
	}
	
	if app.listener_thread != nil {
		thread.join(app.listener_thread)
		thread.destroy(app.listener_thread)
		app.listener_thread = nil
		fmt.println("  ✓ Listener stopped")
	}
	
	print_final_stats(app)
	engine.registry_destroy(&app.client_registry)
}

print_final_stats :: proc(app: ^App) {
	fmt.println("")
	fmt.println("========================================")
	fmt.println("         Server Statistics")
	fmt.println("========================================")
	fmt.printfln("Orders in book: %d", core.book_order_count(&app.book))
	fmt.printfln("Best bid:       %d", core.book_best_bid(&app.book))
	fmt.printfln("Best ask:       %d", core.book_best_ask(&app.book))
	fmt.println("========================================")
}
