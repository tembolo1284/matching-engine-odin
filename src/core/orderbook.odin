package core

import "../types"
import "../memory"
import "../protocol"
import "../util"

// =============================================================================
// Order Book
// =============================================================================
// Central limit order book with price-time priority matching.
//
// Structure:
// - Bids: sorted descending (best bid = highest price)
// - Asks: sorted ascending (best ask = lowest price)
// - Price levels stored in fixed arrays (no dynamic allocation)
// - Orders within a level are FIFO (time priority)
//
// Rule 2: All loops bounded
// Rule 3: No dynamic allocation after init
// Rule 5: Assertions throughout
// =============================================================================

// Limits (Rule 2: bounded loops)
MAX_PRICE_LEVELS :: protocol.MAX_PRICE_LEVELS
MAX_ORDERS :: protocol.MAX_ORDERS_PER_BOOK

// Order pool type alias for convenience
Order_Pool :: memory.Pool(Order, MAX_ORDERS)

// =============================================================================
// Trade Output (result of matching)
// =============================================================================

Trade_Result :: struct {
	buy_user_id:    u32,
	buy_order_id:   u32,
	sell_user_id:   u32,
	sell_order_id:  u32,
	price:          u32,
	quantity:       u32,
}

// Callback type for trade notifications
Trade_Callback :: #type proc(trade: ^Trade_Result, user_data: rawptr)

// =============================================================================
// Order Book Structure
// =============================================================================

Order_Book :: struct {
	symbol:         protocol.Symbol,
	
	// Price levels - sorted arrays
	// Bids: index 0 = best (highest), sorted descending
	// Asks: index 0 = best (lowest), sorted ascending
	bid_levels:     [MAX_PRICE_LEVELS]Price_Level,
	ask_levels:     [MAX_PRICE_LEVELS]Price_Level,
	
	bid_count:      u32,   // Number of active bid levels
	ask_count:      u32,   // Number of active ask levels
	
	// Order pool - shared across all price levels
	orders:         Order_Pool,
	
	// Order ID to handle mapping (for cancel/modify)
	// Simple linear scan for now - can optimize with hash map later
	order_map:      [MAX_ORDERS]Order_Map_Entry,
	order_map_count: u32,
	
	// Server-assigned order ID counter
	next_server_id: u32,
	
	// Trade callback
	on_trade:       Trade_Callback,
	trade_user_data: rawptr,
}

// Order map entry for lookup by user_id + user_order_id
Order_Map_Entry :: struct {
	user_id:       u32,
	user_order_id: u32,
	handle:        memory.Handle,
	active:        bool,
}

// =============================================================================
// Order Book Initialization
// =============================================================================

// Initialize an order book for a symbol
book_init :: proc(book: ^Order_Book, symbol: protocol.Symbol) -> types.Error {
	// Rule 5: Validate input
	if !util.ensure(book != nil, "book is nil") {
		return .Internal
	}
	
	book.symbol = symbol
	book.bid_count = 0
	book.ask_count = 0
	book.order_map_count = 0
	book.next_server_id = 1
	book.on_trade = nil
	book.trade_user_data = nil
	
	// Initialize order pool (Rule 3: allocate at init)
	err := memory.pool_init(&book.orders)
	if err != .None {
		return err
	}
	
	// Initialize all price levels
	for i := 0; i < MAX_PRICE_LEVELS; i += 1 {
		level_init(&book.bid_levels[i], 0, .Buy)
		level_init(&book.ask_levels[i], 0, .Sell)
	}
	
	// Clear order map
	for i := 0; i < MAX_ORDERS; i += 1 {
		book.order_map[i] = Order_Map_Entry{}
	}
	
	return .None
}

// Set trade callback
book_set_trade_callback :: proc(
	book: ^Order_Book,
	callback: Trade_Callback,
	user_data: rawptr,
) {
	book.on_trade = callback
	book.trade_user_data = user_data
}

// =============================================================================
// Order Submission
// =============================================================================

// Submit a new order - matches aggressively then rests remainder
book_add_order :: proc(
	book: ^Order_Book,
	user_id: u32,
	user_order_id: u32,
	price: u32,
	quantity: u32,
	side: protocol.Side,
) -> (memory.Handle, types.Error) {
	// Rule 5: Validate inputs
	if !util.ensure(book != nil, "book is nil") {
		return memory.NULL_HANDLE, .Internal
	}
	if !util.ensure(quantity > 0, "quantity must be > 0") {
		return memory.NULL_HANDLE, .Order_Invalid_Quantity
	}
	if !util.ensure(price > 0, "price must be > 0") {
		return memory.NULL_HANDLE, .Order_Invalid_Price
	}
	
	// Check for duplicate order ID
	if find_order(book, user_id, user_order_id) != memory.NULL_HANDLE {
		return memory.NULL_HANDLE, .Order_Already_Exists
	}
	
	// Allocate order from pool
	handle, err := memory.pool_alloc(&book.orders)
	if err != .None {
		return memory.NULL_HANDLE, err
	}
	
	// Initialize order
	order := memory.pool_get(&book.orders, handle)
	server_id := book.next_server_id
	book.next_server_id += 1
	
	order_init(
		order,
		user_id,
		user_order_id,
		server_id,
		book.symbol,
		price,
		quantity,
		side,
		.Limit,
		server_id,  // Use server_id as timestamp for simplicity
	)
	
	// Add to order map
	map_err := add_to_order_map(book, user_id, user_order_id, handle)
	if map_err != .None {
		memory.pool_free(&book.orders, handle)
		return memory.NULL_HANDLE, map_err
	}
	
	// Try to match against opposite side
	remaining := match_order(book, order, handle)
	
	// If fully filled, clean up
	if remaining == 0 || order.status == .Filled {
		remove_from_order_map(book, user_id, user_order_id)
		memory.pool_free(&book.orders, handle)
		return memory.NULL_HANDLE, .None  // Fully matched, no resting order
	}
	
	// Rest the remaining quantity
	rest_err := rest_order(book, handle)
	if rest_err != .None {
		remove_from_order_map(book, user_id, user_order_id)
		memory.pool_free(&book.orders, handle)
		return memory.NULL_HANDLE, rest_err
	}
	
	return handle, .None
}

// =============================================================================
// Order Cancellation
// =============================================================================

// Cancel an order by user_id and user_order_id
book_cancel_order :: proc(
	book: ^Order_Book,
	user_id: u32,
	user_order_id: u32,
) -> types.Error {
	// Rule 5: Validate input
	if !util.ensure(book != nil, "book is nil") {
		return .Internal
	}
	
	// Find the order
	handle := find_order(book, user_id, user_order_id)
	if handle == memory.NULL_HANDLE {
		return .Order_Not_Found
	}
	
	order := memory.pool_get(&book.orders, handle)
	if order == nil {
		return .Order_Not_Found
	}
	
	// Remove from price level
	levels := book.bid_levels[:] if order.side == .Buy else book.ask_levels[:]
	level_count := book.bid_count if order.side == .Buy else book.ask_count
	
	// Find the price level
	for i: u32 = 0; i < level_count && i < MAX_PRICE_LEVELS; i += 1 {
		if levels[i].price == order.price {
			level_remove_order(&levels[i], handle, &book.orders)
			
			// Remove level if empty
			if level_is_empty(&levels[i]) {
				remove_price_level(book, order.side, i)
			}
			break
		}
	}
	
	// Mark as cancelled and clean up
	order_cancel(order)
	remove_from_order_map(book, user_id, user_order_id)
	memory.pool_free(&book.orders, handle)
	
	return .None
}

// =============================================================================
// Matching Engine Core
// =============================================================================

// Match an incoming order against the book
// Returns remaining quantity after matching
match_order :: proc(book: ^Order_Book, order: ^Order, handle: memory.Handle) -> u32 {
	// Determine which side to match against
	if order.side == .Buy {
		return match_against_asks(book, order, handle)
	} else {
		return match_against_bids(book, order, handle)
	}
}

// Match a buy order against asks (ascending price order)
match_against_asks :: proc(book: ^Order_Book, order: ^Order, handle: memory.Handle) -> u32 {
	iterations: u32 = 0
	max_iterations: u32 = MAX_PRICE_LEVELS * MAX_ORDERS_PER_LEVEL  // Rule 2
	
	for order.remaining > 0 && book.ask_count > 0 && iterations < max_iterations {
		iterations += 1
		
		// Best ask is at index 0 (lowest price)
		level := &book.ask_levels[0]
		
		// Check if prices cross (buy price >= ask price)
		if order.price < level.price {
			break  // No more matches possible
		}
		
		// Match against orders at this level
		match_at_level(book, order, handle, level)
		
		// Remove level if empty
		if level_is_empty(level) {
			remove_price_level(book, .Sell, 0)
		}
	}
	
	return order.remaining
}

// Match a sell order against bids (descending price order)
match_against_bids :: proc(book: ^Order_Book, order: ^Order, handle: memory.Handle) -> u32 {
	iterations: u32 = 0
	max_iterations: u32 = MAX_PRICE_LEVELS * MAX_ORDERS_PER_LEVEL  // Rule 2
	
	for order.remaining > 0 && book.bid_count > 0 && iterations < max_iterations {
		iterations += 1
		
		// Best bid is at index 0 (highest price)
		level := &book.bid_levels[0]
		
		// Check if prices cross (sell price <= bid price)
		if order.price > level.price {
			break  // No more matches possible
		}
		
		// Match against orders at this level
		match_at_level(book, order, handle, level)
		
		// Remove level if empty
		if level_is_empty(level) {
			remove_price_level(book, .Buy, 0)
		}
	}
	
	return order.remaining
}

// Match against orders at a single price level
match_at_level :: proc(
	book: ^Order_Book,
	aggressor: ^Order,
	aggressor_handle: memory.Handle,
	level: ^Price_Level,
) {
	iter := level_iter_begin(level)
	
	for level_iter_valid(&iter) && aggressor.remaining > 0 {
		resting_handle := level_iter_current(&iter)
		resting := memory.pool_get(&book.orders, resting_handle)
		
		if resting == nil {
			level_iter_advance(&iter, &book.orders)
			continue
		}
		
		// Calculate fill quantity
		fill_qty := min(aggressor.remaining, resting.remaining)
		
		// Execute the trade
		old_resting_qty := resting.remaining
		order_fill(aggressor, fill_qty)
		order_fill(resting, fill_qty)
		
		// Update level quantity
		level_update_qty(level, old_resting_qty, resting.remaining)
		
		// Report trade
		report_trade(book, aggressor, resting, level.price, fill_qty)
		
		// Move to next before potentially removing current
		level_iter_advance(&iter, &book.orders)
		
		// Remove filled resting order
		if resting.remaining == 0 {
			level_remove_order(level, resting_handle, &book.orders)
			remove_from_order_map(book, resting.user_id, resting.user_order_id)
			memory.pool_free(&book.orders, resting_handle)
		}
	}
}

// Report a trade via callback
report_trade :: proc(
	book: ^Order_Book,
	aggressor: ^Order,
	resting: ^Order,
	price: u32,
	quantity: u32,
) {
	if book.on_trade == nil {
		return
	}
	
	trade := Trade_Result{}
	
	// Determine buy/sell based on aggressor side
	if aggressor.side == .Buy {
		trade.buy_user_id = aggressor.user_id
		trade.buy_order_id = aggressor.user_order_id
		trade.sell_user_id = resting.user_id
		trade.sell_order_id = resting.user_order_id
	} else {
		trade.buy_user_id = resting.user_id
		trade.buy_order_id = resting.user_order_id
		trade.sell_user_id = aggressor.user_id
		trade.sell_order_id = aggressor.user_order_id
	}
	
	trade.price = price
	trade.quantity = quantity
	
	book.on_trade(&trade, book.trade_user_data)
}

// =============================================================================
// Resting Orders
// =============================================================================

// Add a partially filled or unfilled order to the book
rest_order :: proc(book: ^Order_Book, handle: memory.Handle) -> types.Error {
	order := memory.pool_get(&book.orders, handle)
	if order == nil {
		return .Pool_Invalid_Handle
	}
	
	// Find or create price level
	level_idx, err := find_or_create_level(book, order.price, order.side)
	if err != .None {
		return err
	}
	
	// Get the level
	level: ^Price_Level
	if order.side == .Buy {
		level = &book.bid_levels[level_idx]
	} else {
		level = &book.ask_levels[level_idx]
	}
	
	// Add order to level
	return level_add_order(level, handle, &book.orders)
}

// =============================================================================
// Price Level Management
// =============================================================================

// Find or create a price level, returns index
find_or_create_level :: proc(
	book: ^Order_Book,
	price: u32,
	side: protocol.Side,
) -> (u32, types.Error) {
	if side == .Buy {
		return find_or_create_bid_level(book, price)
	} else {
		return find_or_create_ask_level(book, price)
	}
}

// Find or create bid level (sorted descending by price)
find_or_create_bid_level :: proc(book: ^Order_Book, price: u32) -> (u32, types.Error) {
	// Find insertion point (maintain descending order)
	insert_idx: u32 = 0
	for i: u32 = 0; i < book.bid_count && i < MAX_PRICE_LEVELS; i += 1 {
		if book.bid_levels[i].price == price {
			return i, .None  // Level exists
		}
		if book.bid_levels[i].price > price {
			insert_idx = i + 1
		} else {
			break
		}
	}
	
	// Need to create new level
	if book.bid_count >= MAX_PRICE_LEVELS {
		return 0, .Book_Full
	}
	
	// Shift levels down to make room
	for i := book.bid_count; i > insert_idx; i -= 1 {
		book.bid_levels[i] = book.bid_levels[i - 1]
	}
	
	// Initialize new level
	level_init(&book.bid_levels[insert_idx], price, .Buy)
	book.bid_count += 1
	
	return insert_idx, .None
}

// Find or create ask level (sorted ascending by price)
find_or_create_ask_level :: proc(book: ^Order_Book, price: u32) -> (u32, types.Error) {
	// Find insertion point (maintain ascending order)
	insert_idx: u32 = 0
	for i: u32 = 0; i < book.ask_count && i < MAX_PRICE_LEVELS; i += 1 {
		if book.ask_levels[i].price == price {
			return i, .None  // Level exists
		}
		if book.ask_levels[i].price < price {
			insert_idx = i + 1
		} else {
			break
		}
	}
	
	// Need to create new level
	if book.ask_count >= MAX_PRICE_LEVELS {
		return 0, .Book_Full
	}
	
	// Shift levels down to make room
	for i := book.ask_count; i > insert_idx; i -= 1 {
		book.ask_levels[i] = book.ask_levels[i - 1]
	}
	
	// Initialize new level
	level_init(&book.ask_levels[insert_idx], price, .Sell)
	book.ask_count += 1
	
	return insert_idx, .None
}

// Remove an empty price level
remove_price_level :: proc(book: ^Order_Book, side: protocol.Side, idx: u32) {
	if side == .Buy {
		if idx >= book.bid_count {
			return
		}
		// Shift levels up
		for i := idx; i < book.bid_count - 1; i += 1 {
			book.bid_levels[i] = book.bid_levels[i + 1]
		}
		book.bid_count -= 1
	} else {
		if idx >= book.ask_count {
			return
		}
		// Shift levels up
		for i := idx; i < book.ask_count - 1; i += 1 {
			book.ask_levels[i] = book.ask_levels[i + 1]
		}
		book.ask_count -= 1
	}
}

// =============================================================================
// Order Map (lookup by user_id + user_order_id)
// =============================================================================

// Find order handle by user_id and user_order_id
find_order :: proc(book: ^Order_Book, user_id: u32, user_order_id: u32) -> memory.Handle {
	// Linear scan (Rule 2: bounded by MAX_ORDERS)
	for i: u32 = 0; i < book.order_map_count && i < MAX_ORDERS; i += 1 {
		entry := &book.order_map[i]
		if entry.active && entry.user_id == user_id && entry.user_order_id == user_order_id {
			return entry.handle
		}
	}
	return memory.NULL_HANDLE
}

// Add order to map
add_to_order_map :: proc(
	book: ^Order_Book,
	user_id: u32,
	user_order_id: u32,
	handle: memory.Handle,
) -> types.Error {
	// Find empty slot (reuse inactive entries)
	for i: u32 = 0; i < MAX_ORDERS; i += 1 {
		entry := &book.order_map[i]
		if !entry.active {
			entry.user_id = user_id
			entry.user_order_id = user_order_id
			entry.handle = handle
			entry.active = true
			if i >= book.order_map_count {
				book.order_map_count = i + 1
			}
			return .None
		}
	}
	return .Book_Full
}

// Remove order from map
remove_from_order_map :: proc(book: ^Order_Book, user_id: u32, user_order_id: u32) {
	for i: u32 = 0; i < book.order_map_count && i < MAX_ORDERS; i += 1 {
		entry := &book.order_map[i]
		if entry.active && entry.user_id == user_id && entry.user_order_id == user_order_id {
			entry.active = false
			return
		}
	}
}

// =============================================================================
// Book Queries
// =============================================================================

// Get best bid price (0 if no bids)
book_best_bid :: proc(book: ^Order_Book) -> u32 {
	if book.bid_count == 0 {
		return 0
	}
	return book.bid_levels[0].price
}

// Get best ask price (0 if no asks)
book_best_ask :: proc(book: ^Order_Book) -> u32 {
	if book.ask_count == 0 {
		return 0
	}
	return book.ask_levels[0].price
}

// Get total bid quantity at best price
book_best_bid_qty :: proc(book: ^Order_Book) -> u32 {
	if book.bid_count == 0 {
		return 0
	}
	return book.bid_levels[0].total_qty
}

// Get total ask quantity at best price
book_best_ask_qty :: proc(book: ^Order_Book) -> u32 {
	if book.ask_count == 0 {
		return 0
	}
	return book.ask_levels[0].total_qty
}

// Get spread (0 if no bids or asks)
book_spread :: proc(book: ^Order_Book) -> u32 {
	bid := book_best_bid(book)
	ask := book_best_ask(book)
	if bid == 0 || ask == 0 || ask <= bid {
		return 0
	}
	return ask - bid
}

// Get total order count in book
book_order_count :: proc(book: ^Order_Book) -> u32 {
	return memory.pool_count(&book.orders)
}
