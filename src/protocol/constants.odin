package protocol

// =============================================================================
// Protocol Constants
// =============================================================================
// Binary protocol specification for TCP transport
// Wire-compatible with C/Zig/Rust matching engine implementations
//
// Wire format:
//   Byte 0:     Magic (0x4D = 'M')
//   Byte 1:     Message type (ASCII char)
//   Byte 2+:    Payload (type-specific, network byte order)
//
// All multi-byte integers are BIG-ENDIAN (network byte order)
// =============================================================================

// Protocol identification
MAGIC :: u8(0x4D)             // 'M' for Matching engine

// Header: magic(1) + type(1)
HEADER_SIZE :: 2

// Symbol length (null-padded)
MAX_SYMBOL_LENGTH :: 8

// =============================================================================
// Message Type Bytes (ASCII)
// =============================================================================

MSG_NEW_ORDER    :: u8('N')   // 0x4E - New order
MSG_CANCEL       :: u8('C')   // 0x43 - Cancel order
MSG_FLUSH        :: u8('F')   // 0x46 - Flush/sync
MSG_ACK          :: u8('A')   // 0x41 - Order accepted
MSG_CANCEL_ACK   :: u8('X')   // 0x58 - Cancel confirmed
MSG_TRADE        :: u8('T')   // 0x54 - Trade/execution
MSG_TOP_OF_BOOK  :: u8('B')   // 0x42 - Best bid/ask update
MSG_REJECT       :: u8('R')   // 0x52 - Order rejected

// =============================================================================
// Wire Sizes (total including 2-byte header)
// =============================================================================

// New Order: magic(1) + type(1) + user_id(4) + symbol(8) + price(4) + qty(4) + side(1) + order_id(4) = 27
NEW_ORDER_WIRE_SIZE :: 27

// Cancel: magic(1) + type(1) + user_id(4) + order_id(4) = 10 (no symbol - looked up server-side)
CANCEL_WIRE_SIZE :: 10

// Flush: magic(1) + type(1) = 2
FLUSH_WIRE_SIZE :: 2

// Ack: magic(1) + type(1) + symbol(8) + user_id(4) + order_id(4) = 18
ACK_WIRE_SIZE :: 18

// Cancel Ack: same as Ack = 18
CANCEL_ACK_WIRE_SIZE :: 18

// Trade: magic(1) + type(1) + symbol(8) + buy_uid(4) + buy_oid(4) + sell_uid(4) + sell_oid(4) + price(4) + qty(4) = 34
TRADE_WIRE_SIZE :: 34

// Top of Book: magic(1) + type(1) + symbol(8) + side(1) + price(4) + qty(4) + pad(1) = 20
TOP_OF_BOOK_WIRE_SIZE :: 20

// Reject: magic(1) + type(1) + symbol(8) + user_id(4) + order_id(4) + reason(1) = 19
REJECT_WIRE_SIZE :: 19

// Max message size for buffer allocation
MAX_MESSAGE_SIZE :: TRADE_WIRE_SIZE  // Largest message

// =============================================================================
// Connection Limits
// =============================================================================

MAX_CONNECTIONS :: 1024
READ_BUFFER_SIZE :: 4096
WRITE_BUFFER_SIZE :: 4096

// Timing (nanoseconds)
HEARTBEAT_INTERVAL_NS :: 1_000_000_000      // 1 second
CONNECTION_TIMEOUT_NS :: 30_000_000_000     // 30 seconds

// =============================================================================
// Orderbook Limits (Rule 2: all loops must be bounded)
// =============================================================================

MAX_ORDERS_PER_BOOK :: 1_000_000
MAX_PRICE_LEVELS :: 10_000
MAX_ORDERS_PER_LEVEL :: 10_000
MAX_SYMBOLS :: 256

// Order pool sizing
ORDER_POOL_SIZE :: MAX_ORDERS_PER_BOOK
