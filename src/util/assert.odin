package util

import "core:fmt"
import "base:runtime"

// =============================================================================
// Custom Assertions - Power of Ten Rule 5
// =============================================================================
// "The assertion density of the code should average to a minimum of two
//  assertions per function."
//
// Assertions must:
// - Be side-effect free
// - Be defined as Boolean tests
// - Trigger explicit recovery action on failure
// =============================================================================

// Assertion failure handler type
Assert_Handler :: #type proc(location: runtime.Source_Code_Location, message: string)

// Default handler - prints and returns (no panic in release)
@(private)
default_handler :: proc(loc: runtime.Source_Code_Location, message: string) {
	fmt.eprintf("[ASSERT FAILED] %s:%d in %s: %s\n",
		loc.file_path, loc.line, loc.procedure, message)
}

// Current handler (can be replaced for testing)
@(private)
g_assert_handler: Assert_Handler = default_handler

// Set custom assertion handler (for testing or custom logging)
set_assert_handler :: proc(handler: Assert_Handler) {
	g_assert_handler = handler
}

// =============================================================================
// Assertion Macros
// =============================================================================
// These return bool so callers can handle failures:
//
//   if !ensure(ptr != nil, "null pointer") {
//       return .Internal
//   }
// =============================================================================

// Basic assertion - returns false on failure
ensure :: proc(
	condition: bool,
	message: string = "assertion failed",
	loc := #caller_location,
) -> bool {
	if !condition {
		if g_assert_handler != nil {
			g_assert_handler(loc, message)
		}
		return false
	}
	return true
}

// Assert non-nil pointer
ensure_not_nil :: proc(
	ptr: rawptr,
	message: string = "unexpected nil pointer",
	loc := #caller_location,
) -> bool {
	if ptr == nil {
		if g_assert_handler != nil {
			g_assert_handler(loc, message)
		}
		return false
	}
	return true
}

// Assert value is within bounds [min, max]
ensure_bounds :: proc(
	value: $T,
	min_val: T,
	max_val: T,
	message: string = "value out of bounds",
	loc := #caller_location,
) -> bool where intrinsics.type_is_ordered(T) {
	if value < min_val || value > max_val {
		if g_assert_handler != nil {
			g_assert_handler(loc, message)
		}
		return false
	}
	return true
}

// Assert index is valid for array/slice
ensure_index :: proc(
	index: int,
	length: int,
	message: string = "index out of range",
	loc := #caller_location,
) -> bool {
	if index < 0 || index >= length {
		if g_assert_handler != nil {
			g_assert_handler(loc, message)
		}
		return false
	}
	return true
}

// Assert two values are equal
ensure_eq :: proc(
	a: $T,
	b: T,
	message: string = "values not equal",
	loc := #caller_location,
) -> bool where intrinsics.type_is_comparable(T) {
	if a != b {
		if g_assert_handler != nil {
			g_assert_handler(loc, message)
		}
		return false
	}
	return true
}

// =============================================================================
// Imports needed for type constraints
// =============================================================================

import "base:intrinsics"
