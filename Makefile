# ╔════════════════════════════════════════════════════════════════════════════╗
# ║             Odin Matching Engine - Power of Ten Compliant Build            ║
# ╚════════════════════════════════════════════════════════════════════════════╝

ODIN := odin
PROJECT := matching_engine
SRC_DIR := src
OUT_DIR := build

# Default ports
TCP_PORT := 1234
FIX_PORT := 1237

# Strictest possible flags (Rule 10: zero warnings)
VET_FLAGS := \
	-vet \
	-vet-unused \
	-vet-unused-variables \
	-vet-unused-imports \
	-vet-shadowing \
	-vet-using-stmt \
	-vet-using-param \
	-vet-style \
	-vet-semicolon

WARN_FLAGS := -warnings-as-errors

# Build configurations
DEBUG_FLAGS := -debug -o:none $(VET_FLAGS) $(WARN_FLAGS)
RELEASE_FLAGS := -o:speed -disable-assert $(VET_FLAGS) $(WARN_FLAGS)
AGGRESSIVE_FLAGS := -o:aggressive -disable-assert -no-bounds-check $(VET_FLAGS) $(WARN_FLAGS)

.PHONY: build build-release build-aggressive run run-release run-aggressive test clean help

# ════════════════════════════════════════════════════════════════════════════════
# Build
# ════════════════════════════════════════════════════════════════════════════════

build:
	@mkdir -p $(OUT_DIR)
	@echo "Building debug..."
	@$(ODIN) build $(SRC_DIR) $(DEBUG_FLAGS) -out:$(OUT_DIR)/$(PROJECT)
	@echo "✓ $(OUT_DIR)/$(PROJECT)"

build-release:
	@mkdir -p $(OUT_DIR)
	@echo "Building release..."
	@$(ODIN) build $(SRC_DIR) $(RELEASE_FLAGS) -out:$(OUT_DIR)/$(PROJECT)_release
	@echo "✓ $(OUT_DIR)/$(PROJECT)_release"

build-aggressive:
	@mkdir -p $(OUT_DIR)
	@echo "Building aggressive (no bounds checks)..."
	@$(ODIN) build $(SRC_DIR) $(AGGRESSIVE_FLAGS) -out:$(OUT_DIR)/$(PROJECT)_aggressive
	@echo "✓ $(OUT_DIR)/$(PROJECT)_aggressive"

# ════════════════════════════════════════════════════════════════════════════════
# Run
# ════════════════════════════════════════════════════════════════════════════════

run: build
	@echo ""
	@./$(OUT_DIR)/$(PROJECT) $(TCP_PORT)

run-release: build-release
	@echo ""
	@./$(OUT_DIR)/$(PROJECT)_release $(TCP_PORT)

run-aggressive: build-aggressive
	@echo ""
	@./$(OUT_DIR)/$(PROJECT)_aggressive $(TCP_PORT)

# ════════════════════════════════════════════════════════════════════════════════
# Test & Clean
# ════════════════════════════════════════════════════════════════════════════════

test:
	@echo "Running tests..."
	@$(ODIN) test tests $(VET_FLAGS) $(WARN_FLAGS)

clean:
	@rm -rf $(OUT_DIR)
	@echo "✓ Cleaned"

# ════════════════════════════════════════════════════════════════════════════════
# Help
# ════════════════════════════════════════════════════════════════════════════════

help:
	@echo ""
	@echo "Odin Matching Engine"
	@echo "===================="
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build            Build debug binary"
	@echo "  build-release    Build optimized binary"
	@echo "  build-aggressive Build fastest binary (no bounds checks)"
	@echo ""
	@echo "  run              Build and run (debug)"
	@echo "  run-release      Build and run (release)"
	@echo "  run-aggressive   Build and run (aggressive)"
	@echo ""
	@echo "  test             Run tests"
	@echo "  clean            Remove build artifacts"
	@echo "  help             Show this message"
	@echo ""
	@echo "Ports:"
	@echo "  TCP: $(TCP_PORT)"
	@echo "  FIX: $(FIX_PORT) (not yet implemented)"
	@echo ""
