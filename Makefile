# DEPRECATED: Simple targets in this Makefile are being phased out in favor of mise tasks.
# Please use `mise <task>` instead. Run `mise tasks` to see all available tasks.
# Proto compilation targets will remain in this Makefile as they benefit from Make's dependency tracking.

# Directory containing .proto files
PROTO_DIR := protos

# List of .proto files
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

# Generate output file paths
GO_OUT := $(patsubst $(PROTO_DIR)/%.proto,spark/proto/%/%.pb.go,$(PROTO_FILES))

# Rule to compile .proto files to Go
spark/proto/%/%.pb.go: $(PROTO_DIR)/%.proto
	@echo "Compiling $< to $@"
	@mkdir -p $(dir $@)
	protoc --go_out=$(dir $@) \
		--go_opt=paths=source_relative \
		--proto_path=$(PROTO_DIR) \
		--go-grpc_out=$(dir $@) \
		--go-grpc_opt=paths=source_relative \
		--validate_out="lang=go,paths=source_relative:$(dir $@)" \
		$<

# Default target
all: $(GO_OUT)

# Clean target
clean:
	rm -rf spark/proto/*/*.pb.go
	rm -rf spark/proto/*/*.pb.validate.go

ent:
	@echo "DEPRECATED: Use 'mise gen-ent' instead"
	@cd spark && go generate ./so/ent/...
	@echo "\n!!!!\nEnts generated. Remember to add migration changes with atlas! See README.md for more info.\n!!!!\n"

ssp:
	@echo "DEPRECATED: Use 'mise gen-ssp' instead"
	@cd spark && go generate ./testing/wallet/ssp_api/...
