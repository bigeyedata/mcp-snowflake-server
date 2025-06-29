#!/bin/bash
set -euo pipefail

# Script to run Snowflake MCP Server locally for testing/debugging

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMAGE_NAME="snowflake-mcp-server"
IMAGE_TAG="latest"
CONTAINER_NAME="snowflake-mcp-local"

# Default values
INTERACTIVE=false
ENV_FILE=".env"
DEBUG_MODE=false
ALLOW_WRITE=false
PREFETCH=false

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_debug() {
    if [ "$DEBUG_MODE" = true ]; then
        echo -e "${BLUE}[DEBUG]${NC} $1"
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --interactive, -i     Run in interactive mode with bash shell"
    echo "  --env FILE           Use specific .env file (default: .env)"
    echo "  --debug              Enable debug output"
    echo "  --allow-write        Allow write operations to Snowflake"
    echo "  --prefetch           Prefetch table descriptions on startup"
    echo "  --help, -h           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run with default .env file"
    echo "  $0 --interactive      # Run interactive bash session"
    echo "  $0 --env prod.env     # Run with specific env file"
    echo "  $0 --allow-write      # Enable write operations"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --interactive|-i)
            INTERACTIVE=true
            shift
            ;;
        --env)
            ENV_FILE="$2"
            shift 2
            ;;
        --debug)
            DEBUG_MODE=true
            shift
            ;;
        --allow-write)
            ALLOW_WRITE=true
            shift
            ;;
        --prefetch)
            PREFETCH=true
            shift
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if image exists
if ! docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" >/dev/null 2>&1; then
    print_error "Image ${IMAGE_NAME}:${IMAGE_TAG} not found"
    print_info "Building image..."
    "${SCRIPT_DIR}/build.sh"
fi

# Change to project directory
cd "$PROJECT_DIR"

# Prepare Docker run command
DOCKER_CMD="docker run --rm"
DOCKER_CMD+=" --name ${CONTAINER_NAME}"

# Add environment file if it exists
if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
    print_info "Using environment file: $ENV_FILE"
    DOCKER_CMD+=" --env-file $ENV_FILE"
elif [ -n "$ENV_FILE" ]; then
    print_warn "Environment file not found: $ENV_FILE"
fi

# Add interactive flags
if [ "$INTERACTIVE" = true ]; then
    DOCKER_CMD+=" -it"
else
    DOCKER_CMD+=" -i"
fi

# Add environment variables
DOCKER_CMD+=" -e ALLOW_WRITE=$ALLOW_WRITE"

if [ "$DEBUG_MODE" = true ]; then
    DOCKER_CMD+=" -e MCP_LOG_LEVEL=DEBUG"
fi

# Mount logs directory
DOCKER_CMD+=" -v ${PROJECT_DIR}/logs:/app/logs"

# Mount config directory
DOCKER_CMD+=" -v ${HOME}/.snowflake-mcp:/home/mcp/.snowflake-mcp"

# Add the image
DOCKER_CMD+=" ${IMAGE_NAME}:${IMAGE_TAG}"

# Add command based on mode
if [ "$INTERACTIVE" = true ]; then
    DOCKER_CMD+=" /bin/bash"
    print_info "Starting interactive shell in container..."
    print_info "You can run 'python -m mcp_snowflake_server' to start the MCP server"
else
    # Add MCP server arguments
    if [ "$ALLOW_WRITE" = true ]; then
        DOCKER_CMD+=" --allow_write"
    fi
    
    if [ "$PREFETCH" = true ]; then
        DOCKER_CMD+=" --prefetch"
    fi
    
    print_info "Starting Snowflake MCP Server..."
    print_info "Press Ctrl+C to stop"
fi

# Show the command in debug mode
print_debug "Running command: $DOCKER_CMD"

# Function to cleanup on exit
cleanup() {
    print_info "Stopping container..."
    docker stop "${CONTAINER_NAME}" 2>/dev/null || true
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Run the container
eval $DOCKER_CMD

# Exit code from container
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    print_error "Container exited with code: $EXIT_CODE"
    exit $EXIT_CODE
fi

print_info "Container stopped successfully"