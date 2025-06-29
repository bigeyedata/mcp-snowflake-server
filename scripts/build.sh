#!/bin/bash
set -euo pipefail

# Build script for Snowflake MCP Server Docker image

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMAGE_NAME="snowflake-mcp-server"
IMAGE_TAG="latest"

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

# Get version information
VERSION="dev"
if command -v git >/dev/null 2>&1 && [ -d "$PROJECT_DIR/.git" ]; then
    BRANCH=$(git -C "$PROJECT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    COMMIT=$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")
    VERSION="${BRANCH}-${COMMIT}"
    
    # Check for uncommitted changes
    if ! git -C "$PROJECT_DIR" diff-index --quiet HEAD -- 2>/dev/null; then
        VERSION="${VERSION}-dirty"
        print_warn "Uncommitted changes detected, appending '-dirty' to version"
    fi
fi

print_info "Building Snowflake MCP Server Docker image..."
print_info "Version: $VERSION"
print_info "Project directory: $PROJECT_DIR"

# Change to project directory
cd "$PROJECT_DIR"

# Build the Docker image
print_info "Building Docker image..."

if docker build \
    --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    --build-arg VERSION="$VERSION" \
    --build-arg VCS_REF="$COMMIT" \
    --label "build.timestamp=$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    --label "version=$VERSION" \
    --label "git.branch=$BRANCH" \
    --label "git.commit=$COMMIT" \
    -t "${IMAGE_NAME}:${IMAGE_TAG}" \
    -t "${IMAGE_NAME}:${VERSION}" \
    .; then
    print_info "Build successful!"
else
    print_error "Build failed!"
    exit 1
fi

# Show image details
print_info "Tagged as:"
print_info "  - ${IMAGE_NAME}:${IMAGE_TAG}"
print_info "  - ${IMAGE_NAME}:${VERSION}"

# Display image size and creation time
print_info "Image details:"
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}" | grep "^${IMAGE_NAME}" | head -2

# Show build labels
print_info "Build labels:"
docker inspect "${IMAGE_NAME}:${IMAGE_TAG}" | jq '.[0].Config.Labels' 2>/dev/null || true

print_info "Build complete!"