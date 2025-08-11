#!/bin/bash
# build.sh - Build script for vm-lttb-summarizer

# Set the output binary name
BINARY_NAME="downsampler"

# Clean previous builds
echo "Cleaning previous builds..."
#rm -f $BINARY_NAME*

# Get dependencies
echo "Getting dependencies..."
go mod init downsampler.go 2>/dev/null || true
go mod tidy

# Build for current platform
echo "Building for current platform..."
go build -ldflags="-s -w" -o $BINARY_NAME downsampler.go

# Optional: Build for multiple platforms
echo "Building for multiple platforms..."

# Linux AMD64
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" \
  -o ${BINARY_NAME}-linux-amd64 downsampler.go

# Windows AMD64
GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" \
  -o ${BINARY_NAME}-windows-amd64.exe downsampler.go

# macOS AMD64
GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" \
  -o ${BINARY_NAME}-darwin-amd64 downsampler.go

# macOS ARM64 (M1/M2)
GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" \
  -o ${BINARY_NAME}-darwin-arm64 downsampler.go

echo "Build complete! Binaries created:"
ls -lh ${BINARY_NAME}*
