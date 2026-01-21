#!/bin/bash
# Build wheel for PredictionMarketsAgent with environment-aware versioning
# and manylinux2014_aarch64 platform tag

set -e

echo "Building wheel for PredictionMarketsAgent..."

# Determine environment from git branch
ENV="dev"
if command -v git &> /dev/null; then
    BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    if [ "$BRANCH" = "main" ]; then
        ENV="prod"
    elif [ "$BRANCH" = "staging" ]; then
        ENV="test"
    elif [[ "$BRANCH" == dev/* ]]; then
        ENV="dev"
    fi
fi

echo "Building for environment: $ENV"

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf build/ *.egg-info

# Create environment-specific dist directory
ENV_DIST_DIR="dist/$ENV"
mkdir -p "$ENV_DIST_DIR"

# Build wheel with manylinux2014_aarch64 platform tag
echo "Building wheel with manylinux2014_aarch64 platform tag..."
python3 setup.py bdist_wheel --plat-name manylinux2014_aarch64

# Move wheel to environment-specific directory
WHEEL_FILE=$(ls dist/*.whl 2>/dev/null | head -1)
if [ -n "$WHEEL_FILE" ]; then
    WHEEL_NAME=$(basename "$WHEEL_FILE")
    mv "$WHEEL_FILE" "$ENV_DIST_DIR/"
    echo ""
    echo "✓ Wheel built successfully:"
    ls -lh "$ENV_DIST_DIR/$WHEEL_NAME"
    echo ""
    echo "Wheel location: $ENV_DIST_DIR/$WHEEL_NAME"
else
    echo "Error: Wheel file not found!"
    exit 1
fi

# Clean up build directory
echo ""
echo "Cleaning up build directory..."
rm -rf build/

echo ""
echo "Wheel filename pattern: package_name-version-py3-none-manylinux2014_aarch64"
