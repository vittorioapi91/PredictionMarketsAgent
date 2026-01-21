#!/bin/bash
# Build wheel for PredictionMarketsAgent with environment-aware versioning
# and manylinux2014_aarch64 platform tag

set -e

echo "Building wheel for PredictionMarketsAgent..."

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf build/ dist/ *.egg-info

# Build wheel with manylinux2014_aarch64 platform tag
echo "Building wheel with manylinux2014_aarch64 platform tag..."
python3 setup.py bdist_wheel --plat-name manylinux2014_aarch64

# Show the created wheel
echo ""
echo "✓ Wheel built successfully:"
ls -lh dist/*.whl

echo ""
echo "Wheel filename pattern: package_name-version-py3-none-manylinux2014_aarch64"
