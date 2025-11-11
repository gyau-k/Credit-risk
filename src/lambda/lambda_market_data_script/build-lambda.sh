#!/bin/bash

# Build script for market data Lambda
# Remove 'set -e' to allow better error capture
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
DIST_DIR="${SCRIPT_DIR}/dist"

echo "Building Lambda deployment package for market_data_poller..."

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf "${BUILD_DIR}" "${DIST_DIR}"
mkdir -p "${BUILD_DIR}" "${DIST_DIR}"

# Install dependencies
echo "Installing Python dependencies..."
echo "Python version: $(python3 --version)"
echo "Pip version: $(pip3 --version)"
echo ""
echo "Requirements file contents:"
cat "${SCRIPT_DIR}/requirements.txt"
echo ""

# Install with verbose error reporting
echo "Running pip install..."
if ! pip3 install -r "${SCRIPT_DIR}/requirements.txt" -t "${BUILD_DIR}" --no-cache-dir 2>&1; then
  echo " ERROR: pip install failed!"
  echo "Checking if requirements.txt exists:"
  ls -lh "${SCRIPT_DIR}/requirements.txt"
  echo ""
  echo "Current directory: $(pwd)"
  echo "Build directory: ${BUILD_DIR}"
  exit 1
fi

echo ""
echo " Dependencies installed successfully"
echo "Installed packages in build directory:"
ls -lh "${BUILD_DIR}" | head -15

# Copy Lambda function code
echo ""
echo "Copying Lambda function code..."
if [ ! -f "${SCRIPT_DIR}/lambda_function.py" ]; then
  echo " ERROR: lambda_function.py not found!"
  echo "Files in script directory:"
  ls -lh "${SCRIPT_DIR}"
  exit 1
fi

cp "${SCRIPT_DIR}/lambda_function.py" "${BUILD_DIR}/"

# Verify files
echo "Verifying build directory contents..."
if [ ! -f "${BUILD_DIR}/lambda_function.py" ]; then
  echo " ERROR: lambda_function.py not found in build directory after copy"
  exit 1
fi

echo " Lambda function copied successfully"
echo "Files in build directory:"
ls -lh "${BUILD_DIR}"

# Create deployment package
echo ""
echo "Creating deployment package..."
cd "${BUILD_DIR}"

if ! zip -r "${DIST_DIR}/market_data_poller.zip" . > /dev/null 2>&1; then
  echo " ERROR: Failed to create zip file"
  exit 1
fi

cd - > /dev/null

echo " Build complete!"
echo "  Location: ${DIST_DIR}/market_data_poller.zip"
echo "  Size: $(du -h ${DIST_DIR}/market_data_poller.zip 2>/dev/null | cut -f1 || echo 'N/A')"
echo ""

# Final verification
if [ -f "${DIST_DIR}/market_data_poller.zip" ]; then
  echo " Deployment package created successfully"
  exit 0
else
  echo " ERROR: Deployment package not found!"
  exit 1
fi