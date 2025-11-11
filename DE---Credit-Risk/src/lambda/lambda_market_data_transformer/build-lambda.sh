#!/bin/bash

# Build script for market data transformer Lambda
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
DIST_DIR="${SCRIPT_DIR}/dist"
LAMBDA_NAME="market_data_transformer"

echo "Building Lambda deployment package for ${LAMBDA_NAME}..."

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf "${BUILD_DIR}" "${DIST_DIR}"
mkdir -p "${BUILD_DIR}" "${DIST_DIR}"

# Install dependencies
echo "Installing Python dependencies..."
pip3 install -r "${SCRIPT_DIR}/requirements.txt" -t "${BUILD_DIR}" --quiet

# Copy Lambda function code
echo "Copying Lambda function code..."
cp "${SCRIPT_DIR}/lambda_function.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/config.py" "${BUILD_DIR}/"

# Create ZIP file
echo "Creating deployment package..."
cd "${BUILD_DIR}"
zip -r "${DIST_DIR}/${LAMBDA_NAME}.zip" . -q

# Get ZIP file size
ZIP_SIZE=$(du -h "${DIST_DIR}/${LAMBDA_NAME}.zip" | cut -f1)

echo "  Build complete!"
echo "  Location: ${DIST_DIR}/${LAMBDA_NAME}.zip"
echo "  Size: ${ZIP_SIZE}"
echo ""