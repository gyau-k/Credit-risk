#!/bin/bash

# Build script for Lambda deployment package
# This script creates a ZIP file with Lambda code and dependencies

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BUILD_DIR="${SCRIPT_DIR}/build"
DIST_DIR="${SCRIPT_DIR}/dist"
LAMBDA_NAME="api_poller_lambda"

echo "Building Lambda deployment package for ${LAMBDA_NAME}..."

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf "${BUILD_DIR}"
rm -rf "${DIST_DIR}"

# Create directories
mkdir -p "${BUILD_DIR}"
mkdir -p "${DIST_DIR}"

# Install dependencies
echo "Installing Python dependencies..."
pip3 install -r "${SCRIPT_DIR}/requirements.txt" -t "${BUILD_DIR}" --quiet

# Copy Lambda function code and helper modules
echo "Copying Lambda function code..."
cp "${SCRIPT_DIR}/${LAMBDA_NAME}.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/config.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/api_client.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/validator.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/s3_handler.py" "${BUILD_DIR}/"

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
