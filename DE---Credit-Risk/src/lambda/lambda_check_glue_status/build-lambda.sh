#!/bin/bash

# Build script for Glue status checker Lambda function
# This script packages the Lambda function and its dependencies

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BUILD_DIR="${SCRIPT_DIR}/build"
DIST_DIR="${SCRIPT_DIR}/dist"
LAMBDA_NAME="lambda_check_glue_status"

echo "Building Lambda deployment package for ${LAMBDA_NAME}..."

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf "${BUILD_DIR}"
rm -rf "${DIST_DIR}"

# Create directories
mkdir -p "${BUILD_DIR}"
mkdir -p "${DIST_DIR}"

# Install dependencies
if [ -f "${SCRIPT_DIR}/requirements.txt" ]; then
    echo "Installing Python dependencies..."
    pip3 install -r "${SCRIPT_DIR}/requirements.txt" -t "${BUILD_DIR}" --quiet
fi

# Copy Lambda function
echo "Copying Lambda function code..."
cp "${SCRIPT_DIR}/lambda_function.py" "${BUILD_DIR}/"

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
