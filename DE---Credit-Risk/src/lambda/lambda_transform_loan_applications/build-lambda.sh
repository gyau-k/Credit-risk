#!/bin/bash

# Build script for Loan Applications Transformation Lambda
# Creates deployment package using AWS SDK for Pandas Layer

set -e  # Exit on any error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
DIST_DIR="${SCRIPT_DIR}/dist"

echo "======================================"
echo "Building Loan Applications Transformation Lambda"
echo "======================================"
echo "Script directory: ${SCRIPT_DIR}"

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf "${BUILD_DIR}" "${DIST_DIR}"
mkdir -p "${BUILD_DIR}" "${DIST_DIR}"

# Layer includes: pandas, numpy, pyarrow, boto3, botocore

# Copy Lambda function code
echo "Copying Lambda function code..."
cp "${SCRIPT_DIR}/lambda_function.py" "${BUILD_DIR}/"

echo "Lambda code copied successfully"

# Create deployment package
echo "Creating deployment package..."
cd "${BUILD_DIR}"
zip -r "${DIST_DIR}/lambda_function.zip" . -q

if [ -f "${DIST_DIR}/lambda_function.zip" ]; then
    PACKAGE_SIZE=$(du -h "${DIST_DIR}/lambda_function.zip" | cut -f1)
    echo "======================================"
    echo "Build complete!"
    echo "Package: ${DIST_DIR}/lambda_function.zip"
    echo "Size: ${PACKAGE_SIZE}"
    echo "======================================"
else
    echo "ERROR: Failed to create deployment package!"
    exit 1
fi
