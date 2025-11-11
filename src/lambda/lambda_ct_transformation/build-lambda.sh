#!/bin/bash

# Build script for Customer Transaction Transformation Lambda
# Creates deployment package with all dependencies bundled together

set -e  # Exit on any error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
DIST_DIR="${SCRIPT_DIR}/dist"

echo "======================================"
echo "Building Customer Transaction Transformation Lambda"
echo "======================================"
echo "Script directory: ${SCRIPT_DIR}"

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf "${BUILD_DIR}" "${DIST_DIR}"
mkdir -p "${BUILD_DIR}" "${DIST_DIR}"

# Note: No pip install needed - all dependencies provided by AWS SDK for Pandas Layer
# Layer includes: pandas, numpy, pyarrow, boto3, botocore

# Copy Lambda function code
echo "Copying Lambda function code..."
cp "${SCRIPT_DIR}/transformation_lambda.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/config.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/s3_handler.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/validator.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/masker.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/transformer.py" "${BUILD_DIR}/"
cp "${SCRIPT_DIR}/delta_writer.py" "${BUILD_DIR}/"

echo "Lambda code copied successfully"

# Create deployment package
echo "Creating deployment package..."
cd "${BUILD_DIR}"
zip -r "${DIST_DIR}/transformation_lambda.zip" . -q

if [ -f "${DIST_DIR}/transformation_lambda.zip" ]; then
    PACKAGE_SIZE=$(du -h "${DIST_DIR}/transformation_lambda.zip" | cut -f1)
    echo "======================================"
    echo "Build complete!"
    echo "Package: ${DIST_DIR}/transformation_lambda.zip"
    echo "Size: ${PACKAGE_SIZE}"
    echo "======================================"
else
    echo "ERROR: Failed to create deployment package!"
    exit 1
fi
