#!/bin/bash

set -e  # Exit on error

OUTPUT="THIRD_PARTY_NOTICES.txt"
TEMP_DIR="third_party_licenses"
GO_LICENSES="$HOME/go/bin/go-licenses"

# Check if go-licenses is installed
if [ ! -f "$GO_LICENSES" ]; then
    echo "go-licenses not found. Installing..."
    go install github.com/google/go-licenses@latest
fi

echo "Extracting license information from dependencies..."

# Clean up any existing temporary directory
rm -rf "$TEMP_DIR"

# Run go-licenses to extract all license files
# Ignore the local package itself to avoid AGPL "forbidden" error
$GO_LICENSES save ./cmd/server --save_path="$TEMP_DIR" --ignore gitea.jw6.us/james/calcard 2>&1 | grep -v "contains non-Go code"

# Create the output file header
cat > "$OUTPUT" << 'HEADER'
THIRD PARTY NOTICES
===================

This file contains the licenses for third-party libraries used in this project.

HEADER

# Function to add a license section
add_license() {
    local package_path="$1"
    local license_file="$2"

    echo "" >> "$OUTPUT"
    echo "================================================================================" >> "$OUTPUT"
    echo "Package: $package_path" >> "$OUTPUT"
    echo "================================================================================" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    cat "$license_file" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
}

# Find all LICENSE files and add them to the output
find "$TEMP_DIR" -type f -name "LICENSE*" | sort | while read license_file; do
    # Extract package path from file path
    package_path=$(echo "$license_file" | sed "s|^$TEMP_DIR/||" | sed 's|/LICENSE.*$||')
    add_license "$package_path" "$license_file"
done

# Clean up temporary directory
rm -rf "$TEMP_DIR"

echo "Created $OUTPUT with all third-party licenses"
echo "Cleaned up temporary files"
