#!/bin/bash

set -e  # Exit on error

OUTPUT="THIRD_PARTY_NOTICES.txt"

echo "Extracting license information from dependencies..."

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
    echo "License file: $(basename "$license_file")" >> "$OUTPUT"
    echo "================================================================================" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    cat "$license_file" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
}

# Enumerate modules with their on-disk directories, skipping the main module.
# This avoids go-licenses' dependency on module metadata for stdlib packages.
go list -m -f '{{if not .Main}}{{.Path}}|{{.Dir}}{{end}}' all | while IFS='|' read -r module_path module_dir; do
    [ -n "$module_path" ] || continue
    [ -d "$module_dir" ] || continue
    # Collect common license/notice files in the module directory.
    find "$module_dir" -maxdepth 2 -type f \( \
        -iname "LICENSE*" -o \
        -iname "COPYING*" -o \
        -iname "NOTICE*" \
    \) | sort | while read -r license_file; do
        add_license "$module_path" "$license_file"
    done
done

echo "Created $OUTPUT with all third-party licenses"
