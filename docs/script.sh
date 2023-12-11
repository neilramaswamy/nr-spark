#!/bin/bash

# Set the directory path
directory="."

# Navigate to the directory
cd "$directory" || exit

# Rename files with the specified pattern
for file in ss-manual-ss-manual-*.md; do
    # Extract the "<whatever>" part from the filename
    base_name=$(basename "$file" .md | sed 's/ss-manual-ss-manual-//')
    
    # Rename the file to the new format
    new_name="ss-manual-${base_name}.md"
    
    # Perform the rename
    mv "$file" "$new_name"
    
    echo "Renamed: $file to $new_name"
done

