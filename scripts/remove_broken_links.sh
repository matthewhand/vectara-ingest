#!/bin/bash

# File: remove_broken_links.sh

# Usage: ./remove_broken_links.sh path_to_config.yml

CONFIG_FILE="$1"
BROKEN_LINKS_FILE="$2"

# Check if configuration file and broken links file were provided
if [[ -z "$CONFIG_FILE" || -z "$BROKEN_LINKS_FILE" ]]; then
  echo "Usage: $0 path_to_config.yml broken_links.txt"
  exit 1
fi

# Check if the configuration file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Error: Configuration file '$CONFIG_FILE' not found."
  exit 1
fi

# Check if broken_links.txt exists
if [[ ! -f "$BROKEN_LINKS_FILE" ]]; then
  echo "Error: Broken links file '$BROKEN_LINKS_FILE' not found."
  exit 1
fi

# Create a backup of the original configuration file
cp "$CONFIG_FILE" "${CONFIG_FILE}.backup"

echo "Removing broken links from $CONFIG_FILE..."

# Initialize a temporary file
TEMP_FILE="config_temp.yml"
cp "$CONFIG_FILE" "$TEMP_FILE"

# Iterate over broken URLs and remove them from the temp file
while IFS= read -r BROKEN_URL; do
  # Escape special characters for sed
  ESCAPED_URL=$(printf '%s\n' "$BROKEN_URL" | sed 's/[]\/$*.^[]/\\&/g')

  # Remove the line containing the broken URL using a different delimiter '|'
  sed -i "\|\"$ESCAPED_URL\"|d" "$TEMP_FILE"
done < "$BROKEN_LINKS_FILE"

# Replace the original config with the updated one
mv "$TEMP_FILE" "$CONFIG_FILE"

echo "Broken links have been removed from $CONFIG_FILE."
echo "A backup of the original file is saved as ${CONFIG_FILE}.backup."

