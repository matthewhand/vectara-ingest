#!/bin/bash

# File: check_rss_feeds.sh

# Usage: ./check_rss_feeds.sh path_to_config.yml

CONFIG_FILE="$1"

# Check if a configuration file was provided
if [[ -z "$CONFIG_FILE" ]]; then
  echo "Usage: $0 path_to_config.yml"
  exit 1
fi

# Check if the configuration file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Error: File '$CONFIG_FILE' not found."
  exit 1
fi

echo "Extracting URLs from $CONFIG_FILE..."

# Extract URLs from the rss_pages array in the YAML config using awk
URLS=$(awk '/rss_pages:/,/\]/' "$CONFIG_FILE" | grep -o 'https://[^", ]*')

if [[ -z "$URLS" ]]; then
  echo "No URLs found in rss_pages."
  exit 1
fi

# Initialize output files
WORKING_LINKS="working_links.txt"
BROKEN_LINKS="broken_links.txt"

# Clear or create the output files
> "$WORKING_LINKS"
> "$BROKEN_LINKS"

echo "Testing URLs. This may take a while..."

# Loop through each URL and test it with curl
for URL in $URLS; do
  # Trim any surrounding whitespace
  URL=$(echo "$URL" | xargs)
  
  # Skip empty lines
  if [[ -z "$URL" ]]; then
    continue
  fi

  echo -n "Checking $URL ... "

  # Perform a HEAD request with redirects (-L) and capture the final HTTP status code
  HTTP_STATUS=$(curl -o /dev/null -s -w "%{http_code}" -I -L "$URL")

  # Check if the status code is between 200 and 399 (inclusive)
  if [[ "$HTTP_STATUS" -ge 200 && "$HTTP_STATUS" -lt 400 ]]; then
    echo "OK (Status: $HTTP_STATUS)"
    echo "$URL" >> "$WORKING_LINKS"
  else
    echo "FAILED (Status: $HTTP_STATUS)"
    echo "$URL" >> "$BROKEN_LINKS"
  fi

  # Optional: Add a short delay to be polite to servers
  sleep 0.1
done

echo "Testing complete."
echo "Working links saved to $WORKING_LINKS"
echo "Broken links saved to $BROKEN_LINKS"

