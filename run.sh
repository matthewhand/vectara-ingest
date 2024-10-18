#!/bin/bash

# Usage: ./run.sh <config-file> <secrets-profile>
# Example: ./run.sh config/pg-rss.yaml default

# Exit immediately if a command exits with a non-zero status
set -e

# Function to display usage instructions
usage() {
  echo "Usage: $0 <config-file> <secrets-profile>"
  echo "Example: $0 config/pg-rss.yaml default"
  exit 1
}

# Check for required arguments
if [ $# -ne 2 ]; then
  echo "Error: Missing arguments."
  usage
fi

CONFIG_FILE="$1"
PROFILE="$2"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Configuration file '$CONFIG_FILE' does not exist."
  exit 2
fi

# Check if secrets.toml exists
if [ ! -f "secrets.toml" ]; then
  echo "Error: 'secrets.toml' file does not exist. Please create one following the README instructions."
  exit 3
fi

# Retrieve the crawler type from the config file
crawler_type=$(python3 -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['crawling']['crawler_type'])" | tr '[:upper:]' '[:lower:]')

# Create mount directory
MOUNT_DIR=~/tmp/mount
mkdir -p "$MOUNT_DIR"

# Copy secrets.toml and config file to mount directory
cp "secrets.toml" "$MOUNT_DIR"
cp "$CONFIG_FILE" "$MOUNT_DIR/"

# If crawler_type is gdrive, copy credentials.json
if [[ "$crawler_type" == "gdrive" ]]; then
  if [ -f "credentials.json" ]; then
    cp "credentials.json" "$MOUNT_DIR"
  else
    echo "Error: 'credentials.json' is required for 'gdrive' crawler type but does not exist."
    exit 4
  fi
fi

# Determine architecture
ARCH=$(uname -m)
if [[ "$ARCH" != "arm64" ]]; then
    ARCH="amd64"
fi

# Check for Buildx
has_buildx() {
  docker buildx version > /dev/null 2>&1
}

# Determine the build command based on the availability of Buildx
if has_buildx; then
  BUILD_CMD="buildx build --no-cache"
  echo "Building for $ARCH with Buildx and no cache"
else
  BUILD_CMD="build --no-cache"
  echo "Building for $ARCH without Buildx and no cache"
fi

# Determine if extra features should be installed
sum_tables=$(python3 -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['vectara'].get('summarize_tables', 'false'))" | tr '[:upper:]' '[:lower:]')
mask_pii=$(python3 -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['vectara'].get('mask_pii', 'false'))" | tr '[:upper:]' '[:lower:]')

# Set Docker tag based on extra features
if [[ "$sum_tables" == "true" || "$mask_pii" == "true" ]]; then
    echo "Building with extra features (summarize_tables or mask_pii)"
    TAG="vectara-ingest-full:latest"
    INSTALL_EXTRA="true"
else
    echo "Building without extra features"
    TAG="vectara-ingest:latest"
    INSTALL_EXTRA="false"
fi

# Build Docker image without cache
echo "Building Docker image..."
docker $BUILD_CMD --build-arg INSTALL_EXTRA="$INSTALL_EXTRA" --platform linux/"$ARCH" . --tag="$TAG"

echo "Docker build successful."

# Remove old container if it exists
if docker container inspect vingest > /dev/null 2>&1; then
    echo "Removing existing Docker container 'vingest'..."
    docker rm -f vingest
fi

# Determine config file name
config_file_name=$(basename "$CONFIG_FILE")

# Function to run Docker container with appropriate mounts based on crawler type
run_container() {
    local tag="$1"
    local config_file="$2"
    local profile="$3"
    local additional_mounts=()

    case "$crawler_type" in
        "folder")
            folder=$(python3 -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['folder_crawler']['path'])")
            if [ ! -d "$folder" ]; then
                echo "Error: Folder '$folder' does not exist."
                exit 5
            fi
            additional_mounts+=("-v" "$folder:/home/vectara/data")
            ;;
        "csv")
            csv_path=$(python3 -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['csv_crawler']['file_path'])")
            if [ ! -f "$csv_path" ]; then
                echo "Error: CSV file '$csv_path' does not exist."
                exit 6
            fi
            additional_mounts+=("-v" "$csv_path:/home/vectara/data/file.csv")
            ;;
        "bulkupload")
            json_path=$(python3 -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['bulkupload_crawler']['json_path'])")
            if [ ! -f "$json_path" ]; then
                echo "Error: JSON file '$json_path' does not exist."
                exit 7
            fi
            additional_mounts+=("-v" "$json_path:/home/vectara/data/file.json")
            ;;
    esac

    # Run Docker container
    echo "Running Docker container 'vingest'..."
    docker run -d \
        -v "$MOUNT_DIR:/home/vectara/env" \
        "${additional_mounts[@]}" \
        -e CONFIG="/home/vectara/env/$config_file_name" \
        -e PROFILE="$profile" \
        --name vingest \
        "$tag"
}

# Run the Docker container based on crawler type
run_container "$TAG" "$config_file_name" "$PROFILE"

echo "Success! Ingest job is running."
echo "You can try 'docker logs -f vingest' to see the progress."

