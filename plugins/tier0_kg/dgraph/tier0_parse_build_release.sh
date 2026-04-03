#!/bin/bash

# Usage: ./deploy_dgraph.sh <prefix_version> <source_version>
# Example: ./deploy_dgraph.sh vN 20260310 > deploy_dgraph.log

# sudo apt install keychain
# Add to ~/.bashrc:
# eval "$(keychain --eval --agents ssh id_rsa)"

PREFIX_VERSION="${1:?Usage: $0 <prefix_version> <source_version>}"
SOURCE_VERSION="${2:?Usage: $0 <prefix_version> <source_version>}"

SFTP_HOST="team-expander-everaldo@sftp.transltr.io"
SFTP_CONTROL="/tmp/ssh_mux_$$"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# ---------------------------------------------------------------------------
# Step 0: Activate Python virtual environment
# ---------------------------------------------------------------------------
log "[0/7] Activating Python virtual environment..."
. /home/erodolpho/dgraph/.venv/bin/activate

# ---------------------------------------------------------------------------
# Pre-flight check 1: abort if local output directory already exists
# ---------------------------------------------------------------------------
if [ -d "/data/dgraph/$SOURCE_VERSION" ]; then
    log "WARNING: Local directory /data/dgraph/$SOURCE_VERSION already exists. Aborting to prevent overwrite."
    exit 1
fi

# ---------------------------------------------------------------------------
# Pre-flight check 2: abort if remote SFTP directory already exists
# Opens a master SSH connection (passphrase entered once, reused for uploads)
# ---------------------------------------------------------------------------
log "Pre-flight: Connecting to SFTP host to check for existing remote directory..."
ssh -o ControlMaster=yes \
    -o ControlPath="$SFTP_CONTROL" \
    -o ControlPersist=yes \
    "$SFTP_HOST" "exit"

if echo "ls team-dgraph/$SOURCE_VERSION" | sftp \
    -o ControlMaster=no \
    -o ControlPath="$SFTP_CONTROL" \
    -b /dev/stdin "$SFTP_HOST" > /dev/null 2>&1; then
    log "ERROR: Remote directory ~/team-dgraph/$SOURCE_VERSION already exists on $SFTP_HOST. Aborting to prevent overwrite."
    ssh -o ControlPath="$SFTP_CONTROL" -O exit "$SFTP_HOST" 2>/dev/null
    exit 1
fi

log "Starting deployment: PREFIX_VERSION=$PREFIX_VERSION, SOURCE_VERSION=$SOURCE_VERSION"

# ---------------------------------------------------------------------------
# Step 1: Stop Dgraph services
# ---------------------------------------------------------------------------
log "[1/7] Stopping Dgraph services..."
sudo systemctl stop dgraph-alpha.service
sudo systemctl stop dgraph-zero.service

# ---------------------------------------------------------------------------
# Step 2: Clean up and prepare data directories
# ---------------------------------------------------------------------------
log "[2/7] Cleaning up temporary and bulk directories..."
sudo rm -rf /data/dgraph/tmp
# sudo rm -rf /data/dgraph/bulk
sudo mkdir -p /data/dgraph/$SOURCE_VERSION /data/dgraph/tmp
sudo chmod 777 /data/dgraph/$SOURCE_VERSION /data/dgraph/tmp

# ---------------------------------------------------------------------------
# Step 3: Prepare Dgraph alpha data directory
# ---------------------------------------------------------------------------
log "[3/7] Preparing Dgraph alpha data directory at /opt/dgraph/data..."
sudo rm -rf /opt/dgraph/data
sudo mkdir /opt/dgraph/data
sudo chown -R dgraph:dgraph /opt/dgraph/data
sudo chmod -R 755 /opt/dgraph/data

# ---------------------------------------------------------------------------
# Step 4: Start Dgraph Zero and run bulk loader
# Streams parsed MongoDB data directly into the Dgraph bulk loader via stdin
# ---------------------------------------------------------------------------
log "[4/7] Starting Dgraph Zero service..."
sudo systemctl start dgraph-zero.service
log "[4/7] Waiting 30 seconds for Dgraph Zero to initialize..."
sleep 30

log "[4/7] Running parser and bulk loader (prefix=$PREFIX_VERSION, source=$SOURCE_VERSION)..."
python parser_mongodb.py \
  --mongo_uri "mongodb://su11:27017" \
  --db_name "dogpark_src" \
  --nodes_collection "tier0_kg_nodes" \
  --edges_collection "tier0_kg_edges" \
  --batch_size 7000  \
  --max_items 1000 \
  --prefix_version "$PREFIX_VERSION" \
  --source_version "$SOURCE_VERSION" \
  --schema_path "schema.dgraph" \
  --output_format json \
  --uid_mode blank \
  --schema_metadata_mapping_source https://kgx-storage.rtx.ai/releases/translator_kg/latest/graph-metadata.json | sudo /opt/dgraph/dgraph_v25_0_0 bulk \
  -f /dev/stdin \
  -s /home/erodolpho/dgraph/schema_v9.8/schema.dgraph.$PREFIX_VERSION \
  --out /data/dgraph/$SOURCE_VERSION \
  --tmp /data/dgraph/tmp \
  --format=json \
  --zero localhost:5080 \
  --map_shards=64 \
  --reduce_shards=32 \
  --num_go_routines=8
log "[4/7] Bulk loading complete."

# ---------------------------------------------------------------------------
# Step 5: Link bulk-loaded postings directory to Dgraph alpha data directory
# ---------------------------------------------------------------------------
log "[5/7] Linking bulk output to Dgraph alpha data directory..."
sudo chown -R dgraph:dgraph /data/dgraph/
sudo chmod -R 755 /data/dgraph/
sudo ln -s /data/dgraph/$SOURCE_VERSION/0/p /opt/dgraph/data/p

# ---------------------------------------------------------------------------
# Step 6: Start Dgraph Alpha service
# ---------------------------------------------------------------------------
log "[6/7] Starting Dgraph Alpha service..."
sudo systemctl start dgraph-alpha.service
log "[6/7] Dgraph Alpha started."

# ---------------------------------------------------------------------------
# Step 7: Upload bulk-loaded postings directory to SFTP
# Reuses the SSH master connection opened during pre-flight checks.
# ---------------------------------------------------------------------------
log "[7/7] Uploading /data/dgraph/$SOURCE_VERSION/0/p to $SFTP_HOST..."
scp -Cr /data/dgraph/$SOURCE_VERSION/0/p team-expander-everaldo@sftp.transltr.io:~/team-dgraph/$SOURCE_VERSION/
log "[7/7] Upload complete. Closing SSH master connection..."
