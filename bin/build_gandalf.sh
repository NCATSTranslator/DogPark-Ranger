#!/usr/bin/env bash
# =============================================================================
# build_gandalf.sh
#
# Builds the Gandalf graph from the tier0 KG collections in MongoDB, compacts
# it, points the "latest" symlink at it, restarts the local Gandalf service,
# and uploads the archive to AWS S3. Paths are keyed on a date stamp (MM_DD_YYYY).
#
# Normally started by the hub's GandalfBuilder (hub/databuild/gandalf_builder.py)
# as the post-merge step of a Gandalf build config. The builder passes every
# setting below as an environment variable and streams this script's output
# into the hub's build log. Running it by hand works too: export the same
# variables first (the four GANDALF_*_COLLECTION / MONGO ones are required).
#
# Required:
#   GANDALF_MONGO_URI          e.g. mongodb://localhost:27017
#   GANDALF_MONGO_DB           database that holds the two collections below
#   GANDALF_NODES_COLLECTION   e.g. tier0_kg_nodes
#   GANDALF_EDGES_COLLECTION   e.g. tier0_kg_edges
#
# Optional (the defaults reproduce the previous hand-run script):
#   GANDALF_ROOT               /data/dogpark/gandalf   processed/, compacted/ and tmp/ live under it
#   GANDALF_BUILD_MONGO_BIN    gandalf-build-mongo     use a full path into Gandalf's .venv if it is not on PATH
#   GANDALF_SERVICE            gandalf.service         set it to an empty string to skip the restart
#   GANDALF_S3_BUCKET          unset = skip the upload (a warning is printed)
#   GANDALF_S3_PREFIX          key prefix inside the bucket
#   GANDALF_S3_REGION          passed to `aws --region`
#   GANDALF_S3_PROFILE         passed to `aws --profile`
#   GANDALF_RELEASE            upstream KGX release the graph is built from (printed, nothing more)
#   GANDALF_BUILD_DATE         MM_DD_YYYY stamp for paths and the archive name (default: today)
#
# AWS credentials are not handled here: `aws` uses whatever the user running
# this script already has (instance role, ~/.aws, or GANDALF_S3_PROFILE).
# Needs bash 3.2 or newer, so it also runs on a stock macOS shell.
# =============================================================================

set -euo pipefail

trap 'rc=$?; echo "ERROR: build_gandalf.sh failed at line ${LINENO} (exit ${rc})" >&2' ERR

: "${GANDALF_MONGO_URI:?GANDALF_MONGO_URI is required}"
: "${GANDALF_MONGO_DB:?GANDALF_MONGO_DB is required}"
: "${GANDALF_NODES_COLLECTION:?GANDALF_NODES_COLLECTION is required}"
: "${GANDALF_EDGES_COLLECTION:?GANDALF_EDGES_COLLECTION is required}"

# -----------------------------------------------------------------------------
# Settings and paths (same layout as the previous hand-run script)
# -----------------------------------------------------------------------------
DATE="${GANDALF_BUILD_DATE:-$(date +"%m_%d_%Y")}"
ROOT="${GANDALF_ROOT:-/data/dogpark/gandalf}"
BUILD_BIN="${GANDALF_BUILD_MONGO_BIN:-gandalf-build-mongo}"
# "-" rather than ":-" on purpose: an explicitly empty value skips the restart
SERVICE="${GANDALF_SERVICE-gandalf.service}"
S3_BUCKET="${GANDALF_S3_BUCKET:-}"
S3_PREFIX="${GANDALF_S3_PREFIX:-}"

BUILD_DIR="${ROOT}/processed/${DATE}"
PROCESSED_DIR="${BUILD_DIR}/graph_mmap"
ARCHIVE_DIR="${ROOT}/compacted"
ARCHIVE="${ARCHIVE_DIR}/gandalf_translatorkg_${DATE}.tar.gz"
SYMLINK="${ROOT}/processed/latest"
TMP_DIR="${ROOT}/tmp"

# Never echo credentials that might be embedded in a Mongo URI
SAFE_URI="$(printf '%s' "${GANDALF_MONGO_URI}" | sed -E 's#//[^/@]*@#//***@#')"

# -----------------------------------------------------------------------------
# Preflight: fail in the first second, not after a 30 minute build
# -----------------------------------------------------------------------------
need() {
    if ! command -v "$1" > /dev/null 2>&1; then
        echo "ERROR: required command not found: $1${2:+ ($2)}" >&2
        exit 1
    fi
}

need "${BUILD_BIN}" "set GANDALF_BUILD_MONGO_BIN to its full path inside Gandalf's virtualenv, e.g. /opt/home/dogpark/gandalf/.venv/bin/gandalf-build-mongo"
need tar
if [[ -n "${SERVICE}" ]]; then
    need sudo
    need systemctl
fi
if [[ -n "${S3_BUCKET}" ]]; then
    need aws "the AWS CLI must be installed for the user running this script"
fi

echo "Gandalf build ${DATE}: ${GANDALF_MONGO_DB}.${GANDALF_NODES_COLLECTION} + ${GANDALF_EDGES_COLLECTION} @ ${SAFE_URI}"
if [[ -n "${GANDALF_RELEASE:-}" ]]; then
    echo "Upstream KGX release: ${GANDALF_RELEASE}"
fi

mkdir -p "${TMP_DIR}" "${ARCHIVE_DIR}" "${BUILD_DIR}"

# -----------------------------------------------------------------------------
# Step 1 - Build the Gandalf graph
# -----------------------------------------------------------------------------
echo "[1/5] Building Gandalf graph -> ${PROCESSED_DIR}"
TMPDIR="${TMP_DIR}" "${BUILD_BIN}" \
    --mongo-uri "${GANDALF_MONGO_URI}" \
    --db "${GANDALF_MONGO_DB}" \
    --nodes-collection "${GANDALF_NODES_COLLECTION}" \
    --edges-collection "${GANDALF_EDGES_COLLECTION}" \
    --output "${PROCESSED_DIR}/"

# -----------------------------------------------------------------------------
# Step 2 - Compact the graph into a tarball
# -----------------------------------------------------------------------------
echo "[2/5] Compressing graph_mmap -> ${ARCHIVE}"
( cd "${BUILD_DIR}" && tar -czf "${ARCHIVE}" graph_mmap/ )

# -----------------------------------------------------------------------------
# Step 3 - Update the 'latest' symlink
# -----------------------------------------------------------------------------
echo "[3/5] Updating symlink ${SYMLINK} -> ${PROCESSED_DIR}"
ln -sfn "${PROCESSED_DIR}/" "${SYMLINK}"

# -----------------------------------------------------------------------------
# Step 4 - Restart the Gandalf service
# -----------------------------------------------------------------------------
if [[ -n "${SERVICE}" ]]; then
    echo "[4/5] Restarting ${SERVICE}"
    # -n: fail at once instead of waiting for a password nobody can type
    sudo -n systemctl restart "${SERVICE}"
else
    echo "[4/5] GANDALF_SERVICE is empty, skipping the service restart"
fi

# -----------------------------------------------------------------------------
# Step 5 - Upload the archive to AWS S3
# -----------------------------------------------------------------------------
if [[ -n "${S3_BUCKET}" ]]; then
    S3_URI="s3://${S3_BUCKET}/${S3_PREFIX:+${S3_PREFIX%/}/}$(basename "${ARCHIVE}")"
    echo "[5/5] Uploading ${ARCHIVE} -> ${S3_URI}"
    # --no-progress: the progress meter rewrites one line with carriage returns,
    # which turns a hub log into a single enormous line
    aws_args=(s3 cp "${ARCHIVE}" "${S3_URI}" --no-progress)
    if [[ -n "${GANDALF_S3_REGION:-}" ]]; then
        aws_args+=(--region "${GANDALF_S3_REGION}")
    fi
    if [[ -n "${GANDALF_S3_PROFILE:-}" ]]; then
        aws_args+=(--profile "${GANDALF_S3_PROFILE}")
    fi
    aws "${aws_args[@]}"
else
    echo "[5/5] WARNING: GANDALF_S3_BUCKET is not set, skipping the S3 upload of ${ARCHIVE}"
fi

echo "Done. Build date: ${DATE}"
