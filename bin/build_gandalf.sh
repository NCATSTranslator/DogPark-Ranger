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
# variables first.
#
# Before it builds anything it checks its whole configuration and the
# environment it runs in. When something is missing or can't work, it prints
# every problem, exits with status 2, and has built, restarted and uploaded
# nothing. That beats finding out after a 30 minute build.
#
# Required:
#   GANDALF_MONGO_URI          e.g. mongodb://localhost:27017
#   GANDALF_MONGO_DB           database that holds the two collections below
#   GANDALF_NODES_COLLECTION   e.g. tier0_kg_nodes
#   GANDALF_EDGES_COLLECTION   e.g. tier0_kg_edges
#   GANDALF_S3_BUCKET          bucket for the archive, the name only (no s3://). Set it to an
#                              empty string to skip the upload on purpose. Unset is an error,
#                              so a forgotten setting can't silently skip the upload.
#
# Optional (the defaults reproduce the previous hand-run script):
#   GANDALF_ROOT               /data/dogpark/gandalf   processed/, compacted/ and tmp/ live under it
#   GANDALF_BUILD_MONGO_BIN    gandalf-build-mongo     use a full path into Gandalf's .venv if it is not on PATH
#   GANDALF_SERVICE            gandalf.service         set it to an empty string to skip the restart
#   GANDALF_S3_PREFIX          key prefix inside the bucket
#   GANDALF_S3_REGION          passed to `aws --region`
#   GANDALF_S3_PROFILE         passed to `aws --profile`
#   GANDALF_RELEASE            upstream KGX release the graph is built from (printed, nothing more)
#   GANDALF_BUILD_DATE         MM_DD_YYYY stamp for paths and the archive name (default: today)
#   GANDALF_SKIP_AWS_CHECK     1 = don't ask AWS who we are up front (for a host that reaches S3 but not STS)
#
# In the hub the first four come from the build itself. The rest come from
# GANDALF_BUILD in config.py: "gandalf_bin" is GANDALF_BUILD_MONGO_BIN, "s3" holds
# "bucket", "prefix", "region" and "profile" (None means an empty bucket, so no
# upload), and "service" and "root" are what they say.
#
# The checks: the settings above exist and look right, the commands it needs are
# installed, GANDALF_ROOT is writable, the systemd unit exists and sudo will
# restart it without a password, and AWS accepts the credentials in use.
#
# AWS credentials are not handled here: `aws` uses whatever the user running
# this script already has (instance role, ~/.aws, or GANDALF_S3_PROFILE).
# Needs bash 3.2 or newer, so it also runs on a stock macOS shell.
# Exit status: 0 done, 2 not set up to run (nothing was touched), anything else
# is the failing step's own status, with an ERROR line saying where.
# =============================================================================

set -euo pipefail

trap 'rc=$?; echo "ERROR: build_gandalf.sh failed at line ${LINENO} (exit ${rc})" >&2' ERR

# -----------------------------------------------------------------------------
# Settings and paths (same layout as the previous hand-run script)
# -----------------------------------------------------------------------------
DATE="${GANDALF_BUILD_DATE:-$(date +"%m_%d_%Y")}"
ROOT="${GANDALF_ROOT:-/data/dogpark/gandalf}"
BUILD_BIN="${GANDALF_BUILD_MONGO_BIN:-gandalf-build-mongo}"
# "-" rather than ":-" on purpose: an explicitly empty value skips the restart
SERVICE="${GANDALF_SERVICE-gandalf.service}"
S3_BUCKET="${GANDALF_S3_BUCKET-}"
S3_PREFIX="${GANDALF_S3_PREFIX-}"

BUILD_DIR="${ROOT}/processed/${DATE}"
PROCESSED_DIR="${BUILD_DIR}/graph_mmap"
ARCHIVE_DIR="${ROOT}/compacted"
ARCHIVE="${ARCHIVE_DIR}/gandalf_translatorkg_${DATE}.tar.gz"
SYMLINK="${ROOT}/processed/latest"
TMP_DIR="${ROOT}/tmp"
S3_URI="s3://${S3_BUCKET}/${S3_PREFIX:+${S3_PREFIX%/}/}$(basename "${ARCHIVE}")"

# the same options for every aws call, so the credentials check and the upload
# talk as the same account
aws_flags=()
if [[ -n "${GANDALF_S3_REGION-}" ]]; then
    aws_flags+=(--region "${GANDALF_S3_REGION}")
fi
if [[ -n "${GANDALF_S3_PROFILE-}" ]]; then
    aws_flags+=(--profile "${GANDALF_S3_PROFILE}")
fi

# -----------------------------------------------------------------------------
# Preflight: find every problem in the first seconds, not after a 30 minute
# build. Nothing is created, built, restarted or uploaded until all checks pass.
# -----------------------------------------------------------------------------
PROBLEMS=""
NPROBLEMS=0

problem() {
    PROBLEMS="${PROBLEMS}  - $1"$'\n'
    NPROBLEMS=$((NPROBLEMS + 1))
}

stop_if_problems() {
    if [[ "${NPROBLEMS}" -eq 0 ]]; then
        return 0
    fi
    {
        echo "ERROR: build_gandalf.sh is not set up to run. Nothing was built, restarted or uploaded."
        printf '%s' "${PROBLEMS}"
        echo "In the hub, GANDALF_BUILD in config.py supplies these: \"gandalf_bin\" and \"s3\": {\"bucket\": ..., \"prefix\": ...}"
        echo "(or \"s3\": None to skip the upload on purpose). Restart the hub after editing config.py."
        echo "By hand, export the variables described at the top of this script."
    } >&2
    exit 2
}

require_set() {
    local name="$1" example="$2"
    if [[ -z "${!name-}" ]]; then
        problem "${name} is not set (${example})"
    fi
}

have() {
    command -v "$1" > /dev/null 2>&1
}

# --- the settings themselves ---
require_set GANDALF_MONGO_URI "e.g. mongodb://localhost:27017"
require_set GANDALF_MONGO_DB "the database that holds both collections"
require_set GANDALF_NODES_COLLECTION "e.g. tier0_kg_nodes"
require_set GANDALF_EDGES_COLLECTION "e.g. tier0_kg_edges"
# "+" rather than "-" on purpose: unset is an error, set-but-empty is a choice
if [[ -z "${GANDALF_S3_BUCKET+set}" ]]; then
    problem "GANDALF_S3_BUCKET is not set: name the bucket to upload to, or set it to an empty string to skip the upload on purpose"
fi

uri_re='^mongodb(\+srv)?://'
if [[ -n "${GANDALF_MONGO_URI-}" && ! "${GANDALF_MONGO_URI}" =~ ${uri_re} ]]; then
    problem "GANDALF_MONGO_URI must start with mongodb:// or mongodb+srv://"
fi

date_re='^[0-9]{2}_[0-9]{2}_[0-9]{4}$'
if [[ ! "${DATE}" =~ ${date_re} ]]; then
    problem "GANDALF_BUILD_DATE must look like MM_DD_YYYY, for example 10_09_2026, because it names folders and the archive. Got '${DATE}'"
fi

if [[ "${ROOT}" != /* ]]; then
    problem "GANDALF_ROOT must be an absolute path. Got '${ROOT}'"
else
    nearest="${ROOT}"
    while [[ ! -d "${nearest}" && "${nearest}" != "/" ]]; do
        nearest="$(dirname "${nearest}")"
    done
    if [[ ! -w "${nearest}" ]]; then
        problem "GANDALF_ROOT ${ROOT} can't be created or written: $(id -un) can't write to ${nearest}"
    fi
fi

if [[ -n "${S3_BUCKET}" ]]; then
    bucket_re='^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$'
    if [[ ! "${S3_BUCKET}" =~ ${bucket_re} ]]; then
        problem "GANDALF_S3_BUCKET '${S3_BUCKET}' is not a bucket name: give the name only, without s3:// or a path (GANDALF_S3_PREFIX holds the path)"
    fi
    if [[ "${S3_PREFIX}" == /* || "${S3_PREFIX}" == *://* || "${S3_PREFIX}" == *[[:space:]]* ]]; then
        problem "GANDALF_S3_PREFIX '${S3_PREFIX}' must be a plain path like gandalf: no leading /, no s3://, no spaces"
    fi
    region_re='^[a-z0-9-]+$'
    if [[ -n "${GANDALF_S3_REGION-}" && ! "${GANDALF_S3_REGION}" =~ ${region_re} ]]; then
        problem "GANDALF_S3_REGION '${GANDALF_S3_REGION}' is not a region name, for example us-west-2"
    fi
fi

# --- the commands it will run ---
if ! have "${BUILD_BIN}"; then
    problem "required command not found: ${BUILD_BIN} (set GANDALF_BUILD_MONGO_BIN to its full path inside Gandalf's virtualenv, e.g. /opt/home/dogpark/gandalf/.venv/bin/gandalf-build-mongo)"
fi
if ! have tar; then
    problem "required command not found: tar"
fi
if [[ -n "${SERVICE}" ]]; then
    if ! have sudo; then
        problem "required command not found: sudo (needed to restart ${SERVICE}; an empty GANDALF_SERVICE skips the restart)"
    fi
    if ! have systemctl; then
        problem "required command not found: systemctl (needed to restart ${SERVICE}; an empty GANDALF_SERVICE skips the restart)"
    fi
fi
if [[ -n "${S3_BUCKET}" ]] && ! have aws; then
    problem "required command not found: aws (the AWS CLI must be installed for the user running this script)"
fi

# only talk to systemd and AWS once everything above is in order
stop_if_problems

# --- systemd and AWS: can this user really do what the last two steps need? ---
if [[ -n "${SERVICE}" ]]; then
    if ! systemctl cat "${SERVICE}" > /dev/null 2>&1; then
        problem "systemd has no unit called ${SERVICE} (check GANDALF_SERVICE)"
    fi
    # -l only lists the command without running it, -n never waits for a password nobody can type
    if ! reason="$(sudo -n -l systemctl restart "${SERVICE}" 2>&1)"; then
        problem "sudo won't run 'systemctl restart ${SERVICE}' for $(id -un) without a password: ${reason%%$'\n'*}"
    fi
fi

IDENTITY=""
if [[ -n "${S3_BUCKET}" && "${GANDALF_SKIP_AWS_CHECK-}" != "1" ]]; then
    if IDENTITY="$(aws sts get-caller-identity --query Arn --output text \
        --cli-connect-timeout 10 --cli-read-timeout 20 ${aws_flags[@]+"${aws_flags[@]}"} 2>&1)"; then
        IDENTITY="${IDENTITY##*$'\n'}"
    else
        problem "AWS rejected the credentials of $(id -un): ${IDENTITY%%$'\n'*} (use an instance role, ~/.aws or GANDALF_S3_PROFILE; GANDALF_SKIP_AWS_CHECK=1 skips this check)"
        IDENTITY=""
    fi
fi
stop_if_problems

# Never echo credentials that might be embedded in a Mongo URI
SAFE_URI="$(printf '%s' "${GANDALF_MONGO_URI}" | sed -E 's#//[^/@]*@#//***@#')"

echo "Gandalf build ${DATE}: ${GANDALF_MONGO_DB}.${GANDALF_NODES_COLLECTION} + ${GANDALF_EDGES_COLLECTION} @ ${SAFE_URI}"
if [[ -n "${GANDALF_RELEASE:-}" ]]; then
    echo "Upstream KGX release: ${GANDALF_RELEASE}"
fi
echo "Output:   ${PROCESSED_DIR}"
if [[ -n "${SERVICE}" ]]; then
    echo "Restart:  ${SERVICE}"
else
    echo "Restart:  skipped (GANDALF_SERVICE is empty)"
fi
if [[ -n "${S3_BUCKET}" ]]; then
    echo "Upload:   ${S3_URI}${IDENTITY:+ (as ${IDENTITY})}"
else
    echo "Upload:   skipped (GANDALF_S3_BUCKET is empty)"
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
    echo "[5/5] Uploading ${ARCHIVE} -> ${S3_URI}"
    # --no-progress: the progress meter rewrites one line with carriage returns,
    # which turns a hub log into a single enormous line
    aws s3 cp "${ARCHIVE}" "${S3_URI}" --no-progress ${aws_flags[@]+"${aws_flags[@]}"}
else
    echo "[5/5] GANDALF_S3_BUCKET is empty, skipping the S3 upload of ${ARCHIVE}"
fi

echo "Done. Build date: ${DATE}"
