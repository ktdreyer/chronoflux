#!/usr/bin/env bash
# One-time setup of the BigQuery dataset and tables; no service accounts or IAM changes.
set -euo pipefail

usage() {
  echo "Usage: $0 PROJECT LOCATION [DATASET]" >&2
  echo "Example: $0 aipcc-cicd us-east1 chronoflux" >&2
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
  usage
  exit 0
fi
if (( $# < 2 || $# > 3 )); then
  usage
  exit 2
fi

project=$1
location=$2
dataset=${3:-chronoflux}
repo_url=https://github.com/ktdreyer/chronoflux
description="Chronoflux CI build timing data. Source: $repo_url"

# These values are interpolated into BigQuery SQL identifiers below.
if [[ ! $project =~ ^[a-z][a-z0-9-]*$ ||
      ! $location =~ ^[a-zA-Z0-9-]+$ ||
      ! $dataset =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
  echo "Invalid project, location, or dataset identifier" >&2
  exit 2
fi
if ! command -v bq >/dev/null 2>&1; then
  echo "Install the bq CLI (Google Cloud SDK) and authenticate first" >&2
  exit 1
fi

schema="$(dirname -- "${BASH_SOURCE[0]}")/../src/chronoflux/schema.sql"
if [[ ! -r $schema ]]; then
  echo "Missing schema: $schema" >&2
  exit 1
fi

if details=$(bq show --format=json "$project:$dataset" 2>/dev/null); then
  existing_location=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["location"])' <<< "$details")
  if [[ ${existing_location,,} != ${location,,} ]]; then
    echo "Dataset $project:$dataset exists in $existing_location, not $location" >&2
    exit 1
  fi
  echo "Using existing dataset $project:$dataset ($existing_location)"
  existing_description=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("description", ""))' <<< "$details")
  existing_label=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("labels", {}).get("app", ""))' <<< "$details")
  if [[ $existing_description != *"$repo_url"* || $existing_label != chronoflux ]]; then
    # Keep any existing description instead of replacing the owner's notes.
    if [[ $existing_description == *"$repo_url"* ]]; then
      description=$existing_description
    elif [[ -n $existing_description ]]; then
      description="$existing_description Source: $repo_url"
    fi
    bq --project_id="$project" update --description="$description" --set_label=app:chronoflux "$project:$dataset"
  fi
else
  echo "Creating dataset $project:$dataset in $location"
  bq --project_id="$project" --location="$location" mk --dataset \
    --description="$description" --label=app:chronoflux "$project:$dataset"
fi

# schema.sql uses {dataset} for a fully qualified project.dataset identifier.
# CREATE TABLE IF NOT EXISTS makes repeated bootstrap runs safe.
sed "s/{dataset}/$project.$dataset/g" "$schema" |
  bq --project_id="$project" --location="$location" query --use_legacy_sql=false

echo "BigQuery tables ready in $project.$dataset"
