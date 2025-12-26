#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WRAPPER_DIR="${SCRIPT_DIR}/../gradle/wrapper"
BASE64_FILE="${WRAPPER_DIR}/gradle-wrapper.jar.base64"
JAR_FILE="${WRAPPER_DIR}/gradle-wrapper.jar"

if [[ ! -f "${BASE64_FILE}" ]]; then
  echo "Base64 wrapper file not found: ${BASE64_FILE}" >&2
  exit 1
fi

mkdir -p "${WRAPPER_DIR}"
base64 --decode "${BASE64_FILE}" > "${JAR_FILE}"

echo "Restored ${JAR_FILE}"
