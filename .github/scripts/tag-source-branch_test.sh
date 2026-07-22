#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOLVER="${SCRIPT_DIR}/tag-source-branch.sh"
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "${TEST_DIR}"' EXIT

git -C "${TEST_DIR}" init --bare origin.git >/dev/null
git -C "${TEST_DIR}" init work >/dev/null
git -C "${TEST_DIR}/work" config user.name "Workflow Test"
git -C "${TEST_DIR}/work" config user.email "workflow-test@example.com"
git -C "${TEST_DIR}/work" remote add origin "${TEST_DIR}/origin.git"

commit_file() {
  local contents="$1"
  printf '%s\n' "${contents}" >"${TEST_DIR}/work/file.txt"
  git -C "${TEST_DIR}/work" add file.txt
  git -C "${TEST_DIR}/work" commit -m "${contents}" >/dev/null
}

assert_branch() {
  local want="$1"
  local sha="$2"
  local got

  got="$(git -C "${TEST_DIR}/work" rev-parse "${sha}" | (
    cd "${TEST_DIR}/work"
    xargs bash "${RESOLVER}"
  ))"
  if [ "${got}" != "${want}" ]; then
    echo "resolved branch ${got@Q}, want ${want@Q}" >&2
    exit 1
  fi
}

commit_file "develop commit"
git -C "${TEST_DIR}/work" branch -M develop
git -C "${TEST_DIR}/work" push -u origin develop >/dev/null
assert_branch develop HEAD

git -C "${TEST_DIR}/work" switch -c release/next >/dev/null
commit_file "release commit"
git -C "${TEST_DIR}/work" push -u origin release/next >/dev/null
assert_branch release/next HEAD

release_commit="$(git -C "${TEST_DIR}/work" rev-parse HEAD)"
commit_file "newer release commit"
git -C "${TEST_DIR}/work" push origin release/next >/dev/null
assert_branch release/next "${release_commit}"

git -C "${TEST_DIR}/work" branch release/alternate
git -C "${TEST_DIR}/work" push origin release/alternate >/dev/null
if (
  cd "${TEST_DIR}/work"
  bash "${RESOLVER}" "$(git rev-parse HEAD)"
) 2>"${TEST_DIR}/error"; then
  echo "ambiguous tag commit unexpectedly resolved to a branch" >&2
  exit 1
fi
if ! grep -q "multiple branches" "${TEST_DIR}/error"; then
  echo "ambiguous resolution did not explain the failure" >&2
  cat "${TEST_DIR}/error" >&2
  exit 1
fi
