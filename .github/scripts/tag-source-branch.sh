#!/usr/bin/env bash

set -euo pipefail

COMMIT="$(git rev-parse "${1:?usage: tag-source-branch.sh <commit>}^{commit}")"

remote_branches() {
  git for-each-ref --format='%(refname)' refs/remotes/origin | while IFS= read -r ref; do
    if [ "${ref}" != "refs/remotes/origin/HEAD" ]; then
      printf '%s\n' "${ref}"
    fi
  done
}

branch_name() {
  printf '%s\n' "${1#refs/remotes/origin/}"
}

mapfile -t exact_branches < <(
  while IFS= read -r ref; do
    if [ "$(git rev-parse "${ref}^{commit}")" = "${COMMIT}" ]; then
      branch_name "${ref}"
    fi
  done < <(remote_branches)
)

if [ "${#exact_branches[@]}" -eq 1 ]; then
  printf '%s\n' "${exact_branches[0]}"
  exit 0
fi

if [ "${#exact_branches[@]}" -gt 1 ]; then
  printf 'tag commit %s is the head of multiple branches: %s\n' \
    "${COMMIT}" "${exact_branches[*]}" >&2
  exit 1
fi

mapfile -t containing_branches < <(
  while IFS= read -r ref; do
    if git merge-base --is-ancestor "${COMMIT}" "${ref}"; then
      branch_name "${ref}"
    fi
  done < <(remote_branches)
)

if [ "${#containing_branches[@]}" -eq 1 ]; then
  printf '%s\n' "${containing_branches[0]}"
  exit 0
fi

if [ "${#containing_branches[@]}" -gt 1 ]; then
  printf 'tag commit %s belongs to multiple branches: %s\n' \
    "${COMMIT}" "${containing_branches[*]}" >&2
else
  printf 'tag commit %s does not belong to any remote branch\n' "${COMMIT}" >&2
fi
exit 1
