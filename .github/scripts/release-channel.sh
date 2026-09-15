#!/usr/bin/env bash

# Prints the release channel a ref publishes to: "main", "beta", or nothing at
# all for a ref that publishes to no channel.
#
# The channel is read from the ref itself rather than from which branches
# contain the commit. A release merge that fast-forwards leaves main and develop
# on the same commit, so "which branch is this tag on" has no single answer
# precisely when a release is being published -- and a question with no answer
# is a failed publish. The tag name always has one.

set -euo pipefail

REF="${1:?usage: release-channel.sh <git ref>}"
DEFAULT_BRANCH="${DEFAULT_BRANCH:-main}"
BETA_BRANCH="${BETA_BRANCH:-develop}"

case "${REF}" in
  refs/tags/*)
    tag="${REF#refs/tags/}"
    # Matched case-insensitively because both spellings have been tagged.
    if [[ "${tag,,}" =~ -rc[0-9]+$ ]]; then
      printf 'beta\n'
    else
      printf 'main\n'
    fi
    ;;
  *)
    branch="${REF#refs/heads/}"
    case "${branch}" in
      "${DEFAULT_BRANCH}") printf 'main\n' ;;
      "${BETA_BRANCH}") printf 'beta\n' ;;
      # Every other branch publishes a sha-tagged image and no channel tag.
      *) printf '\n' ;;
    esac
    ;;
esac
