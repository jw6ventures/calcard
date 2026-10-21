#!/usr/bin/env bash

# Prints the release channel a ref publishes to: "main", "beta", or nothing at
# all for a branch that publishes to no channel. Exits non-zero, printing
# nothing on stdout, for a tag it does not recognize as a release.
#
# The channel is read from the ref itself rather than from which branches
# contain the commit. A release merge that fast-forwards leaves main and develop
# on the same commit, so "which branch is this tag on" has no single answer
# precisely when a release is being published. The tag name always has one.
#
# Tags fail closed. Every tag publishes an image under its own name and a chart
# versioned from it, so a tag that is not exactly vMAJOR.MINOR.PATCH or
# vMAJOR.MINOR.PATCH-{rc,beta,alpha}N would either overwrite a channel tag
# (latest, beta), publish an image the chart's "v<appVersion>" default never
# names, or fail at helm package after :beta had already been pushed.

set -euo pipefail

REF="${1:?usage: release-channel.sh <git ref>}"
DEFAULT_BRANCH="${DEFAULT_BRANCH:-main}"
BETA_BRANCH="${BETA_BRANCH:-develop}"

STABLE_TAG='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
# The marker is matched case-insensitively because both -rcN and -RCN have
# been tagged; the version part is not, so V1.2.0-rc1 is still refused.
PRERELEASE_TAG='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-([A-Za-z]+)[0-9]+$'

refuse() {
  local message="$1"
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    # stderr, because stdout is the channel the caller captures.
    printf '::error title=Unrecognized release tag::%s\n' "${message}" >&2
  else
    printf '%s: %s\n' "$(basename "$0")" "${message}" >&2
  fi
  exit 1
}

case "${REF}" in
  refs/tags/*)
    tag="${REF#refs/tags/}"
    if [[ "${tag}" =~ ${STABLE_TAG} ]]; then
      printf 'main\n'
    elif [[ "${tag}" =~ ${PRERELEASE_TAG} ]] && [[ "${BASH_REMATCH[4],,}" =~ ^(rc|beta|alpha)$ ]]; then
      printf 'beta\n'
    else
      refuse "tag '${tag}' is not a release tag (want vMAJOR.MINOR.PATCH or vMAJOR.MINOR.PATCH-rcN/-betaN/-alphaN); nothing was published"
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
