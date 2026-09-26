#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOLVER="${SCRIPT_DIR}/release-channel.sh"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

export DEFAULT_BRANCH=main
export BETA_BRANCH=develop
# Cases that care about annotations set this themselves; the rest must not
# depend on whether the suite itself runs under Actions.
ACTIONS_CI="${GITHUB_ACTIONS:-}"
unset GITHUB_ACTIONS

failures=0

fail() {
  echo "$*" >&2
  failures=$((failures + 1))
}

assert_channel() {
  local want="$1" ref="$2" got status=0
  got="$(bash "${RESOLVER}" "${ref}" 2>/dev/null)" || status=$?
  if [ "${status}" -ne 0 ]; then
    fail "release-channel.sh ${ref} exited ${status}, want channel ${want@Q}"
  elif [ "${got}" != "${want}" ]; then
    fail "release-channel.sh ${ref} = ${got@Q}, want ${want@Q}"
  fi
}

# A refused tag must exit non-zero with nothing on stdout, so a caller that
# captures the channel cannot go on to publish under an empty or stale one.
assert_refused() {
  local ref="$1" got status=0
  got="$(bash "${RESOLVER}" "${ref}" 2>/dev/null)" || status=$?
  if [ "${status}" -eq 0 ]; then
    fail "release-channel.sh ${ref} was accepted as ${got@Q}, want refusal"
  elif [ -n "${got}" ]; then
    fail "release-channel.sh ${ref} refused but still printed channel ${got@Q}"
  fi
}

# assert_stderr pins what the resolver says: nothing for a recognized ref, a
# plain message locally, and an ::error:: annotation under Actions so the
# refusal is visible on the run summary rather than only in the step log.
assert_stderr() {
  local want="$1" ref="$2" stderr
  case "${want}" in
    silent)
      stderr="$(bash "${RESOLVER}" "${ref}" 2>&1 >/dev/null || true)"
      if [ -n "${stderr}" ]; then
        fail "release-channel.sh ${ref} wrote to stderr unexpectedly: ${stderr@Q}"
      fi
      ;;
    error)
      stderr="$(bash "${RESOLVER}" "${ref}" 2>&1 >/dev/null || true)"
      if [ -z "${stderr}" ]; then
        fail "release-channel.sh ${ref} refused without saying why"
      elif [[ "${stderr}" == ::* ]]; then
        fail "release-channel.sh ${ref} emitted a workflow command outside Actions: ${stderr@Q}"
      fi
      ;;
    annotated-error)
      stderr="$(GITHUB_ACTIONS=true bash "${RESOLVER}" "${ref}" 2>&1 >/dev/null || true)"
      if [[ "${stderr}" != "::error"* ]]; then
        fail "release-channel.sh ${ref} under Actions did not emit an ::error:: annotation: ${stderr@Q}"
      fi
      ;;
    *)
      fail "assert_stderr: unknown expectation ${want@Q} for ${ref}"
      ;;
  esac
}

# A stable tag is exactly vMAJOR.MINOR.PATCH. The chart defaults its image to
# "v<appVersion>" and the binary reads its version (and so its migrations) from
# the tag, so the v and the canonical numbers are part of the contract.
assert_channel main refs/tags/v1.2.0
assert_channel main refs/tags/v0.0.0
assert_channel main refs/tags/v10.20.30
assert_stderr silent refs/tags/v1.2.0

assert_refused refs/tags/1.2.0
assert_refused refs/tags/V1.2.0
assert_refused refs/tags/v01.2.0
assert_refused refs/tags/v1.02.0
assert_refused refs/tags/v1.2.00

# rc, beta and alpha are the pre-release markers the release process produces.
# Each takes a number. The marker is case-insensitive because both spellings
# have been tagged in this repository; the version part is held to the stable
# rules.
assert_channel beta refs/tags/v1.2.0-rc1
assert_channel beta refs/tags/v1.2.0-rc12
assert_channel beta refs/tags/v1.1.3-RC1
assert_channel beta refs/tags/v1.2.0-beta1
assert_channel beta refs/tags/v1.2.0-beta10
assert_channel beta refs/tags/v1.2.0-alpha1
assert_channel beta refs/tags/v1.2.0-alpha10
assert_channel beta refs/tags/v1.2.0-BETA1
assert_stderr silent refs/tags/v1.2.0-beta1

assert_refused refs/tags/1.2.0-rc1
assert_refused refs/tags/V1.2.0-rc1
assert_refused refs/tags/v01.2.0-rc1

# Every tag publishes an image under its own name, so a tag that is not a
# release shape is refused outright: diverting it to beta would still push
# :beta and :<tag> before helm package rejected the version. That covers the
# near misses -- a marker without a number, a dotted or unknown pre-release, a
# four- or two-component version -- and the channel names themselves, which
# would otherwise overwrite the channel image of the same name.
assert_refused refs/tags/v1.2.0-rc
assert_refused refs/tags/v1.2.0-rc1.1
assert_refused refs/tags/v1.2.0-alpha.1
assert_refused refs/tags/v1.2.0-dev1
assert_refused refs/tags/v1.2.0.1
assert_refused refs/tags/v1.2
assert_refused refs/tags/nightly
assert_refused refs/tags/latest
assert_refused refs/tags/beta
assert_refused refs/tags/main
assert_refused refs/tags/develop
assert_refused refs/tags/
assert_stderr error refs/tags/nightly
assert_stderr annotated-error refs/tags/nightly
assert_stderr annotated-error refs/tags/latest

# Branch pushes keep their existing channels, and every other branch publishes
# a sha-tagged image alone.
assert_channel main refs/heads/main
assert_channel beta refs/heads/develop
assert_channel "" refs/heads/release/next
assert_channel "" refs/heads/feature/thing
assert_stderr silent refs/heads/feature/thing

# Every tag already in the repository must still classify: vX.Y.Z to main,
# vX.Y.Z-rcN in either case to beta. A tag that stopped resolving would make a
# re-run of its release fail. CI fetches tags for this; finding none there means
# the checkout lost them and this check silently stopped running.
repo_tags=0
if git -C "${REPO_ROOT}" rev-parse --git-dir >/dev/null 2>&1; then
  while IFS= read -r tag; do
    [ -n "${tag}" ] || continue
    repo_tags=$((repo_tags + 1))
    if [[ "${tag}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      assert_channel main "refs/tags/${tag}"
    else
      assert_channel beta "refs/tags/${tag}"
    fi
  done < <(git -C "${REPO_ROOT}" tag -l)
fi
if [ "${ACTIONS_CI}" = "true" ] && [ "${repo_tags}" -eq 0 ]; then
  fail "no git tags found under Actions; the checkout must fetch tags for the existing-tag cases to run"
fi

if [ "${failures}" -ne 0 ]; then
  echo "${failures} release-channel case(s) failed" >&2
  exit 1
fi
echo "release-channel.sh: all cases passed"
