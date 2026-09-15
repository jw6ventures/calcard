#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOLVER="${SCRIPT_DIR}/release-channel.sh"

export DEFAULT_BRANCH=main
export BETA_BRANCH=develop

failures=0

assert_channel() {
  local want="$1" ref="$2" got
  got="$(bash "${RESOLVER}" "${ref}")"
  if [ "${got}" != "${want}" ]; then
    echo "release-channel.sh ${ref} = ${got@Q}, want ${want@Q}" >&2
    failures=$((failures + 1))
  fi
}

# A release tag publishes the stable channel, a release candidate the beta one.
# Neither asks which branches hold the commit, which is what previously failed
# when a fast-forward release merge left main and develop on the same tip.
assert_channel main refs/tags/v1.2.0
assert_channel main refs/tags/v1.2.0.1
assert_channel beta refs/tags/v1.2.0-rc1
assert_channel beta refs/tags/v1.2.0-rc12

# Both spellings have been tagged in this repository, and an -RC tag is a
# release candidate whichever case it was written in.
assert_channel beta refs/tags/v1.1.3-RC1

# A tag naming a release candidate without a number is not one: the suffix has
# to be the rc marker the release process actually produces.
assert_channel main refs/tags/v1.2.0-rc
assert_channel main refs/tags/v1.2.0-beta1

# Branch pushes keep their existing channels, and every other branch publishes
# a sha-tagged image alone.
assert_channel main refs/heads/main
assert_channel beta refs/heads/develop
assert_channel "" refs/heads/release/next
assert_channel "" refs/heads/feature/thing

if [ "${failures}" -ne 0 ]; then
  echo "${failures} release-channel case(s) failed" >&2
  exit 1
fi
echo "release-channel.sh: all cases passed"
