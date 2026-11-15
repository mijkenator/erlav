#!/usr/bin/env bash
# Checks that a release tag matches the version in src/erlav_nif.app.src, so a
# tag can never publish a Hex package whose version differs from the tag.
#
# Hex requires three-part semver but this repo's tags are two-part ( 1.25 ), so
# a two-part tag N.M is read as N.M.0 and a leading "v" is ignored.
#
# Usage: ci/check_hex_version.sh <tag>
# Prints the normalized version on success; exits 1 on mismatch, 2 on bad input.
set -euo pipefail

TAG="${1:?usage: $0 <tag>}"
APP_SRC="$(dirname "$0")/../src/erlav_nif.app.src"

version="${TAG#v}"
if [[ "$version" =~ ^[0-9]+\.[0-9]+$ ]]; then
    version="${version}.0"
fi
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "check_hex_version: tag '${TAG}' is not N.M or N.M.P" >&2
    exit 2
fi

vsn=$(sed -n 's/.*{vsn, *"\([^"]*\)"}.*/\1/p' "$APP_SRC" | head -n 1)
if [ -z "$vsn" ]; then
    echo "check_hex_version: no {vsn, \"...\"} found in ${APP_SRC}" >&2
    exit 2
fi

if [ "$vsn" != "$version" ]; then
    echo "check_hex_version: tag '${TAG}' means version ${version}, but ${APP_SRC} has vsn ${vsn}" >&2
    echo "Bump vsn in the release commit before tagging." >&2
    exit 1
fi
echo "$version"
