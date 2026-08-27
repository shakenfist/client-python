#!/bin/bash

# Scan this repository's git history for leaked credentials.
#
# Two things happen here, and the second is the more important one:
#
# 1. gitleaks scans every commit reachable from HEAD -- which on a pull
#    request means the whole of develop plus the branch under test --
#    and the script fails if anything is found.
#
# 2. A positive control proves the scanner can still fire. A detector
#    which reports nothing is indistinguishable from a detector which is
#    broken: a rule set that changed shape under us, a shallow clone, an
#    allowlist grown wide enough to forgive everything. So we plant two
#    credentials in a scratch directory and fail if gitleaks does not
#    report both. Green here means "scanned and found nothing", not
#    "did nothing".
#
# Reachability from HEAD, rather than gitleaks' default of every ref, is
# deliberate: it keeps the scan away from any branch carrying built
# artefacts, and on a pull request HEAD already reaches the branch under
# test and all of develop.
#
# Usage:
#   tools/gitleaks-scan.sh [--gitleaks PATH]
#
# Runs from anywhere inside the working tree -- it changes to the top
# itself -- but the clone must be a full one, not shallow.

set -e

GITLEAKS=gitleaks
while [ $# -gt 0 ]; do
    case "$1" in
        --gitleaks)
            if [ -z "$2" ]; then
                echo "--gitleaks needs a path."
                exit 1
            fi
            GITLEAKS="$2"
            shift 2
            ;;
        *)
            # Refuse rather than ignore. A silently discarded flag would
            # leave the caller believing they had changed the scan.
            echo "Unrecognised argument: $1"
            echo "Usage: tools/gitleaks-scan.sh [--gitleaks PATH]"
            exit 1
            ;;
    esac
done

# Resolve gitleaks once, here, while the caller's working directory is
# still the one they typed the argument against. The scan runs from the
# top of the tree (below), so anything still relative at that point
# resolves somewhere the caller did not mean -- which is a "command not
# found" halfway through a security scan that reported it had found the
# binary. Three forms have to land in the same place: a bare name on
# PATH, a path with a slash in it, and a bare name that happens to be an
# executable in the caller's directory.
resolved=$(command -v "$GITLEAKS" 2>/dev/null || true)
if [ -z "$resolved" ] && [ -x "$GITLEAKS" ]; then
    resolved="$GITLEAKS"
fi
if [ -z "$resolved" ]; then
    echo "gitleaks not found. Install it, or pass --gitleaks PATH."
    exit 1
fi
case "$resolved" in
    /*) ;;
    *) resolved="$(cd "$(dirname "$resolved")" && pwd)/$(basename "$resolved")" ;;
esac
GITLEAKS="$resolved"

# The positive control plants an SSH private key, so ssh-keygen is as
# much a dependency of this script as gitleaks is. Debian's gitleaks
# package does not pull openssh-client in, and a minimal image may not
# carry it, in which case set -e would abort on a bare "command not
# found" -- skipping every "do not trust a clean scan" message below,
# which is the one outcome this script is written to prevent.
if ! command -v ssh-keygen >/dev/null 2>&1; then
    echo "ssh-keygen not found, so the positive control cannot plant its"
    echo "second credential. Install openssh-client."
    echo
    echo "Do not trust a clean scan until this passes."
    exit 1
fi

echo "Using $("$GITLEAKS" version) from $GITLEAKS"

if [ "$(git rev-parse --is-shallow-repository)" = "true" ]; then
    echo "This is a shallow clone, so most of history cannot be scanned."
    echo "Check out with fetch-depth: 0."
    exit 1
fi

cd "$(git rev-parse --show-toplevel)"

# The positive control. Both credentials are generated here rather than
# written into this file, because a literal one would be found by the
# scan below -- correctly, since a credential in a committed file is
# exactly what we are looking for.
#
# Two rules rather than one, so a single rule quietly disappearing
# between gitleaks releases is visible rather than merely halving the
# control. Both come from the stock rule set; this repository carries no
# gitleaks config of its own.
CONTROL=$(mktemp -d)
trap 'rm -rf "$CONTROL"' EXIT

printf 'GITHUB_TOKEN = "ghp_%s"\n' \
    "$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 36)" \
    > "$CONTROL/planted.py"
ssh-keygen -q -t rsa -b 2048 -N '' -C control@example.com \
    -f "$CONTROL/id_rsa"

echo
echo "Positive control: two credentials planted in a scratch directory."
set +e
"$GITLEAKS" detect --source "$CONTROL" --no-git --redact --no-banner \
    --report-path "$CONTROL/report.json" --report-format json
control_status=$?
set -e

# The control ran under `set +e`, so it may have failed before writing a
# report at all -- a renamed flag in a newer release, a missing rules
# file, an OOM. Unguarded, `set -e` would then abort on the failed
# command substitution and the last thing the reader saw would be a
# Python traceback, rather than the message below which exists for
# exactly this case. The traceback still prints, because knowing whether
# the report was absent or malformed is worth having; it is the exit
# that is taken over. The path goes via argv rather than being
# interpolated into the source, so an odd TMPDIR cannot break the parse.
if ! found=$(python3 - "$CONTROL/report.json" <<'PYTHON'
import json
import sys

with open(sys.argv[1]) as f:
    print(' '.join(sorted({x['RuleID'] for x in json.load(f)})), end='')
PYTHON
); then
    echo
    echo "gitleaks' report could not be read (exit ${control_status}), so the"
    echo "positive control cannot be evaluated."
    echo
    echo "Do not trust a clean scan until this passes."
    exit 1
fi

for rule in github-pat private-key; do
    case " $found " in
        *" $rule "*) ;;
        *)
            echo
            echo "The positive control failed: gitleaks did not report the"
            echo "$rule rule against a credential planted for it to find."
            echo "Rules which did fire: ${found:-none}."
            echo
            echo "Do not trust a clean scan until this passes."
            exit 1
            ;;
    esac
done

if [ $control_status -eq 0 ]; then
    echo "The positive control did not set a failure exit code."
    exit 1
fi

echo "Positive control passed: both planted credentials were reported."
echo

# The real scan.
echo "Scanning every commit reachable from HEAD."
"$GITLEAKS" detect --source . --log-opts="HEAD" --redact --verbose \
    --no-banner
