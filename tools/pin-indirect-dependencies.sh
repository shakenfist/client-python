#!/bin/bash
#
# Detect unpinned indirect (transitive) dependencies and create a PR
# to record them in pyproject.toml as an optional "pinned" extra.
#
# Usage: tools/pin-indirect-dependencies.sh <venv-pip-path> <project-dir>
#
# Requires:
#   - GITHUB_TOKEN environment variable (for gh CLI)
#   - pyproject.toml with "# END_OF_INDIRECT_DEPS" marker

set -e

PIP_PATH="$1"
PROJECT_DIR="$2"

if [ -z "${PIP_PATH}" ] || [ -z "${PROJECT_DIR}" ]; then
    echo "Usage: $0 <venv-pip-path> <project-dir>"
    exit 1
fi

cd "${PROJECT_DIR}"
datestamp=$(date "+%Y%m%d")
git checkout -b "pin-dependencies-${datestamp}"

for depver in $("${PIP_PATH}" freeze --local); do
    dep="${depver%%==*}"
    if [ "$(grep -ic "${dep}==" pyproject.toml)" -lt 1 ]; then
        sed -i \
            "s/    # END_OF_INDIRECT_DEPS/    \"${depver}\",\n    # END_OF_INDIRECT_DEPS/" \
            pyproject.toml
    fi
done

# Did we find something new?
if [ "$(git diff | wc -l)" -gt 0 ]; then
    echo "New dependencies detected..."
    echo
    git diff
    git config --global user.name "shakenfist-bot"
    git config --global user.email "bot@shakenfist.com"
    git commit -a -m "Update pinned dependencies."
    git push -f origin "pin-dependencies-${datestamp}"
    echo
    gh label create dependencies --color 0075ca \
        --description "Pull requests that update a dependency file" \
        2>/dev/null || true
    gh pr create \
        --assignee mikalstill \
        --reviewer mikalstill \
        --title "Update pinned dependencies." \
        --body "New indirect dependencies were detected." \
        --label dependencies
    echo
    echo "Pull request created."
fi
