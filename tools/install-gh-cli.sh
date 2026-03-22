#!/bin/bash
#
# Install the GitHub CLI (gh) on Debian/Ubuntu systems.
#
# Usage: tools/install-gh-cli.sh

set -e

sudo apt update
sudo apt install -y curl

KEYRING="/usr/share/keyrings/githubcli-archive-keyring.gpg"
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | sudo dd of="${KEYRING}"
sudo chmod go+r "${KEYRING}"

ARCH=$(dpkg --print-architecture)
REPO="deb [arch=${ARCH} signed-by=${KEYRING}]"
REPO="${REPO} https://cli.github.com/packages stable main"
echo "${REPO}" | sudo tee /etc/apt/sources.list.d/github-cli.list \
    > /dev/null

sudo apt update
sudo apt install -y gh
