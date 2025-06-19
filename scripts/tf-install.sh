#!/bin/bash

set -euo pipefail
cd -P -- "$(dirname -- "$0")/../"

plugin_vsn="$(grep -o 'plugin_rel_vsn, "[^"]*"' rebar.config | cut -d'"' -f2)"
plugin_namevsn="emqx_ecq-$plugin_vsn"
pushd ../tf-emqx-performance-test
dashboard_url=$(terraform output -json | jq '.emqx_dashboard_url.value' -r)
popd

echo "Dashboard URL: $dashboard_url"
echo "Plugin name-vsn: $plugin_namevsn"

echo "Building the plugin..."
# Build the plugin.
make

echo "Allowing plugin installation..."
# Allow plugin installation
pushd ../tf-emqx-performance-test
ansible emqx -m command -a "emqx ctl plugins allow $plugin_namevsn" --become --limit 'emqx-core-1.*'
popd

echo "Installing the plugin..."
# Install the plugin.
creds="perftest:perftest"
curl -s -u $creds -X POST $dashboard_url/api/v5/plugins/install \
-H "Content-Type: multipart/form-data" \
-F "plugin=@_build/default/emqx_plugrel/${plugin_namevsn}.tar.gz"

echo "Starting the plugin..."
curl -s -u perftest:perftest -X PUT "$dashboard_url/api/v5/plugins/${plugin_namevsn}/start"

echo "Plugin installed."
echo "Plugins:"
curl -s -u perftest:perftest "$dashboard_url/api/v5/plugins" | jq
