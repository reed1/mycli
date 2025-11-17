#!/bin/bash

set -euo pipefail

temp_dir=$(mktemp -d /tmp/test_proj_mycli.XXXXXX)

script_dir=$(readlink -f $(dirname $0))

cat <<EOF > "$temp_dir/mycli"
#!/bin/bash

cd "$script_dir"
uv run mycli "\$@"
EOF

chmod +x "$temp_dir/mycli"

export PATH="$temp_dir":$PATH

db "$@"

rm -rf "$temp_dir"
