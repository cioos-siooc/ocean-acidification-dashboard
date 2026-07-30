#!/bin/sh
set -e

# If CLICKHOUSE_PASSWORD is set, configure the default user password.
if [ -n "$CLICKHOUSE_PASSWORD" ]; then
  mkdir -p /etc/clickhouse-server/users.d
  cat > /etc/clickhouse-server/users.d/default-password.xml <<EOF
<clickhouse>
  <users>
    <default>
      <password>${CLICKHOUSE_PASSWORD}</password>
    </default>
  </users>
</clickhouse>
EOF
fi

exec /entrypoint.sh "$@"
