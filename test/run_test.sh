#!/bin/bash

# Get the folder where this script is saved
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 [1/4] Creating local test CSV..."
cat <<EOF > "${BASE_DIR}/test_data.csv"
1,Alice,NYC
2,Bob,LA
3,Charlie,CHI
EOF

echo "📁 [2/4] Uploading to HDFS..."
docker exec namenode hdfs dfs -mkdir -p /mif/test/raw
docker cp "${BASE_DIR}/test_data.csv" namenode:/tmp/test_data.csv
docker exec namenode hdfs dfs -put -f /tmp/test_data.csv /mif/test/raw/

echo "⚙️  [3/4] Running Hive SQL..."
docker cp "${BASE_DIR}/test_hive.sql" hiveserver2:/tmp/test_hive.sql
OUTPUT=$(docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n hive -f /tmp/test_hive.sql 2>&1)
echo "$OUTPUT"

echo "🧹 [4/4] Cleaning up..."
docker exec namenode hdfs dfs -rm -r -skipTrash /mif/test 2>/dev/null || true
docker exec namenode rm -f /tmp/test_data.csv || true
docker exec hiveserver2 rm -f /tmp/test_hive.sql || true
rm -f "${BASE_DIR}/test_data.csv"

echo "------------------------------------------------"
if echo "$OUTPUT" | grep -q "Alice"; then
    echo "✅ TEST PASSED: Data successfully processed through Tez/Hive."
else
    echo "❌ TEST FAILED: Could not find test data in output."
fi