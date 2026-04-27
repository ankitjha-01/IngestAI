#!/bin/bash

set -euo pipefail

NAMENODE="namenode"
DIVIDER="─────────────────────────────────────────────────────"

PROCESSING_DATE=$(date +%F)

hdfs() {
    docker exec "$NAMENODE" hdfs "$@"
}

section() {
    echo ""
    echo "$DIVIDER"
    echo "  $1"
    echo "$DIVIDER"
}

# 1. Cluster health
section "Cluster Health"
hdfs dfsadmin -report 2>/dev/null \
    | grep -E "Name:|Live datanodes|Dead datanodes|DFS Used:|DFS Remaining:"

# 2. Raw contents
section "Raw Data Contents — /mif/raw"
hdfs dfs -ls -R /mif/raw

# 3. HDFS namespace overview
section "HDFS Namespace — /mif (disk usage)"
hdfs dfs -du -h /mif/

# 4. Block health check on card transactions
# section "Block Health — /mif/raw/card_transactions"
# hdfs fsck /mif/raw/card_transactions/ -files -blocks 2>/dev/null \
#     | grep -E "^/|Status:|Total blocks|Minimally|Corrupt|Missing"

# 5. File count summary
section "File Count Summary"

# total across all partitions
TOTAL=$(hdfs dfs -ls -R /mif/raw 2>/dev/null | grep "^-" | wc -l | tr -d ' ')
echo "  Files in /mif/raw: $TOTAL"

# per source (partition-aware)
for source in card_transactions merchant_updates atm_withdrawals fraud_flags; do
    COUNT=$(hdfs dfs -ls -R /mif/raw/"$source"/processing_date="$PROCESSING_DATE" 2>/dev/null \
        | grep "^-" | wc -l | tr -d ' ')
    printf "    %-25s %s file(s)\n" "$source" "$COUNT"
done

# Done
echo ""
echo "Verification completed."
echo ""