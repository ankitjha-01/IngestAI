## 0. System Architecture Overview

This system follows a distributed master–worker architecture similar to production Hadoop clusters.

### Cluster Layout

- **Master Node:**
  - NameNode (HDFS metadata)
  - ResourceManager (YARN resource scheduling)

- **Worker Nodes (x2):**
  - DataNode (HDFS storage)
  - NodeManager (YARN execution)

### Key Idea

Each worker node contributes:
- **Storage** via DataNode  
- **Compute** via NodeManager  

This ensures:
- Distributed storage (HDFS)
- Distributed computation (YARN)
- Data locality (compute runs near data)


## 1. The Storage Layer: HDFS (Hadoop Distributed File System)

HDFS is the storage layer of the system. In this architecture, storage is distributed across worker nodes, while metadata is managed centrally by the master node.

### Theoretical Architecture
*   **NameNode (`namenode`):** The master directory. It does not store actual data; it manages the *metadata* (the map of where every block of data lives on the cluster). 
*   **DataNodes (`datanode1`, `datanode2`):** These run on worker nodes and store the actual data blocks. Each worker node contributes storage to the distributed system.
*   **RPC (Remote Procedure Call) - Port 9000:** The internal communication network. DataNodes constantly send "heartbeats" via RPC to the NameNode to report their status and the blocks they hold. 

### 🛠️ Practical Observation: Exploring HDFS
Once the cluster is running, navigate to:
👉 **`http://localhost:9870`**

*   **Datanodes Tab:** Verify that **2 Live Nodes** (`datanode1` and `datanode2`) are active. This confirms the storage is distributed.
*   **Browse the File System:** Navigate to **"Utilities" -> "Browse the file system"**. This interface allows for visual exploration of the HDFS directories (e.g., `/mif/hive/warehouse` and `/mif/test/raw`).

---

## 2. The Compute & Resource Layer: YARN

YARN (Yet Another Resource Negotiator) s the resource management layer of the system. It allocates CPU and memory across worker nodes and enables distributed execution of jobs.

### Theoretical Architecture
*   **ResourceManager (`resourcemanager`):** Runs on the master node alongside the NameNode. It manages cluster-wide resources (CPU, memory) and schedules jobs.
*   **NodeManagers (`nodemanager1`, `nodemanager2`):** Run on each worker node. They report available resources and execute tasks inside containers.

### How Execution Works

When a job is submitted (e.g., via Hive):

1. YARN launches an **ApplicationMaster (AM)** for that job  
2. The AM requests **containers** (CPU + memory) from the ResourceManager  
3. Containers are allocated on NodeManagers  
4. Execution engines like **Tez or MapReduce run inside these containers**

> **Key Idea:** YARN only manages resources — it does not execute tasks directly.

### Optimization: Data Locality

YARN tries to run tasks on the same nodes where data exists.

> This reduces network transfer and improves performance — a core reason Hadoop scales well.


### 🛠️ Practical Observation: Exploring YARN
Navigate to:
👉 **`http://localhost:8088`**

*   **Cluster Metrics:** Under **Active Nodes**, the value should be **`2`**, confirming both worker nodes are available for distributed computation.
*   **Resource Limits:** Total allocated memory (e.g., 4096 MB) and vCores (4) are displayed here, matching the `yarn-site.xml` configuration.

---

## 3. The Metadata & Execution Layer: Hive, MapReduce, & Tez

HDFS only understands raw files and directories. Apache Hive bridges this gap by translating SQL queries into distributed processing jobs.

### Execution Engines: MapReduce vs. Tez
Understanding the difference between MapReduce and Tez is critical for performance tuning.

*   **The MapReduce Bottleneck:** MapReduce is a rigid, disk-heavy process involving three main phases:
    1.  **Map:** Reads data from HDFS, processes it, and writes intermediate results to local disk.
    2.  **Shuffle & Sort:** Moves intermediate data across the network and writes sorted data again to disk.
    3.  **Reduce:** Aggregates the grouped data from multiple blocks/nodes and *writes the final output back to HDFS*.
    Because complex queries require multiple MapReduce jobs sequentially, the constant disk I/O makes it incredibly slow.
*   **The Tez Optimization:** Apache Tez replaces this rigid model by converting SQL queries into a DAG (Directed Acyclic Graph). Tez minimizes disk I/O by chaining tasks together in a DAG, allowing intermediate data to be passed directly between tasks using memory or optimized local storage instead of writing to HDFS between stages.

### 🛠️ Practical Observation: Beeline & HiveServer2
To interact with Hive directly from the terminal, use the Beeline CLI to connect to HiveServer2:
```bash
docker exec -it hiveserver2 beeline -u "jdbc:hive2://localhost:10000" -n hive
```

To monitor active queries, navigate to:
👉 **`http://localhost:10002`**

*   **Active Sessions & Queries:** When connected via Beeline or a script, the active session is visible here.
*   **Execution Details:** By clicking into a running query, you can view the execution plan and verify that the **Execution Engine** is actively using `TEZ`.
* you can manually switch engines and test execution:

```sql
-- Switch to MapReduce and test
SET hive.execution.engine=mr;
SELECT 1;
-- Switch back to Tez and test
SET hive.execution.engine=tez;
SELECT 1;
```
> *Note:* For a tiny query, the overhead of starting Tez might make it seem slower than MapReduce. However, for large datasets with complex operations (joins, aggregations), Tez is significantly faster because it avoids writing intermediate data to disk and processes data in memory.

---

## 4. Data Modeling: External, Internal, and ACID Tables

In Hadoop, data storage and schema management are decoupled, resulting in distinct table architectures.

### 1. External Tables (The "Staging" Area)
```sql
CREATE EXTERNAL TABLE test_users_raw (...) LOCATION '/mif/test/raw/';
```
*   **Concept:** Hive acts as a lens over existing files in HDFS. Hive manages the *schema* in the Metastore database, but HDFS manages the *data*.
*   **Lifecycle:** Executing `DROP TABLE` deletes the schema, but **the underlying data files in HDFS remain untouched**. 

### 2. Internal (Managed) Tables
*   **Concept:** Hive assumes full ownership of both the schema and the data. Data is typically moved into Hive's default warehouse directory (`/mif/hive/warehouse`).
*   **Lifecycle:** Executing `DROP TABLE` deletes the schema **and** permanently deletes the underlying HDFS data.

### 3. Transactional (ACID) Tables
```sql
CREATE TABLE test_users_final (...) STORED AS ORC TBLPROPERTIES("transactional"="true");
```
*   **Concept:** A highly specialized type of **Internal (Managed)** table. Since HDFS is natively "append-only," running `UPDATE` or `DELETE` commands requires complex logic. 

*   **Mechanism:** ACID tables maintain "delta" files. Instead of rewriting an entire dataset during an update, Hive writes a small delta file recording the change. During a query, Hive merges the base data and deltas on the fly. This architecture requires the highly compressed `ORC` format and strictly relies on the Tez execution engine.

---

## 5. The End-to-End Flow in Action

Executing the provided `run_test.sh` script demonstrates the full lifecycle of data moving through the platform. Here is how to observe the system at work:

1.  **Data Landing (HDFS):** 
    *   *Action:* The script executes `hdfs dfs -put`, bypassing Hive entirely and landing raw CSV data directly into HDFS.
    *   *Observe:* Go to **`http://localhost:9870`** -> Utilities -> Browse File System. Navigate to `/mif/test/raw` to physically see the CSV file appear.
2.  **The Gateway (HiveServer2):** 
    *   *Action:* The script connects via Beeline to submit `test_hive.sql`.
    *   *Observe:* Go to **`http://localhost:10002`**. Under "Active Sessions," a new connection for user `hive` will appear, showing the SQL query being submitted.
3.  **Distributed Execution (Tez & YARN):** 
    *   *Action:* Hive submits a job to YARN, which launches an ApplicationMaster (AM). The AM requests containers from the ResourceManager and schedules tasks on NodeManagers.
    *   *Observe:* Go to **`http://localhost:8088`**. An application will appear in the applications table. This UI shows how YARN distributes tasks across both worker nodes (`nodemanager1` and `nodemanager2`), utilizing their combined CPU and memory resources.
4. **Tracking Execution via Web UIs:**
    * **HiveServer2 UI (`http://localhost:10002`):** View active sessions and query execution plans
    * **YARN UI (`http://localhost:8088`):** Monitor running applications, memory usage, and CPU (vCores) allocated for the job
