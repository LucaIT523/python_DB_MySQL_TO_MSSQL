# 

<div align="center">
   <h1>python_DB_MSSQL_TO_MySQL</h1>
</div>



This code implements a database synchronization system between Microsoft SQL Server (MSSQL) and MySQL.



### 1. Core Architecture

**Main Workflow**

```
DB Connection → Schema Comparison → Table Synchronization → Record Synchronization → Commit Changes
```

**Key Components**

1. **Connection Management**
2. **Schema Synchronization**
3. **Data Synchronization**
4. **Type Conversion System**

### 2. Technical Implementation

**A. Connection Setup**

```
# MSSQL Connection
mssql_con = pyodbc.connect('Driver={SQL Server};...')

# MySQL Connection
mysql_con = mysql.connector.connect(...)
```

- Uses pyodbc for MSSQL connection
- Leverages mysql-connector-python for MySQL

**B. Schema Synchronization**

```
def table_sync(self, ms_table_list, my_table_list):
    # Remove extra tables in MySQL
    for table in my_table_list:
        if table not in ms_table_list:
            self.my_cursor.execute(f"DROP TABLE {table}")
```

- Maintains table parity between databases
- Drops MySQL tables not present in MSSQL

**C. Data Type Conversion**

```
pythonCopydef change_MSSQL2MySQL_datatype(self, info):
    if info[1] == "uniqueidentifier":
        return ["char", 36]
    elif info[1] == "image":
        return "longblob"
```

- Handles key type conversions:
  - `uniqueidentifier` → `CHAR(36)`
  - `image` → `LONGBLOB`

**D. Record Synchronization**

```
def table_record_sync(self):
    # Three-way sync process:
    1. Insert new records
    2. Update modified records
    3. Delete removed records
```

- Uses primary key comparison for delta detection
- Implements granular field-level comparison

### 3. Key Features

**A. Intelligent Synchronization**

- Schema version control through table structure comparison
- Column parity checks with `check_table_column_name()`
- Automatic table recreation on structural changes

**B. Data Handling**

- Ordered record fetching (`ORDER BY` primary key)
- Character data normalization (whitespace stripping)
- Type-aware comparison logic

**C. Transaction Management**

- Explicit commit operations after each modification
- Connection cleanup in finally blocks
- Error handling with transaction rollback

### 4. Operational Parameters

| **Synchronization Logic** | **Implementation**            |
| ------------------------- | ----------------------------- |
| Insert Threshold          | New MSSQL records             |
| Update Threshold          | Field-level value differences |
| Delete Threshold          | MySQL-exclusive records       |
| Batch Size                | Full-table operation          |
| Concurrency               | Single-threaded execution     |





### **Contact Us**

For any inquiries or questions, please contact us.

telegram : @topdev1012

email :  skymorning523@gmail.com

Teams :  https://teams.live.com/l/invite/FEA2FDDFSy11sfuegI