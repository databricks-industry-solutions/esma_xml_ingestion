# Databricks notebook source
# MAGIC %md
# MAGIC # Flatten and Explode Table Processing
# MAGIC 
# MAGIC ## What
# MAGIC This notebook transforms nested XML structures from the raw table into flattened, normalized bronze tables. It recursively explodes arrays and nested structs, creating separate tables linked by foreign keys.
# MAGIC 
# MAGIC ## Why
# MAGIC Nested XML structures are difficult to query efficiently. Flattening creates a relational model that enables faster analytics, simpler joins, and better performance for BI tools. Each level of nesting becomes its own table, preserving referential integrity while improving query performance.
# MAGIC 
# MAGIC ## How
# MAGIC The notebook uses a recursive function to traverse the DataFrame schema, extracting simple fields, flattening structs, and exploding arrays. Each array generates a child table with a foreign key to its parent. Surrogate keys ensure uniqueness and enable efficient joins.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters
# MAGIC 
# MAGIC **What:** Define catalog names, schemas, and processing options for flattening
# MAGIC 
# MAGIC **Key Parameters:**
# MAGIC - `catalog` / `raw_schema`: Source location containing raw XML data
# MAGIC - `bronze_schema`: Destination schema for flattened bronze tables
# MAGIC - `table_prefix`: Naming prefix for generated tables (e.g., "emir_")
# MAGIC - `checkpoint_path`: Streaming checkpoint location for fault tolerance

# COMMAND ----------

dbutils.widgets.text("catalog","esma")
dbutils.widgets.text("raw_schema","emir_raw")
dbutils.widgets.text("bronze_schema","emir_bronze")

dbutils.widgets.text("table_prefix", "emir_")
dbutils.widgets.text("checkpoint_path", "/Volumes/esma/default/regulatory_data/emir/checkpoints/")

dbutils.widgets.text("files_per_trigger", "16")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve Parameters

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")

table_prefix = dbutils.widgets.get("table_prefix")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

files_per_trigger = int(dbutils.widgets.get("files_per_trigger"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Table as Stream
# MAGIC 
# MAGIC **What:** Load the raw XML data from Delta Lake as a streaming DataFrame
# MAGIC 
# MAGIC **Why:** Streaming enables incremental processing of new data without reprocessing the entire history. As new XML files arrive and get written to the raw table, this stream automatically picks them up.
# MAGIC 
# MAGIC **How:** Use Delta streaming to read from the raw table created by the ingestion notebook.

# COMMAND ----------

raw_table = f"{table_prefix}_raw"
df = (
    spark.readStream
    .format("delta")
    # .option("filesPerTrigger", files_per_trigger)
    .table(f"{catalog}.{raw_schema}.{raw_table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Recursive Flattening Function
# MAGIC 
# MAGIC **What:** A function that recursively traverses nested DataFrames and generates a list of flattened table definitions
# MAGIC 
# MAGIC **Why:** XML schemas can be deeply nested (structs within arrays within structs). A recursive approach systematically processes each level, creating child tables as needed while preserving relationships.
# MAGIC 
# MAGIC **How:** For each DataFrame, the function:
# MAGIC 1. Creates surrogate keys (_sk) using MD5 hashes for uniqueness
# MAGIC 2. Flattens struct fields into columns
# MAGIC 3. Explodes array fields into child tables with foreign keys
# MAGIC 4. Recursively processes child tables
# MAGIC 
# MAGIC **Example:** A `Transaction` record containing an array of `Counterparties` becomes two tables: `transaction` (parent) and `transaction_Counterparties` (child), linked by `_parent_fk_transaction`.

# COMMAND ----------

import json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer, lit, row_number, concat, coalesce, hash, md5, posexplode_outer
from pyspark.sql.window import Window

def generate_flat_schemas(
    schema: json, df: DataFrame, parent_name: str, df_name: str, parent_sk_col: str = None, 
    parent_table_name: str = None, depth: int = 0
):
    """
    Generate flattened schema and return list of [table_name, dataframe] pairs.
    
    Returns:
        List of [table_name, dataframe] pairs for all flattened tables
    """

    df_list = []
    
    # Create dynamic foreign key column name
    fk_column_name = (f"_parent_fk_{parent_table_name}" if parent_sk_col and parent_table_name 
                     else "_parent_fk" if parent_sk_col else None)
    
    # Build key generation expressions
    df_fields = {f.name: f.dataType for f in df.schema.fields}
    cols = [name for name, dtype in df_fields.items() 
                  if type(dtype) not in [ ArrayType] and not name.startswith('_')]
    key_cols = cols[:10] if len(cols) >= 10 else cols
    
    hash_components = []

    #TODO: Needs validating that you need all the columns or a subset such as 10 that will give us uniqueness
    # for col_name in key_cols:
    for col_name in cols:
        if col_name in df_fields:
            hash_components.append(coalesce(col(col_name).cast("string"), lit("null")))
    
    # Add parent FK if available
    if parent_sk_col and parent_sk_col in df_fields:
        hash_components.append(coalesce(col(parent_sk_col).cast("string"), lit("null")))
    
    # # Add array position if available
    if "array_pos" in df_fields:
        hash_components.append(col("array_pos").cast("string"))

    # Create comprehensive hash
    content_hash = md5(concat(*hash_components)) if hash_components else md5(lit(df_name))
    sk_expr = content_hash.alias("_sk")
    
    select_exprs = []
    added_columns = set()
    
    # Add base columns
    if "file_name" in df_fields:
        select_exprs.append(col("file_name"))
        added_columns.add("file_name")
    
    select_exprs.append(sk_expr)
    added_columns.add("_sk")
    
    if parent_sk_col and fk_column_name and parent_sk_col in df_fields:
        select_exprs.append(col(parent_sk_col).alias(fk_column_name))
        added_columns.add(fk_column_name)
    
    # Process schema fields
    flat_cols = [f.name for f in schema.fields if type(f.dataType) not in [StructType, ArrayType]]
    struct_cols_current = [[parent_name, f] for f in schema.fields if type(f.dataType) is StructType]
    array_cols = [[parent_name, f] for f in schema.fields if type(f.dataType) is ArrayType]
    
    # Add simple columns (excluding duplicates)
    for col_name in flat_cols:
        if col_name not in added_columns:
            select_exprs.append(col(col_name))
            added_columns.add(col_name)
    
    # Process struct flattening
    if struct_cols_current:
        struct_expressions = []
        while struct_cols_current:
            struct_cols_child = []
            for struct_col in struct_cols_current:
                parent_path, field_info = struct_col
                current_path = field_info.name if parent_path == "" else f"{parent_path}.{field_info.name}"
                
                # Add simple fields from struct
                for sub_field in field_info.dataType.fields:
                    if type(sub_field.dataType) not in [StructType, ArrayType]:
                        field_path = f"{current_path}.{sub_field.name}"
                        alias_name = f"{current_path}_{sub_field.name}".replace(".", "_")
                        struct_expressions.append(col(field_path).alias(alias_name))
                
                # Find nested structs and arrays
                for field in field_info.dataType.fields:
                    new_path = struct_col[1].name if struct_col[0] == "" else f"{struct_col[0]}.{struct_col[1].name}"
                    if type(field.dataType) is StructType:
                        struct_cols_child.append([new_path, field])
                    elif type(field.dataType) is ArrayType:
                        array_cols.append([new_path, field])
            
            struct_cols_current = struct_cols_child
        
        select_exprs.extend(struct_expressions)
    
    # Create flattened dataframe and add to result list
    df_struct = df.select(*select_exprs)
    df_list.append([df_name, df_struct])
    
    # Process arrays recursively
    for array_col in array_cols:
        array_path = array_col[1].name if array_col[0] == "" else f"{array_col[0]}.{array_col[1].name}"
        child_table_name = array_path.replace(".", "_")
        child_fk_col_name = f"_parent_fk_{df_name}"
        
        # Create child dataframe with exploded array and position for uniqueness
        df_child = df.select("file_name", sk_expr.alias("_parent_key"), array_path) \
                     .filter(col(array_path).isNotNull()) \
                     .selectExpr("file_name", f"_parent_key as {child_fk_col_name}", 
                               f"posexplode_outer({array_col[1].name}) as (array_pos, {child_table_name})")
        
        # Recursive call and accumulate results
        child_df_list = generate_flat_schemas(df_child.schema, df_child, "", child_table_name, 
                                               child_fk_col_name, df_name, depth + 1)
        df_list.extend(child_df_list)
    
    return df_list

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Flattened Schema Structure
# MAGIC 
# MAGIC **What:** Apply the flattening function to the raw DataFrame schema
# MAGIC 
# MAGIC **Why:** This creates a blueprint of all tables to be generated, with their schemas and relationships defined.

# COMMAND ----------

df_schema = df.schema
base_table_name = f"base"

# COMMAND ----------

df_list = generate_flat_schemas(df_schema, df, "", base_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Table Creation Function
# MAGIC 
# MAGIC **What:** A function to write all flattened DataFrames to Delta tables using streaming writes
# MAGIC 
# MAGIC **Why:** Batch writing would require reprocessing all data each time. Streaming writes with checkpoints enable incremental processing and ensure exactly-once semantics.
# MAGIC 
# MAGIC **How:** For each DataFrame in the flattened list, create a corresponding Delta table with a checkpoint location. The `availableNow` trigger processes all available data then stops.

# COMMAND ----------

def create_all_flattened_tables(df_list, catalog, schema, table_prefix="", 
                               write_mode="append", checkpoint_base_path="/tmp/flattened_tables_checkpoint"):
    """
    Create tables for all flattened dataframes using streaming writes
    
    Args:
        df_list: List of [table_name, dataframe] pairs from flatten_schema function
        catalog: Target catalog name  
        schema: Target schema name
        table_prefix: Optional prefix for table names
        write_mode: Write mode ("append", "complete", "update") - streaming modes only
        checkpoint_base_path: Base path for streaming checkpoints
    """
    
    if not df_list:
        return 
    
    # Create catalog and schema if not exists
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    for table_name, table_df in df_list:
        try:
            # Build full table name and checkpoint path
            full_table_name = f"{table_prefix}_{table_name}" if table_prefix else table_name
            full_path = f"{catalog}.{schema}.{full_table_name}"
            checkpoint_path = f"{checkpoint_base_path}/{full_table_name}"

            # Create streaming write with micro-batch processing
            stream_query = table_df.writeStream \
                .format("delta") \
                .outputMode(write_mode) \
                .option("checkpointLocation", checkpoint_path) \
                .option("mergeSchema", "true") \
                .trigger(availableNow=True) \
                .toTable(full_path)
            
        except Exception as e:
            print(f"Error creating table {full_table_name}: {e}")
            return

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Flattening Pipeline
# MAGIC 
# MAGIC **What:** Create all bronze tables by running the flattening function
# MAGIC 
# MAGIC **Why:** This is the final execution step that transforms raw nested data into queryable relational tables.
# MAGIC 
# MAGIC **How:** Call the table creation function with the list of flattened DataFrames, which writes them to the bronze schema with proper checkpointing.

# COMMAND ----------

create_all_flattened_tables(df_list, catalog, bronze_schema, table_prefix=table_prefix, checkpoint_base_path=checkpoint_path)
