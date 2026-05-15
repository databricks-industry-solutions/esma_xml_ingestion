# Databricks notebook source
# MAGIC %md
# MAGIC # Flatten and Explode Table Processing
# MAGIC 
# MAGIC This notebook transforms nested XML structures from the raw table into flattened, normalized bronze tables. Nested XML structures with multiple levels of arrays and structs are difficult to query efficiently in their original form. Flattening creates a relational model that enables faster analytics, simpler joins, and better performance for BI tools.
# MAGIC 
# MAGIC The notebook uses a recursive function to traverse the DataFrame schema, extracting simple fields, flattening nested structs into columns, and exploding arrays into separate tables. Each level of nesting becomes its own table, with surrogate keys linking child tables to their parents. This preserves referential integrity while improving query performance and making the data more accessible to standard SQL and BI tools.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters
# MAGIC 
# MAGIC Configure the flattening process by specifying source and destination locations:
# MAGIC 
# MAGIC - `catalog` / `raw_schema`: Source location containing raw XML data from the ingestion pipeline
# MAGIC - `bronze_schema`: Destination schema for flattened bronze tables
# MAGIC - `table_prefix`: Naming prefix for generated tables (e.g., "emir_") to organize related tables
# MAGIC - `checkpoint_path`: Streaming checkpoint location for fault tolerance and recovery
# MAGIC 
# MAGIC Adjust these parameters based on your data organization strategy before running the notebook.

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
# MAGIC 
# MAGIC Get the widget values and store them as Python variables for use throughout the notebook.

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
# MAGIC Load the raw XML data from Delta Lake as a streaming DataFrame. Streaming enables incremental processing of new data without reprocessing the entire history, which is crucial for efficiency as your data volume grows. As new XML files arrive and get written to the raw table by the ingestion pipeline, this stream automatically picks them up and processes only the new records.

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
# MAGIC Define the core function that recursively traverses nested DataFrames and generates a list of flattened table definitions. XML schemas can be deeply nested (structs within arrays within structs), so a recursive approach systematically processes each level, creating child tables as needed while preserving relationships.
# MAGIC 
# MAGIC For each DataFrame, the function:
# MAGIC 1. Creates surrogate keys (`_sk`) using MD5 hashes to ensure uniqueness
# MAGIC 2. Flattens struct fields into columns with dot-notation names (e.g., `Address_City`)
# MAGIC 3. Explodes array fields into separate child tables with foreign keys back to the parent
# MAGIC 4. Recursively processes child tables to handle deeply nested structures
# MAGIC 
# MAGIC For example, a `Transaction` record containing an array of `Counterparties` becomes two tables: `transaction` (parent) and `transaction_Counterparties` (child), linked by the foreign key `_parent_fk_transaction`. This enables you to query transactions independently or join with counterparties as needed.

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
# MAGIC Apply the flattening function to the raw DataFrame schema to create a blueprint of all tables to be generated. This analysis phase determines the table structure and relationships before any data processing occurs, allowing you to understand the output schema before writing tables.

# COMMAND ----------

df_schema = df.schema
base_table_name = f"base"

# COMMAND ----------

df_list = generate_flat_schemas(df_schema, df, "", base_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Table Creation Function
# MAGIC 
# MAGIC Define a function to write all flattened DataFrames to Delta tables using streaming writes. Batch writing would require reprocessing all data each time the notebook runs, which becomes inefficient as data volumes grow. Streaming writes with checkpoints enable incremental processing and ensure exactly-once semantics, meaning no data is duplicated or lost even if the job fails midway.
# MAGIC 
# MAGIC For each DataFrame in the flattened list, the function creates a corresponding Delta table with a dedicated checkpoint location. The `availableNow` trigger processes all available data and then stops, making it suitable for scheduled batch jobs.

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
# MAGIC Execute the flattening pipeline by creating all bronze tables from the raw data. This is the final execution step that transforms raw nested XML data into queryable relational tables optimized for analytics.
# MAGIC 
# MAGIC Call the table creation function with the list of flattened DataFrames, which writes them to the bronze schema with proper checkpointing for reliability. Monitor the output to see which tables are created and track processing progress.

# COMMAND ----------

create_all_flattened_tables(df_list, catalog, bronze_schema, table_prefix=table_prefix, checkpoint_base_path=checkpoint_path)
