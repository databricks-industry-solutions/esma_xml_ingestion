# Databricks notebook source
# MAGIC %md
# MAGIC # XML Schema XSD Processing
# MAGIC 
# MAGIC This notebook converts XML Schema Definition (XSD) files into JSON schemas that Apache Spark can use for structured XML data ingestion. Apache Spark's XML reader requires schemas in JSON format rather than XSD, so this conversion enables type-safe parsing of complex regulatory XML documents while maintaining data validation rules.
# MAGIC 
# MAGIC The notebook processes regulatory XML files by creating multiple schema artifacts: master schemas for the overall document structure, payload schemas for business data, and metadata schemas for headers and control information. Using Spark's XSDToSchema utility (Scala), it converts XSD files and creates specialized Python schemas for different XML components. These schemas are stored as JSON files for reuse across ingestion pipelines, ensuring data quality from the earliest stage of ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters
# MAGIC 
# MAGIC Define the paths and mappings that control the schema conversion process:
# MAGIC 
# MAGIC - `schemas_path`: Output directory for generated JSON schemas
# MAGIC - `master_xsd_path`: Path to the main XSD file defining the overall document structure
# MAGIC - `payload_xsd_path`: Path to the XSD containing business data definitions
# MAGIC - `row_tag`: XML element name used as the row boundary for Spark reading (e.g., "Stat", "Tx")
# MAGIC - `schema_mappings_json`: JSON array mapping XML fields to their XSD files
# MAGIC 
# MAGIC Configure these parameters based on your specific XML schema files and structure before running the notebook.

# COMMAND ----------
dbutils.widgets.text("schemas_path", "/Volumes/esma/default/regulatory_data/emir/schemas/")
dbutils.widgets.text("master_xsd_path", "/Volumes/esma/default/regulatory_data/emir/xsd/master_schema.xsd")
dbutils.widgets.text("payload_xsd_path", "/Volumes/esma/default/regulatory_data/emir/xsd/payload_schema.xsd")
dbutils.widgets.text("row_tag", "Stat")
dbutils.widgets.text("schema_mappings_json", '[{"field": "Hdr", "file_path": "/path/to/header.xsd"}, {"field": "Pyld", "file_path": "/path/to/payload.xsd", "payload": true}]')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve and Parse Parameters
# MAGIC 
# MAGIC Parse the schema mappings JSON to identify which XSD files correspond to header and payload sections of the XML documents. This mapping tells the notebook how to separate metadata from business data.

# COMMAND ----------

import json

schemas_path = dbutils.widgets.get("schemas_path")
master_xsd_path = dbutils.widgets.get("master_xsd_path") 
payload_xsd_path = dbutils.widgets.get("payload_xsd_path")
row_tag = dbutils.widgets.get("row_tag")
schema_mappings_json = dbutils.widgets.get("schema_mappings_json")

# Parse schema mappings from JSON string
schema_mappings = json.loads(schema_mappings_json)

print(f"Schemas output path: {schemas_path}")
print(f"Master XSD path: {master_xsd_path}")
print(f"Payload XSD path: {payload_xsd_path}")  
print(f"Row tag: {row_tag}")
print(f"Using schema mappings from job parameters:")
for mapping in schema_mappings:
    print(f"  - {mapping['field']}: {mapping['file_path']}")
    if mapping.get('payload'):
        print(f"    (Payload field)")
        payloadXsdPath = mapping['file_path']

# COMMAND ----------

# MAGIC %md
# MAGIC ## XSD to JSON Schema Conversion (Scala)
# MAGIC 
# MAGIC Convert XSD files to JSON schemas using Spark's native XSD parser. Spark's `spark-xml` library includes a Scala-based XSD reader that understands complex XSD structures (imports, complex types, restrictions), ensuring accurate schema translation.
# MAGIC 
# MAGIC The Scala function reads each XSD file, parses it into a Spark StructType, and serializes it as JSON. This process runs for the master XSD, payload XSD, and any additional XSD files found in the schemas directory.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Get widget values
# MAGIC val masterXsdPath = dbutils.widgets.get("master_xsd_path")
# MAGIC val schemaPath = dbutils.widgets.get("schemas_path") 
# MAGIC val payloadXsdPath = dbutils.widgets.get("payload_xsd_path")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.execution.datasources.xml.XSDToSchema
# MAGIC import java.nio.file.{Files, Paths}
# MAGIC import java.nio.charset.StandardCharsets
# MAGIC 
# MAGIC // Function to convert XSD to JSON schema
# MAGIC def convertXsdToJson(xsdPath: String, outputJsonPath: String): Unit = {
# MAGIC   try {
# MAGIC     if (xsdPath.nonEmpty) {
# MAGIC       println(s"Processing XSD: $xsdPath")
# MAGIC       
# MAGIC       // Read XSD file and convert to Spark schema
# MAGIC       val xsdContent = scala.io.Source.fromFile(xsdPath).mkString
# MAGIC       val schema = XSDToSchema.read(xsdContent)
# MAGIC       
# MAGIC       // Convert schema to JSON
# MAGIC       val schemaJson = schema.json
# MAGIC       
# MAGIC       // Write JSON schema to file
# MAGIC       Files.write(Paths.get(outputJsonPath), schemaJson.getBytes(StandardCharsets.UTF_8))
# MAGIC       
# MAGIC       println(s"✓ Schema converted and written to: $outputJsonPath")
# MAGIC       println(s"  Schema has ${schema.fields.length} top-level fields")
# MAGIC       
# MAGIC     } else {
# MAGIC       println(s"⚠ XSD path is empty, skipping conversion")
# MAGIC     }
# MAGIC   } catch {
# MAGIC     case e: Exception => 
# MAGIC       println(s"✗ Error converting XSD $xsdPath: ${e.getMessage}")
# MAGIC       e.printStackTrace()
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC println("=== XSD to JSON Schema Conversion ===")
# MAGIC 
# MAGIC // Convert each XSD file to JSON schema
# MAGIC if (masterXsdPath.nonEmpty) {
# MAGIC   val filename = masterXsdPath.split("/").last.replace(".xsd", ".json")
# MAGIC   val outputPath = s"${schemaPath}${filename}"
# MAGIC   convertXsdToJson(masterXsdPath, outputPath)
# MAGIC }
# MAGIC 
# MAGIC import java.io.File
# MAGIC 
# MAGIC val xsdDir = new File(schemaPath)
# MAGIC 
# MAGIC println(xsdDir)
# MAGIC if (xsdDir.exists && xsdDir.isDirectory) {
# MAGIC   val xsdFiles = xsdDir.listFiles
# MAGIC     .filter(f => f.isFile && f.getName.endsWith(".xsd"))
# MAGIC     .filterNot(f => 
# MAGIC       f.getName.equalsIgnoreCase(new File(masterXsdPath).getName) || 
# MAGIC       f.getName.equalsIgnoreCase(new File(payloadXsdPath).getName)
# MAGIC     )
# MAGIC   xsdFiles.foreach { file =>
# MAGIC     val filename = file.getName.replace(".xsd", ".json")
# MAGIC     val outputPath = s"${schemaPath}${filename}"
# MAGIC     convertXsdToJson(file.getAbsolutePath, outputPath)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC if (payloadXsdPath.nonEmpty) {
# MAGIC   val filename = payloadXsdPath.split("/").last.replace(".xsd", ".json")
# MAGIC   val outputPath = s"${schemaPath}${filename}"
# MAGIC   convertXsdToJson(payloadXsdPath, outputPath)
# MAGIC }
# MAGIC println("=== Conversion Complete ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Specialized Schemas
# MAGIC 
# MAGIC Generate two specialized schemas from the master schema to enable efficient data processing:
# MAGIC 
# MAGIC 1. `pyld_schema.json` - Contains only the payload (business data) structure
# MAGIC 2. `hdr_pyld_metadata_schema.json` - Contains header and metadata fields
# MAGIC 
# MAGIC Separating payload from metadata improves query performance and allows different processing strategies. Headers typically contain routing and control information (submission dates, sender IDs, file batching details), while payloads contain the actual regulatory data records (transactions, statements, reports).
# MAGIC 
# MAGIC The Python utility function extracts specific fields from the master schema based on your schema mappings, creating filtered schemas for targeted parsing. For example, in EMIR data, the header contains submission metadata while the payload contains transaction records.

# COMMAND ----------

from util.xsd_processor import create_specialized_schemas

master_json_path = master_xsd_path.replace(".xsd", ".json")
payload_json_path = payload_xsd_path.replace(".xsd", ".json")

result = create_specialized_schemas(
    master_json_path=master_json_path,
    schema_mappings=schema_mappings,
    row_tag_name=row_tag,
    output_folder=schemas_path,
    validate_schemas=True
)

if result["success"]:
    print(f"✓ Created pyld_schema.json and hdr_pyld_metadata_schema.json")
    pyld_schema_path = result['pyld_schema_path']
    metadata_schema_path = result['metadata_schema_path']
else:
    print(f"✗ Error: {result['error']}")
    pyld_schema_path = None
    metadata_schema_path = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Row Tag XSD
# MAGIC 
# MAGIC Extract a single repeating element (row tag) from the payload XSD and create a standalone XSD file for validation. Spark's XML reader processes XML files by identifying a repeating element as "rows" in a DataFrame, and the row tag XSD enables row-level validation during streaming ingestion, catching malformed records before they enter your data lake.
# MAGIC 
# MAGIC The utility extracts the XML element definition matching the row tag name from the payload XSD, along with its type definition and dependencies, creating a minimal, focused XSD for validation. For example, if your row tag is "Stat", this extracts the StatType definition and creates `row_tag_schema.xsd` containing only that structure for efficient validation.

# COMMAND ----------

import os
from util.xsd_processor import create_row_tag_xsd

row_tag_xsd_output = os.path.join(schemas_path, f"row_tag_schema.xsd")

result = create_row_tag_xsd(
    payload_xsd_path=payload_xsd_path,
    row_tag_name=row_tag,
    output_path=row_tag_xsd_output,
    validate_output=True,
)

if result["success"]:
    print(f"✅ Row tag XSD created successfully!")
    print(f"   Output file: {os.path.basename(result['output_path'])}")
    print(f"   Row tag element: {result['row_tag_element']}")
    print(f"   Row tag type: {result['row_tag_type']}")
    
    # Check file size
    file_size = os.path.getsize(row_tag_xsd_output)
    print(f"   File size: {file_size:,} bytes")
    
    row_tag_xsd_path = result['output_path']
    
    print(f"\n💡 This XSD can be used with Spark XML processing:")
    print(f"   spark.read.format('xml').option('row_tag', '{row_tag}').schema(...).load(...)")
else:
    print(f"❌ Row tag XSD creation failed: {result['error']}")
    row_tag_xsd_path = None
