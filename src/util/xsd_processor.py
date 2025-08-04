"""
XSD Processor - Schema manipulation utilities for MiFIR transaction reporting

Provides two main functions:
1. create_specialized_schemas() - Creates pyld_schema.json and hdr_pyld_metadata_schema.json
2. create_row_tag_xsd() - Creates standalone XSD for specific row tag elements

Uses explicit schema mappings for reliable processing - you specify exactly which
fields to replace, eliminating complex auto-detection logic. Includes integrated
namespace handling to prevent "unbound prefix" errors.

For XML generation functionality, see xml_generator.py module.
"""

import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
from typing import Dict, List, Optional, Tuple, Union
import copy
import json
import re

# Integrated namespace utilities (previously from xsd_namespace_utils.py)
import re
from xml.dom import minidom


def create_specialized_schemas(
    master_json_path: str,
    schema_mappings: List[Dict[str, Union[str, bool]]],
    row_tag_name: str,
    output_folder: str,
    validate_schemas: bool = True
) -> Dict[str, Union[str, bool]]:
    """
    Create specialized JSON schemas from master and explicitly mapped schema files.
    
    Creates two schemas:
    1. pyld_schema.json - Contains only the specified row tag structure
    2. hdr_pyld_metadata_schema.json - Contains everything except row tag fields,
       with automatic cleanup of empty structs left after row tag removal
    
    Args:
        master_json_path: Path to the master JSON schema file
        schema_mappings: List of mappings with "field", "file_path", and optional "payload" boolean
        row_tag_name: Name of the row tag to extract (e.g., "Tx", "Rpt")
        output_folder: Directory where output schemas will be saved
        validate_schemas: Whether to validate the generated schemas (default: True)
    
    Returns:
        Dictionary with success status, file paths, and optional validation results
    """
    try:
        if not all([master_json_path, schema_mappings, row_tag_name, output_folder]):
            return {"success": False, "error": "All parameters must be provided"}
        
        if not os.path.exists(master_json_path):
            return {"success": False, "error": f"Master JSON schema file not found: {master_json_path}"}
        
        if not isinstance(schema_mappings, list) or len(schema_mappings) == 0:
            return {"success": False, "error": "schema_mappings must be a non-empty list"}
        
        print(f"Creating specialized schemas: {len(schema_mappings)} mappings for row_tag '{row_tag_name}'")
        
        os.makedirs(output_folder, exist_ok=True)
        
        with open(master_json_path, 'r') as f:
            original_master_schema = json.load(f)
        
        # Remove top-level wrapper before processing to ensure clean metadata schema
        master_schema = unwrap_master_schema(original_master_schema)
        
        processed_mappings = {}
        payload_field = None
        
        for mapping in schema_mappings:
            field_name = mapping.get("field")
            file_path = mapping.get("file_path") 
            is_payload = mapping.get("payload", False)
            
            if not field_name or not file_path:
                return {"success": False, "error": f"Invalid mapping: {mapping}. Must have 'field' and 'file_path'"}
            
            if not os.path.exists(file_path):
                return {"success": False, "error": f"Schema file not found: {file_path}"}
            
            if file_path.endswith('.xsd'):
                json_path = _convert_xsd_to_json(file_path, output_folder)
                if not json_path:
                    return {"success": False, "error": f"Failed to convert XSD to JSON: {file_path}"}
                schema_path = json_path
            else:
                schema_path = file_path
            
            with open(schema_path, 'r') as f:
                schema_content = json.load(f)
            
            processed_mappings[field_name] = schema_content
            
            if is_payload:
                if payload_field is not None:
                    return {"success": False, "error": f"Multiple payload fields specified: {payload_field} and {field_name}"}
                payload_field = field_name
        
        if payload_field is None:
            return {"success": False, "error": "No payload field specified in schema_mappings"}
        
        # Simplified: Check if each mapped field exists in the master schema
        for field_name in processed_mappings.keys():
            if not _field_exists_in_schema(master_schema, field_name):
                return {"success": False, "error": f"Field '{field_name}' not found in master schema"}
        
        combined_schema = _combine_schemas(master_schema, processed_mappings)
        
        payload_schema = processed_mappings[payload_field]
        pyld_schema = _extract_row_tag_schema(payload_schema, row_tag_name)
        if not pyld_schema:
            return {"success": False, "error": f"Could not extract row tag '{row_tag_name}' from payload schema"}
        
        # Add corrupted_record column at the top level for Spark XML corrupt record handling
        pyld_schema = _add_corrupted_record_column(pyld_schema)
        
        metadata_schema = _remove_row_tag_fields(combined_schema, row_tag_name)
        
        pyld_path = os.path.join(output_folder, "pyld_schema.json")
        metadata_path = os.path.join(output_folder, "hdr_pyld_metadata_schema.json")
        
        with open(pyld_path, 'w') as f:
            json.dump(pyld_schema, f, indent=2)
        with open(metadata_path, 'w') as f:
            json.dump(metadata_schema, f, indent=2)
        
        print(f"Generated schemas: {os.path.basename(pyld_path)}, {os.path.basename(metadata_path)}")
        
        validation_results = None
        if validate_schemas:
            pyld_valid = _validate_schema_file(pyld_path, "Payload schema")
            metadata_valid = _validate_schema_file(metadata_path, "Metadata schema")
            
            validation_results = {
                "pyld_valid": pyld_valid,
                "metadata_valid": metadata_valid,
                "all_valid": pyld_valid and metadata_valid
            }
        
        return {
            "success": True,
            "pyld_schema_path": pyld_path,
            "metadata_schema_path": metadata_path,
            "schema_mappings": {field: "loaded" for field in processed_mappings.keys()},
            "validation_results": validation_results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def create_row_tag_xsd(
    payload_xsd_path: str,
    row_tag_name: str,
    output_path: str,
    validate_output: bool = True
) -> Dict[str, Union[str, bool]]:
    """
    Create a standalone XSD for a specific row tag from a payload XSD.
    
    Extracts element structure and creates new XSD with that element as root.
    Uses no-namespace pattern compatible with Auto Loader XML fragments.
    
    Args:
        payload_xsd_path: Path to the payload XSD file
        row_tag_name: Name of the element to extract (e.g., "Tx", "Rpt")
        output_path: Path where the row tag XSD will be saved
        validate_output: Whether to validate the output XSD (default: True)
        
    Returns:
        Dictionary with success status, output path, and element info
    """
    try:
        if not all([payload_xsd_path, row_tag_name, output_path]):
            return {"success": False, "error": "All parameters must be provided"}
        
        if not os.path.exists(payload_xsd_path):
            return {"success": False, "error": f"Payload XSD file not found: {payload_xsd_path}"}
        
        print(f"Creating row tag XSD for '{row_tag_name}' from {os.path.basename(payload_xsd_path)}")
        
        payload_tree = ET.parse(payload_xsd_path)
        payload_root = payload_tree.getroot()
        
        row_tag_info = _find_row_tag_in_payload(payload_root, row_tag_name)
        
        if not row_tag_info:
            return {"success": False, "error": f"Row tag '{row_tag_name}' not found in payload XSD"}
        
        row_tag_xsd = _create_row_tag_xsd(payload_root, row_tag_info)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        _save_xsd_file(row_tag_xsd, output_path)
        
        print(f"Generated: {os.path.basename(output_path)}")
        
        if validate_output:
            if _validate_xsd_schema(output_path):
                print("✓ XSD validation passed")
            else:
                print("⚠ XSD validation failed")
        
        return {
            "success": True,
            "output_path": output_path,
            "row_tag_element": row_tag_info['element_name'],
            "row_tag_type": row_tag_info['type_name'],
            "namespace_used": "no-namespace (Auto Loader compatible)"
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def unwrap_master_schema(master_schema: dict) -> dict:
    """
    Remove the top-level wrapper from master schema if it exists.
    
    This function looks for a top-level struct that contains only one field
    with a 'name' property, and promotes the content of that field to be
    the new root schema. This prevents unnecessary wrapper levels in the
    final metadata schema output.
    
    Args:
        master_schema: The master schema dictionary to unwrap
        
    Returns:
        Unwrapped schema starting directly with meaningful content
        
    Example:
        Input:  {"type": "struct", "fields": [{"name": "Root", "type": {...}}]}
        Output: {...}  # The content of the "Root" field's type
    """
    # Check if this is a struct with fields
    if (isinstance(master_schema, dict) and 
        master_schema.get('type') == 'struct' and 
        'fields' in master_schema):
        
        fields = master_schema['fields']
        
        # If there's exactly one field with a name, unwrap it
        if (len(fields) == 1 and 
            isinstance(fields[0], dict) and 
            'name' in fields[0] and 
            'type' in fields[0]):
            
            single_field = fields[0]
            field_type = single_field['type']
            
            # If the field's type is a dict (schema), return it directly
            if isinstance(field_type, dict):
                print(f"✓ Unwrapped master schema: removed top-level field '{single_field['name']}'")
                return field_type
    
    # If no unwrapping needed, return original
    print("ℹ Master schema: no top-level unwrapping needed")
    return master_schema

def _field_exists_in_schema(schema_dict: dict, field_name: str) -> bool:
    """
    Check if a field name exists anywhere in the schema structure.
    
    Args:
        schema_dict: The schema dictionary to search in
        field_name: The field name to look for
        
    Returns:
        True if the field exists, False otherwise
    """
    if not isinstance(schema_dict, dict):
        return False
    
    if schema_dict.get('type') == 'struct' and 'fields' in schema_dict:
        for field in schema_dict['fields']:
            if field.get('name') == field_name:
                return True
            # Check nested structures
            if 'type' in field and _field_exists_in_schema(field['type'], field_name):
                return True
    
    return False


def _convert_xsd_to_json(xsd_path: str, output_folder: str) -> Optional[str]:
    """
    Convert XSD file to JSON schema format. 
    For now, this is a placeholder that expects pre-converted JSON files.
    In a real implementation, this would use a Scala-based XSD to JSON converter.
    """
    print(f"    ⚠ XSD to JSON conversion not implemented. Please convert XSD files to JSON format using Scala converter.")
    print(f"    Expected JSON file: {xsd_path.replace('.xsd', '.json')}")
    
    # Check if a corresponding JSON file already exists
    json_path = xsd_path.replace('.xsd', '.json')
    if os.path.exists(json_path):
        print(f"    ✓ Found existing JSON file: {json_path}")
        return json_path
    
    # Try in the output folder
    basename = os.path.basename(xsd_path).replace('.xsd', '.json')
    output_json_path = os.path.join(output_folder, basename)
    if os.path.exists(output_json_path):
        print(f"    ✓ Found JSON file in output folder: {output_json_path}")
        return output_json_path
    
    return None





def _combine_schemas(master_schema: dict, field_mappings: Dict[str, dict]) -> dict:
    """
    Combine the master schema with field-specific schemas by replacing 
    placeholder fields with actual schema content recursively.
    """
    def replace_fields_recursive(schema_part: dict, path: str = "") -> dict:
        if not isinstance(schema_part, dict):
            return schema_part
        
        if schema_part.get('type') == 'struct' and 'fields' in schema_part:
            new_fields = []
            
            for field in schema_part['fields']:
                field_name = field.get('name', '')
                current_path = f"{path}.{field_name}" if path else field_name
                
                if field_name in field_mappings:
                    replacement_schema = field_mappings[field_name]
                    new_field = {
                        "name": field_name,
                        "type": replacement_schema,
                        "nullable": field.get('nullable', True),
                        "metadata": field.get('metadata', {})
                    }
                    new_fields.append(new_field)
                else:
                    new_field = field.copy()
                    if 'type' in new_field:
                        new_field['type'] = replace_fields_recursive(
                            new_field['type'], current_path
                        )
                    new_fields.append(new_field)
            
            result = schema_part.copy()
            result['fields'] = new_fields
            return result
            
        elif 'fields' in schema_part:
            new_fields = []
            
            for field in schema_part['fields']:
                field_name = field.get('name', '')
                current_path = f"{path}.{field_name}" if path else field_name
                
                if field_name in field_mappings:
                    replacement_schema = field_mappings[field_name]
                    new_field = {
                        "name": field_name,
                        "type": replacement_schema,
                        "nullable": field.get('nullable', True),
                        "metadata": field.get('metadata', {})
                    }
                    new_fields.append(new_field)
                else:
                    new_field = field.copy()
                    if 'type' in new_field and isinstance(new_field['type'], dict):
                        new_field['type'] = replace_fields_recursive(
                            new_field['type'], current_path
                        )
                    new_fields.append(new_field)
            
            result = schema_part.copy()
            result['fields'] = new_fields
            return result
        
        elif isinstance(schema_part, dict):
            result = {}
            for key, value in schema_part.items():
                if isinstance(value, dict):
                    result[key] = replace_fields_recursive(value, f"{path}.{key}" if path else key)
                elif isinstance(value, list):
                    result[key] = [
                        replace_fields_recursive(item, f"{path}.{key}[{i}]" if path else f"{key}[{i}]") 
                        if isinstance(item, dict) else item 
                        for i, item in enumerate(value)
                    ]
                else:
                    result[key] = value
            return result
        
        return schema_part
    
    return replace_fields_recursive(master_schema)


def _validate_schema_file(file_path: str, schema_name: str) -> bool:
    """Validate that a schema file can be loaded as a Spark schema."""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                schema_json = json.load(f)
            
            try:
                from pyspark.sql.types import StructType
                schema = StructType.fromJson(schema_json)
                field_count = len(schema.fields)
                print(f"✓ {schema_name}: {field_count} fields")
                return True
            except ImportError:
                if isinstance(schema_json, dict) and 'fields' in schema_json:
                    field_count = len(schema_json['fields'])
                    print(f"✓ {schema_name}: {field_count} fields (basic validation)")
                    return True
                else:
                    print(f"✗ {schema_name}: Invalid schema structure")
                    return False
        else:
            print(f"✗ {schema_name}: File not found")
            return False
    except Exception as e:
        print(f"✗ {schema_name}: Validation failed - {str(e)}")
        return False


def _extract_row_tag_schema(schema_dict: dict, row_tag_name: str, path: str = "") -> Optional[dict]:
    """Extract the row_tag field structure from the combined schema."""
    if not isinstance(schema_dict, dict):
        return None
    
    if schema_dict.get('type') == 'struct' and 'fields' in schema_dict:
        for field in schema_dict['fields']:
            field_name = field.get('name', '')
            current_path = f"{path}.{field_name}" if path else field_name
            
            if field_name == row_tag_name:
                field_type = field.get('type', {})
                print(f"✓ Found row_tag field '{row_tag_name}' at: {current_path}")
                
                if field_type.get('type') == 'array':
                    element_type = field_type.get('elementType', {})
                    print(f"  row_tag is array, extracting elementType")
                    return element_type
                else:
                    print(f"  Returning field type directly")
                    return field_type
            else:
                if 'type' in field and isinstance(field['type'], dict):
                    nested_result = _extract_row_tag_schema(field['type'], row_tag_name, current_path)
                    if nested_result:
                        return nested_result
    
    return None
    

def _remove_row_tag_fields(schema_dict: dict, row_tag_name: str, path: str = "") -> dict:
    """
    Remove row_tag fields and their children from the schema.
    Also removes empty structs that result from the removal.
    """
    if not isinstance(schema_dict, dict):
        return schema_dict
    
    if schema_dict.get('type') == 'struct' and 'fields' in schema_dict:
        filtered_fields = []
        
        for field in schema_dict['fields']:
            field_name = field.get('name', '')
            current_path = f"{path}.{field_name}" if path else field_name
            
            if field_name == row_tag_name:
                print(f"✓ Removed row_tag field '{row_tag_name}' at: {current_path}")
                continue
            else:
                new_field = field.copy()
                if 'type' in new_field:
                    new_field['type'] = _remove_row_tag_fields(
                        new_field['type'], row_tag_name, current_path
                    )
                    
                    # Check if the field's type is now an empty struct
                    if _is_empty_struct(new_field['type']):
                        print(f"✓ Removed empty struct field '{field_name}' at: {current_path}")
                        continue
                
                filtered_fields.append(new_field)
        
        result = schema_dict.copy()
        result['fields'] = filtered_fields
        return result
    
    return schema_dict


def _is_empty_struct(schema_part: dict) -> bool:
    """
    Check if a schema part represents an empty struct.
    
    Returns True if:
    - It's a struct type with no fields
    - It's a struct type with only empty nested structs
    """
    if not isinstance(schema_part, dict):
        return False
    
    if schema_part.get('type') != 'struct':
        return False
    
    fields = schema_part.get('fields', [])
    
    # No fields = empty struct
    if len(fields) == 0:
        return True
    
    # Check if all fields are empty structs themselves
    for field in fields:
        field_type = field.get('type', {})
        if not _is_empty_struct(field_type):
            return False  # Found a non-empty field
    
    # All fields are empty structs
    return True


def _add_corrupted_record_column(schema_dict: dict) -> dict:
    """
    Add a corrupted_record column at the top level of a schema.
    
    This column is used by Spark XML reader with the option:
    .option("columnNameOfCorruptRecord", "corrupted_record")
    
    Args:
        schema_dict: The schema dictionary to modify
        
    Returns:
        Modified schema with corrupted_record column added
    """
    if not isinstance(schema_dict, dict):
        return schema_dict
    
    # Create corrupted_record field definition
    corrupted_record_field = {
        "name": "corrupted_record",
        "type": "string",
        "nullable": True,
        "metadata": {
            "description": "Column for corrupt records when using Spark XML reader"
        }
    }
    
    # If it's a struct type, add to fields array
    if schema_dict.get('type') == 'struct' and 'fields' in schema_dict:
        # Make a copy to avoid modifying the original
        result_schema = schema_dict.copy()
        result_schema['fields'] = schema_dict['fields'].copy()
        
        # Check if corrupted_record already exists
        existing_field_names = [field.get('name') for field in result_schema['fields']]
        if 'corrupted_record' not in existing_field_names:
            # Add corrupted_record as the first field for visibility
            result_schema['fields'].insert(0, corrupted_record_field)
            print("✓ Added corrupted_record column at top level of payload schema")
        else:
            print("ℹ corrupted_record column already exists in schema")
        
        return result_schema
    
    # If it's not a struct, wrap it in a struct with corrupted_record
    elif isinstance(schema_dict, dict):
        print("ℹ Wrapping non-struct schema with corrupted_record column")
        return {
            "type": "struct",
            "fields": [
                corrupted_record_field,
                {
                    "name": "data",
                    "type": schema_dict,
                    "nullable": True,
                    "metadata": {}
                }
            ]
        }
    
    return schema_dict


def _validate_xsd_schema(xsd_filepath: str) -> bool:
    """Validate that an XSD file is well-formed and can be parsed."""
    try:
        # Try to parse the XSD file
        tree = ET.parse(xsd_filepath)
        root = tree.getroot()
        
        # Basic validation - check if it's a schema element
        if not root.tag.endswith('schema'):
            print(f"✗ File does not contain a valid XSD schema root element")
            return False
        
        # Check for target namespace
        target_namespace = root.get('targetNamespace')
        if not target_namespace:
            print(f"⚠ Schema does not have a target namespace")
        
        print(f"✓ XSD schema validation passed")
        return True
        
    except ET.ParseError as e:
        print(f"✗ XSD parsing error: {str(e)}")
        return False
    except Exception as e:
        print(f"✗ XSD validation error: {str(e)}")
        return False


def _find_row_tag_in_payload(payload_root: ET.Element, row_tag_name: str) -> Optional[dict]:
    """Find the row tag element definition in the payload XSD."""
    target_namespace = payload_root.get('targetNamespace')
    
    # Strategy 1: Look for direct element definitions
    for child in payload_root:
        if child.tag.endswith('element') and child.get('name') == row_tag_name:
            element_type = child.get('type')
            return {
                'element_name': row_tag_name,
                'type_name': element_type,
                'element_def': child,
                'location': 'direct_element'
            }
    
    # Strategy 2: Look for the row tag by analyzing the document structure
    row_tag_path = _analyze_document_structure_for_row_tag(payload_root, row_tag_name)
    if row_tag_path:
        return row_tag_path
    
    # Strategy 3: Look for the row tag within complex type definitions (fallback)
    for child in payload_root:
        if child.tag.endswith('complexType'):
            type_name = child.get('name')
            row_tag_elem = _find_element_in_complex_type(child, row_tag_name)
            if row_tag_elem:
                element_type = row_tag_elem.get('type')
                return {
                    'element_name': row_tag_name,
                    'type_name': element_type,
                    'element_def': row_tag_elem,
                    'location': f'complex_type.{type_name}'
                }
    
    return None


def _find_element_in_complex_type(complex_type: ET.Element, element_name: str) -> Optional[ET.Element]:
    """Recursively search for an element within a complex type definition."""
    for elem in complex_type.iter():
        if elem.tag.endswith('element'):
            elem_name = elem.get('name')
            if elem_name == element_name:
                return elem
    return None


def _analyze_document_structure_for_row_tag(payload_root: ET.Element, row_tag_name: str) -> Optional[dict]:
    """Analyze the document structure to find the row tag path."""
    # Look for the Document element first
    document_type = None
    for child in payload_root:
        if child.tag.endswith('element') and child.get('name') == 'Document':
            document_type = child.get('type')
            break
        
    if not document_type:
        print(f"    No Document element found")
        return None
    
    print(f"    Found Document element with type: {document_type}")
    
    # Find the Document complex type
    document_complex_type = None
    for child in payload_root:
        if child.tag.endswith('complexType') and child.get('name') == document_type:
            document_complex_type = child
            break
    
    if not document_complex_type:
        print(f"    Document complex type '{document_type}' not found")
        return None
    
    # Look for elements within Document that might contain the row tag
    for elem in document_complex_type.iter():
        if elem.tag.endswith('element'):
            elem_name = elem.get('name')
            elem_type = elem.get('type')
            
            print(f"    Checking element: {elem_name} -> {elem_type}")
            
            # Check if this element type contains our row tag
            if elem_type:
                containing_type = _find_complex_type_by_name(payload_root, elem_type)
                if containing_type:
                    print(f"      Found complex type: {elem_type}")
                    row_tag_elem = _find_element_in_complex_type(containing_type, row_tag_name)
                    if row_tag_elem is not None:
                        element_type = row_tag_elem.get('type')
                        print(f"    ✅ Found row tag in {elem_name} -> {elem_type}: {row_tag_name} -> {element_type}")
                        return {
                            'element_name': row_tag_name,
                            'type_name': element_type,
                            'element_def': row_tag_elem,
                            'location': f'Document.{elem_name}.{elem_type}'
                        }
                    else:
                        print(f"      Row tag '{row_tag_name}' not found in {elem_type}")
                else:
                    print(f"      Complex type '{elem_type}' not found")
    
    print(f"    Row tag '{row_tag_name}' not found in Document structure")
    return None


def _find_complex_type_by_name(root: ET.Element, type_name: str) -> Optional[ET.Element]:
    """Find a complex type definition by name."""
    for child in root:
        if child.tag.endswith('complexType') and child.get('name') == type_name:
            return child
    return None


def _collect_required_types(payload_root: ET.Element, start_type: str, required_types: set, visited: set = None):
    """Recursively collect all types required by a given type."""
    if visited is None:
        visited = set()
    
    if start_type in visited:
        return  # Avoid infinite recursion
    
    visited.add(start_type)
    required_types.add(start_type)
    
    # Find the type definition
    type_def = None
    for child in payload_root:
        if (child.tag.endswith('complexType') or child.tag.endswith('simpleType')) and child.get('name') == start_type:
            type_def = child
            break
    
    if not type_def:
        return
    
    # Find all type references within this type definition
    for elem in type_def.iter():
        # Check 'type' attributes
        if 'type' in elem.attrib:
            referenced_type = elem.attrib['type']
            # Remove namespace prefix if present
            if ':' in referenced_type:
                referenced_type = referenced_type.split(':')[-1]
            
            # Skip built-in XSD types
            if not referenced_type.startswith('xs:') and referenced_type != start_type:
                _collect_required_types(payload_root, referenced_type, required_types, visited)
        
        # Check 'base' attributes (for extensions and restrictions)
        if 'base' in elem.attrib:
            base_type = elem.attrib['base']
            # Remove namespace prefix if present
            if ':' in base_type:
                base_type = base_type.split(':')[-1]
            
            # Skip built-in XSD types
            if not base_type.startswith('xs:') and base_type != start_type:
                _collect_required_types(payload_root, base_type, required_types, visited)


# ============================================================================
# INTEGRATED NAMESPACE UTILITIES (previously from xsd_namespace_utils.py)
# ============================================================================


def _create_row_tag_xsd(payload_root: ET.Element, row_tag_info: dict) -> ET.Element:
    """
    Create a new XSD structure with the row tag as the root element using the no-namespace pattern.
    
    Follows the no-namespace pattern: no target namespace, unqualified local type references,
    xs: prefix only for XSD built-in types, elementFormDefault="qualified".
    """
    # Create new schema root with clean attributes to avoid duplicates
    schema_root = ET.Element('xs:schema', attrib={
        'xmlns:xs': 'http://www.w3.org/2001/XMLSchema',
        'elementFormDefault': 'qualified'
    })
    
    # Add the row tag as root element with unqualified type reference
    root_element = ET.SubElement(schema_root, 'xs:element')
    root_element.set('name', row_tag_info['element_name'])
    
    # Use unqualified type reference (no tns: prefix)
    type_name = row_tag_info['type_name']
    if type_name.startswith('tns:'):
        type_name = type_name[4:]  # Remove 'tns:' prefix if present
    root_element.set('type', type_name)
    
    # Collect all required types starting from the row tag type
    required_types = set()
    _collect_required_types(payload_root, type_name, required_types)
    
    # Add all required type definitions with no-namespace type fixing
    added_count = 0
    total_fixes = 0
    
    for child in payload_root:
        if (child.tag.endswith('complexType') or child.tag.endswith('simpleType')):
            child_type_name = child.get('name')
            if child_type_name and child_type_name in required_types:
                type_copy = copy.deepcopy(child)
                
                # Apply the no-namespace type reference fixing
                before_fixes = _count_prefixed_type_references(type_copy)
                _update_type_references_no_namespace(type_copy, required_types)
                after_fixes = _count_prefixed_type_references(type_copy)
                
                fixes_in_this_type = before_fixes - after_fixes
                total_fixes += fixes_in_this_type
                
                schema_root.append(type_copy)
                added_count += 1
    
    # Final pass to ensure all type references are clean
    remaining_fixes = _count_prefixed_type_references(schema_root)
    if remaining_fixes > 0:
        _update_type_references_no_namespace(schema_root, required_types)
    
    return schema_root


def _update_type_references_no_namespace(element: ET.Element, local_types: set):
    """
    Update type references in an element to use the no-namespace pattern:
    - xs: prefix for XSD built-in types 
    - No prefix for local types
    - Remove any tns: prefixes
    
    Args:
        element: Element to process
        local_types: Set of locally defined type names
    """
    
    # Comprehensive list of built-in XSD types that should have xs: prefix
    builtin_types = {
        'string', 'decimal', 'integer', 'int', 'boolean', 'date', 'dateTime', 'time',
        'float', 'double', 'base64Binary', 'anyURI', 'QName', 'normalizedString',
        'token', 'language', 'NMTOKEN', 'NMTOKENS', 'Name', 'NCName', 'ID', 'IDREF',
        'IDREFS', 'ENTITY', 'ENTITIES', 'hexBinary', 'notationDeclaration',
        'duration', 'gYear', 'gYearMonth', 'gMonth', 'gMonthDay', 'gDay',
        'byte', 'short', 'long', 'unsignedByte', 'unsignedShort', 'unsignedInt',
        'unsignedLong', 'positiveInteger', 'nonPositiveInteger', 'negativeInteger',
        'nonNegativeInteger'
    }
    
    def fix_type_reference(type_ref: str) -> str:
        """Fix a single type reference to use the no-namespace pattern."""
        if not type_ref:
            return type_ref
            
        # If already has xs: prefix and it's a built-in type, keep it
        if type_ref.startswith('xs:'):
            return type_ref
            
        # Remove any tns: prefix
        if type_ref.startswith('tns:'):
            type_ref = type_ref[4:]  # Remove 'tns:' prefix
        
        # Determine correct pattern based on type
        if type_ref in builtin_types:
            return f'xs:{type_ref}'
        elif type_ref in local_types:
            # Local types use no prefix in no-namespace pattern
            return type_ref
        else:
            # Unknown type - check if it looks like a built-in type
            if any(builtin in type_ref.lower() for builtin in ['string', 'decimal', 'int', 'date', 'time', 'boolean']):
                return f'xs:{type_ref}'
            else:
                # Assume it's a local type and use no prefix
                return type_ref
    
    # Fix type attributes
    if 'type' in element.attrib:
        old_ref = element.attrib['type']
        new_ref = fix_type_reference(old_ref)
        if old_ref != new_ref:
            element.attrib['type'] = new_ref
    
    # Fix base attributes  
    if 'base' in element.attrib:
        old_ref = element.attrib['base']
        new_ref = fix_type_reference(old_ref)
        if old_ref != new_ref:
            element.attrib['base'] = new_ref
    
    # Recursively process all children
    for child in element:
        _update_type_references_no_namespace(child, local_types)


def _count_prefixed_type_references(element: ET.Element) -> int:
    """Count the number of prefixed type references (tns:, etc.) in an element."""
    count = 0
    
    for elem in element.iter():
        for attr_name in ['type', 'base']:
            if attr_name in elem.attrib:
                type_ref = elem.attrib[attr_name]
                if ':' in type_ref and not type_ref.startswith('xs:'):
                    count += 1
    
    return count


def _save_xsd_file(root: ET.Element, output_path: str):
    """
    Save XSD file using the no-namespace pattern without adding namespace prefixes.
    
    Preserves the no-namespace pattern by not adding target namespace declarations
    or tns: prefixes, keeping only xs: prefix for XSD built-ins.
    """
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        rough_string = ET.tostring(root, encoding='unicode')
        
        # Check for and fix duplicate xmlns:xs attributes
        if 'xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xs="http://www.w3.org/2001/XMLSchema"' in rough_string:
            rough_string = rough_string.replace(
                'xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xs="http://www.w3.org/2001/XMLSchema"',
                'xmlns:xs="http://www.w3.org/2001/XMLSchema"'
            )
        
        # Try pretty formatting with minidom
        try:
            dom = minidom.parseString(rough_string)
            pretty_xml = dom.toprettyxml(indent="  ")
            lines = []
            for line in pretty_xml.split('\n'):
                if line.strip() and not line.strip().startswith('<?xml'):
                    lines.append(line)
            formatted_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + '\n'.join(lines)
        except Exception as e:
            # Manual basic formatting 
            formatted_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + rough_string.replace('><', '>\n<')

        # Write the file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(formatted_xml)
        
    except Exception as e:
        print(f"Error saving no-namespace XSD: {e}")
        raise
