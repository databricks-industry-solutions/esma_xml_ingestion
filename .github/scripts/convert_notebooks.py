#!/usr/bin/env python3

import os
import re
import markdown
import glob
import html


def parse_databricks_notebook(filepath):
    """Parse a Databricks .py notebook format into cells"""
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Split by COMMAND ----------
    sections = re.split(r'# COMMAND ----------', content)
    cells = []
    
    for section in sections:
        if not section.strip():
            continue
            
        # Check if this is a markdown cell
        if '# MAGIC %md' in section:
            # Extract markdown content
            lines = section.split('\n')
            md_lines = []
            for line in lines:
                if line.startswith('# MAGIC %md'):
                    # Remove '# MAGIC %md'
                    md_lines.append(line[11:].strip())
                elif line.startswith('# MAGIC '):
                    # Remove '# MAGIC '
                    md_lines.append(line[8:])
                elif line.startswith('# MAGIC'):
                    # Remove '# MAGIC'
                    md_lines.append(line[7:])
            
            md_content = '\n'.join(md_lines)
            cells.append({'type': 'markdown', 'content': md_content})
        elif '# MAGIC %scala' in section:
            # Extract Scala code content
            lines = section.split('\n')
            scala_lines = []
            for line in lines:
                if line.startswith('# MAGIC %scala'):
                    continue
                elif line.startswith('# MAGIC '):
                    # Remove '# MAGIC '
                    scala_lines.append(line[8:])
                elif line.startswith('# MAGIC'):
                    # Remove '# MAGIC'
                    scala_lines.append(line[7:])
            
            scala_content = '\n'.join(scala_lines).strip()
            if scala_content:
                cells.append({'type': 'scala', 'content': scala_content})
        else:
            # This is a code cell
            # Remove any leading comments that aren't actual code
            lines = section.split('\n')
            code_lines = []
            for line in lines:
                if not line.startswith('# DBTITLE'):
                    code_lines.append(line)
            
            code_content = '\n'.join(code_lines).strip()
            if code_content:
                cells.append({'type': 'code', 'content': code_content})
    
    return cells


def extract_headers_from_markdown(md_content):
    """Extract H2 and H3 headers from markdown content for navigation"""
    headers = []
    for line in md_content.split('\n'):
        line = line.strip()
        if line.startswith('## ') and not line.startswith('### '):
            text = line[3:].strip()
            # Create URL-safe ID
            header_id = re.sub(r'[^\w\s-]', '', text.lower())
            header_id = re.sub(r'[-\s]+', '-', header_id).strip('-')
            headers.append({
                'level': 2,
                'text': text,
                'id': header_id
            })
        elif line.startswith('### '):
            text = line[4:].strip()
            # Create URL-safe ID
            header_id = re.sub(r'[^\w\s-]', '', text.lower())
            header_id = re.sub(r'[-\s]+', '-', header_id).strip('-')
            headers.append({
                'level': 3,
                'text': text,
                'id': header_id
            })
    return headers


def convert_to_html_fragment(filepath):
    """Convert Databricks .py notebook to HTML fragment with syntax highlighting"""
    filename = os.path.basename(filepath)
    name_without_ext = os.path.splitext(filename)[0]
    
    cells = parse_databricks_notebook(filepath)
    html_content = []
    all_headers = []
    
    for i, cell in enumerate(cells):
        if cell['type'] == 'markdown':
            # Extract headers for navigation
            headers = extract_headers_from_markdown(cell['content'])
            all_headers.extend(headers)
            
            # Convert markdown to HTML with header IDs
            md_html = markdown.markdown(
                cell['content'], 
                extensions=['fenced_code', 'tables', 'nl2br', 'toc', 'attr_list']
            )
            html_content.append(f'''<div class="cell border-box-sizing text_cell rendered">
<div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
{md_html}
</div>
</div>
</div>''')
        elif cell['type'] == 'code':
            # Create code cell with proper syntax highlighting for Python
            escaped_code = html.escape(cell['content'])
            html_content.append(f'''<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="inner_cell">
<div class="input_area">
<div class="highlight hl-ipython3">
<pre class="language-python"><code class="language-python">{escaped_code}</code></pre>
</div>
</div>
</div>
</div>
</div>''')
        elif cell['type'] == 'scala':
            # Create Scala code cell with proper syntax highlighting
            escaped_code = html.escape(cell['content'])
            html_content.append(f'''<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="inner_cell">
<div class="input_area">
<div class="highlight hl-scala">
<pre class="language-scala"><code class="language-scala">{escaped_code}</code></pre>
</div>
</div>
</div>
</div>
</div>''')
    
    # Return just the content fragment (no full HTML document)
    fragment_content = '\n'.join(html_content)
    
    # Write fragment to temp file for the main script to read
    temp_path = f"temp_{name_without_ext}_fragment.html"
    with open(temp_path, 'w') as f:
        f.write(fragment_content)
    
    return name_without_ext, fragment_content, all_headers


if __name__ == "__main__":
    # Process all .py files in src directory (your notebooks are there)
    notebook_data = {}
    notebook_headers = {}
    for py_file in glob.glob('src/*.py'):
        if not py_file.endswith('__init__.py'):  # Skip __init__.py files
            name, fragment, headers = convert_to_html_fragment(py_file)
            notebook_data[name] = fragment
            notebook_headers[name] = headers
            print(f"Converted {py_file} to HTML fragment with {len(headers)} headers")
    
    # Write notebook data to JSON files for the main script
    import json
    with open('notebook_fragments.json', 'w') as f:
        json.dump(notebook_data, f)
    with open('notebook_headers.json', 'w') as f:
        json.dump(notebook_headers, f)