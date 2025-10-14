#!/usr/bin/env python3
"""
Utility to split markdown files into navigable sections based on headings.
"""

import re
import markdown
from typing import List, Dict


def split_markdown_into_sections(md_content: str, split_level: int = 2) -> Dict[str, Dict]:
    """
    Split markdown content into sections based on heading levels.
    
    Args:
        md_content: Raw markdown content
        split_level: Heading level to split on (1 for H1, 2 for H2, etc.)
    
    Returns:
        Dictionary of sections with metadata:
        {
            'section_id': {
                'title': 'Section Title',
                'content': '<html>content</html>',
                'order': 1,
                'level': 2
            }
        }
    """
    sections = {}
    
    # Pattern to match headings at the specified level
    # Matches: ## Title, ### Title, etc.
    heading_pattern = r'^(#{' + str(split_level) + r'})\s+(.+)$'
    
    lines = md_content.split('\n')
    current_section = None
    current_lines = []
    section_order = 0
    
    for line in lines:
        match = re.match(heading_pattern, line, re.MULTILINE)
        
        if match:
            # Save previous section if exists
            if current_section and current_lines:
                section_content = '\n'.join(current_lines)
                sections[current_section['id']]['content'] = markdown.markdown(
                    section_content,
                    extensions=['extra', 'codehilite', 'tables', 'fenced_code']
                )
            
            # Start new section
            section_order += 1
            hashes = match.group(1)
            title = match.group(2).strip()
            section_id = _create_section_id(title)
            
            current_section = {
                'id': section_id,
                'title': title,
                'level': len(hashes),
                'order': section_order
            }
            
            sections[section_id] = current_section.copy()
            sections[section_id]['content'] = ''
            current_lines = [line]  # Include the heading in the section
        else:
            current_lines.append(line)
    
    # Save last section
    if current_section and current_lines:
        section_content = '\n'.join(current_lines)
        sections[current_section['id']]['content'] = markdown.markdown(
            section_content,
            extensions=['extra', 'codehilite', 'tables', 'fenced_code']
        )
    
    return sections


def _create_section_id(title: str) -> str:
    """Create a URL-safe section ID from a title."""
    # Convert to lowercase, replace spaces with hyphens
    section_id = title.lower()
    section_id = re.sub(r'[^\w\s-]', '', section_id)
    section_id = re.sub(r'[-\s]+', '-', section_id)
    return section_id.strip('-')


def get_markdown_sections(filepath: str, split_level: int = 2) -> Dict[str, Dict]:
    """
    Read a markdown file and split it into sections.
    
    Args:
        filepath: Path to markdown file
        split_level: Heading level to split on (1 for H1, 2 for H2)
    
    Returns:
        Dictionary of sections
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    return split_markdown_into_sections(content, split_level)


def create_section_navigation(sections: Dict[str, Dict], parent_title: str) -> List[Dict]:
    """
    Create navigation items for sections.
    
    Args:
        sections: Dictionary of sections
        parent_title: Title of the parent document (e.g., "README")
    
    Returns:
        List of navigation items sorted by order
    """
    nav_items = []
    
    for section_id, section_data in sorted(
        sections.items(), 
        key=lambda x: x[1]['order']
    ):
        nav_items.append({
            'id': section_id,
            'title': section_data['title'],
            'parent': parent_title,
            'level': section_data['level'],
            'icon': _get_icon_for_level(section_data['level'])
        })
    
    return nav_items


def _get_icon_for_level(level: int) -> str:
    """Get emoji icon for heading level."""
    icons = {
        1: '📖',
        2: '📄',
        3: '📝',
        4: '•',
    }
    return icons.get(level, '•')


if __name__ == '__main__':
    import sys
    import json
    
    if len(sys.argv) < 2:
        print("Usage: python split_markdown.py <markdown_file> [split_level]")
        sys.exit(1)
    
    filepath = sys.argv[1]
    split_level = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    
    sections = get_markdown_sections(filepath, split_level)
    
    # Output as JSON
    output = {
        'filepath': filepath,
        'split_level': split_level,
        'sections': sections,
        'navigation': create_section_navigation(sections, filepath)
    }
    
    print(json.dumps(output, indent=2))

