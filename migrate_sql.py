import re
import sys
import os

# Set this to True if you want to import into an existing table
MERGE_MODE = True 

def transform_siteid(old_siteid):
    """
    Logic:
    - 10108 -> siteid: 10108, baseline_id: 10108-1
    - 10108-1 -> siteid: 10108, baseline_id: 10108-2
    """
    old_siteid = old_siteid.strip("'\"")
    if '-' in old_siteid:
        # It's a child url
        parts = old_siteid.split('-')
        prefix = parts[0]
        try:
            suffix = int(parts[1])
            new_siteid = prefix
            new_baseline_id = f"{prefix}-{suffix + 1}"
        except ValueError:
            # Fallback if suffix is not a number
            new_siteid = prefix
            new_baseline_id = f"{old_siteid}-1"
    else:
        # It's a parent domain
        new_siteid = old_siteid
        new_baseline_id = f"{old_siteid}-1"
    
    return f"'{new_siteid}'", f"'{new_baseline_id}'"

def split_sql_row(row_content):
    """
    Splits row content by comma, correctly handling quoted strings and escaped backslashes.
    """
    values = []
    current = []
    in_string = False
    quote_char = None
    escaped = False
    
    i = 0
    while i < len(row_content):
        char = row_content[i]
        if escaped:
            current.append(char)
            escaped = False
        elif char == '\\':
            current.append(char)
            escaped = True
        elif char in ("'", '"'):
            if not in_string:
                in_string = True
                quote_char = char
                current.append(char)
            elif char == quote_char:
                # Handle double-quote escape ''
                if i + 1 < len(row_content) and row_content[i+1] == quote_char:
                    current.append(char)
                    current.append(char)
                    i += 1
                else:
                    in_string = False
                    quote_char = None
                    current.append(char)
            else:
                current.append(char)
        elif char == ',' and not in_string:
            values.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        i += 1
            
    values.append("".join(current).strip())
    return values

def extract_rows(values_content):
    """
    Extracts individual (row1), (row2) blocks, correctly handling parens in strings.
    """
    rows = []
    current_row = []
    paren_depth = 0
    in_string = False
    quote_char = None
    escaped = False
    
    i = 0
    while i < len(values_content):
        char = values_content[i]
        if escaped:
            current_row.append(char)
            escaped = False
        elif char == '\\':
            current_row.append(char)
            escaped = True
        elif char in ("'", '"'):
            if not in_string:
                in_string = True
                quote_char = char
            elif char == quote_char:
                # Handle double-quote escape ''
                if i + 1 < len(values_content) and values_content[i+1] == quote_char:
                    current_row.append(char)
                    current_row.append(char)
                    i += 1
                else:
                    in_string = False
                    quote_char = None
            current_row.append(char)
        elif char == '(' and not in_string:
            if paren_depth == 0:
                current_row = []
            current_row.append(char)
            paren_depth += 1
        elif char == ')' and not in_string:
            paren_depth -= 1
            current_row.append(char)
            if paren_depth == 0:
                rows.append("".join(current_row))
                current_row = []
        elif paren_depth > 0:
            current_row.append(char)
        i += 1
            
    return rows

def split_sql_statements(content):
    """
    Splits SQL content into statements, correctly handling semicolons in strings.
    """
    statements = []
    current_stmt = []
    in_string = False
    quote_char = None
    escaped = False
    
    i = 0
    while i < len(content):
        char = content[i]
        if escaped:
            current_stmt.append(char)
            escaped = False
        elif char == '\\':
            current_stmt.append(char)
            escaped = True
        elif char in ("'", '"'):
            if not in_string:
                in_string = True
                quote_char = char
            elif char == quote_char:
                if i + 1 < len(content) and content[i+1] == quote_char:
                    current_stmt.append(char)
                    current_stmt.append(char)
                    i += 1
                else:
                    in_string = False
                    quote_char = None
            current_stmt.append(char)
        elif char == ';' and not in_string:
            current_stmt.append(char)
            statements.append("".join(current_stmt))
            current_stmt = []
        else:
            current_stmt.append(char)
        i += 1
    
    if current_stmt:
        statements.append("".join(current_stmt))
    
    return statements

def migrate_sql(input_file, output_file):
    print(f"Migrating {input_file} to {output_file} (Merge Mode: {MERGE_MODE})...")
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    processed_statements = []

    if MERGE_MODE:
        # 1. Ensure column exists
        alter_stmt = "ALTER TABLE `defacement_sites` ADD COLUMN IF NOT EXISTS `baseline_id` varchar(100) DEFAULT NULL AFTER `siteid`;\n"
        processed_statements.append(alter_stmt)

    # 2. Split into statements and process
    statements = split_sql_statements(content)
    
    for stmt in statements:
        if MERGE_MODE:
            # Skip table drop/create
            if re.search(r"CREATE TABLE `defacement_sites`|DROP TABLE IF EXISTS `defacement_sites`|TRUNCATE TABLE `defacement_sites`", stmt, flags=re.IGNORECASE):
                continue
            
            # Skip index modifications
            if re.search(r"ALTER TABLE `defacement_sites`[\s\n]+ADD (?:PRIMARY KEY|UNIQUE KEY|KEY|FULLTEXT KEY|CONSTRAINT)", stmt, flags=re.IGNORECASE):
                continue
            
            # Skip Auto-Increment settings
            if re.search(r"ALTER TABLE `defacement_sites`[\s\n]+MODIFY `id` .* AUTO_INCREMENT=.*", stmt, flags=re.IGNORECASE):
                continue

        # Process INSERT
        if re.search(r"INSERT INTO `defacement_sites`", stmt, flags=re.IGNORECASE):
            # Update Column List
            if MERGE_MODE:
                # 1. Remove `id` from column list for safe merging
                stmt = re.sub(r"INSERT INTO `defacement_sites` \(`id`, ", r"INSERT INTO `defacement_sites` (", stmt, flags=re.IGNORECASE)
            
            # 2. Add baseline_id to column list
            insert_head_pattern = r"(INSERT INTO `defacement_sites` \([^)]*`siteid`)((?:, [^)]*)?\))"
            stmt = re.sub(insert_head_pattern, r"\1, `baseline_id`\2", stmt, flags=re.IGNORECASE)
            
            # Process VALUES
            match = re.search(r"(.*?VALUES)(.*);", stmt, flags=re.IGNORECASE | re.DOTALL)
            if match:
                header = match.group(1)
                values_str = match.group(2)
                
                rows = extract_rows(values_str)
                processed_rows = []
                for row in rows:
                    inner_content = row[1:-1]
                    values = split_sql_row(inner_content)
                    
                    if MERGE_MODE and len(values) > 0:
                        # Strip original ID (index 0)
                        values.pop(0)
                        
                        # Indices shifted: siteid is now at 6
                        siteid_idx = 6
                    else:
                        siteid_idx = 7

                    if len(values) > siteid_idx:
                        old_siteid = values[siteid_idx].strip()
                        new_siteid_val, new_baseline_id_val = transform_siteid(old_siteid)
                        
                        values[siteid_idx] = " " + new_siteid_val
                        values.insert(siteid_idx + 1, " " + new_baseline_id_val)
                        processed_rows.append("(" + ",".join(values) + ")")
                    else:
                        processed_rows.append(row)
                
                # Combine rows and add ON DUPLICATE KEY UPDATE for Merge Mode
                stmt = header + "\n" + ",\n".join(processed_rows)
                if MERGE_MODE:
                    # Specifically only update siteid and baseline_id if row already exists
                    # We also set timestamp = timestamp to prevent the ON UPDATE CURRENT_TIMESTAMP from firing
                    stmt += "\nON DUPLICATE KEY UPDATE `siteid` = VALUES(`siteid`), `baseline_id` = VALUES(`baseline_id`), `timestamp` = `timestamp`;"
                else:
                    stmt += ";"
        
        elif not MERGE_MODE:
            # Fresh mode: update CREATE TABLE
            table_pattern = r"(`siteid` varchar\(100\) NOT NULL,)"
            stmt = re.sub(table_pattern, r"\1\n  `baseline_id` varchar(100) DEFAULT NULL,", stmt)

        processed_statements.append(stmt)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("".join(processed_statements))
    
    print("Migration complete!")

if __name__ == "__main__":
    input_path = "/home/priti/Web-Crawler/Web_Crawler/defacement_sites (1) (1).sql"
    output_path = "/home/priti/Web-Crawler/Web_Crawler/defacement_sites_migrated.sql"
    
    if os.path.exists(input_path):
        migrate_sql(input_path, output_path)
    else:
        print(f"Error: No file found at {input_path}")
