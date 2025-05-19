import json
import mysql.connector
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import re

# --- Database Connection Details ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sql@#&50490',
    'database': 'semantic_catalog_db'
}

# --- LLM Configuration (same as llm_enrichment.py and search_api.py) ---
LLM_MODEL_NAME = 'gemma-3-4b-it-qat' # Your model in LM Studio
LLM_BASE_URL = 'http://127.0.0.1:1234/v1' # LM Studio OpenAI-compatible endpoint

# --- Prompt Template for Relationship Inference ---
RELATIONSHIP_INFERENCE_PROMPT_TEMPLATE = """
You are a database schema analysis expert. Based on the following database schema, identify potential relationships between tables and columns.
Consider relationships like:
- Potential Foreign Keys (e.g., a column in one table seems to refer to a primary key in another table, even if not explicitly defined).
- Semantic Similarity (e.g., columns with different names but similar meaning or data patterns).

Schema:
{schema_details}

Please provide your inferred relationships as a JSON list of objects. Each object should have the following structure:
{{
  "source_table": "table_name_1",
  "source_column": "column_name_1",
  "target_table": "table_name_2",
  "target_column": "column_name_2",
  "relationship_type": "description of relationship (e.g., 'potential foreign key: Orders.customer_id -> Customers.customer_id', 'semantic similarity: Products.prod_name -> Order_Items.item_name')",
  "justification": "your reasoning for this inference"
}}

If no relationships are inferred, return an empty JSON list [].
Ensure the output is ONLY the JSON list, with no other text before or after it.

Inferred Relationships (JSON List):
"""

def load_extracted_metadata(filepath="extracted_metadata.json"):
    """Loads the extracted metadata from the JSON file."""
    try:
        with open(filepath, 'r') as f:
            metadata = json.load(f)
        print(f"Successfully loaded metadata from {filepath}")
        return metadata
    except FileNotFoundError:
        print(f"Error: Metadata file not found at {filepath}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {filepath}")
        return None

def format_schema_for_llm(metadata):
    """Formats the schema details from metadata for the LLM prompt."""
    if not metadata or 'tables' not in metadata:
        return "No schema details available."

    schema_str = "Tables and Columns:\\n"
    for table_name, table_details in metadata['tables'].items():
        schema_str += f"- Table: {table_name}\\n"
        if 'columns' in table_details and isinstance(table_details['columns'], list):
            for col_data in table_details['columns']: # Iterate over list of column dicts
                column_name = col_data.get('name', 'UNKNOWN_COLUMN')
                col_type = col_data.get('type', 'UNKNOWN_TYPE')
                schema_str += f"  - Column: {column_name} (Type: {col_type})\\n"
        elif 'columns' in table_details and isinstance(table_details['columns'], dict):
            # Fallback for dictionary structure, though the error indicates it's a list
            for column_name, col_details_dict in table_details['columns'].items():
                col_type = col_details_dict.get('type', 'UNKNOWN_TYPE')
                schema_str += f"  - Column: {column_name} (Type: {col_type})\\n"
        schema_str += "\\n"
    
    # Add information about existing foreign keys if available
    # This helps the LLM avoid suggesting already defined FKs as "new"
    fk_str = "Existing Foreign Keys (for context, do not re-suggest these as new potential FKs unless there's a different semantic link):\\n"
    has_fks = False
    for table_name, table_details in metadata['tables'].items():
        if 'foreign_keys' in table_details and table_details['foreign_keys']:
            has_fks = True
            fk_str += f"- Table: {table_name}\\n"
            for fk in table_details['foreign_keys']:
                fk_str += f"  - FK Name: {fk['constraint_name']}\\n"
                fk_str += f"    Column: {fk['column_name']}\\n" # Adjusted to use 'column_name'
                fk_str += f"    References: {fk['references_table']}({fk['references_column']})\\n" # Adjusted to use 'references_table' and 'references_column'
    if not has_fks:
        fk_str += "None explicitly defined in metadata.\\n"
    
    return schema_str + "\\n" + fk_str


def store_inferred_relationships(relationships):
    """Stores the inferred relationships in the database."""
    if not relationships:
        print("No relationships to store.")
        return

    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO inferred_relationships 
            (source_table, source_column, target_table, target_column, relationship_type, justification, llm_model_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            relationship_type = VALUES(relationship_type), 
            justification = VALUES(justification),
            llm_model_version = VALUES(llm_model_version)
        """
        
        stored_count = 0
        for rel in relationships:
            # Basic validation
            if not all(k in rel for k in ["source_table", "source_column", "target_table", "target_column", "relationship_type", "justification"]):
                print(f"Skipping invalid relationship object: {rel}")
                continue

            cursor.execute(insert_query, (
                rel['source_table'], rel['source_column'],
                rel['target_table'], rel['target_column'],
                rel['relationship_type'], rel['justification'],
                LLM_MODEL_NAME 
            ))
            stored_count += 1
        
        conn.commit()
        print(f"Successfully stored/updated {stored_count} inferred relationships in the database.")

    except mysql.connector.Error as err:
        print(f"Database error while storing relationships: {err}")
    except Exception as e:
        print(f"An unexpected error occurred during storage: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def parse_llm_json_output(llm_output_str):
    """Parses the LLM output string to extract the JSON list."""
    # Try to find JSON list within ```json ... ``` or just the list
    match = re.search(r'```json\s*([\s\S]*?)\s*```|(\[[\s\S]*\])', llm_output_str, re.DOTALL)
    if match:
        json_str = match.group(1) or match.group(2)
        if json_str:
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                print(f"JSON Decode Error: {e}")
                print(f"Problematic JSON string: {json_str}")
                return None
    print("Could not find a valid JSON list in the LLM output.")
    print(f"LLM Raw Output was: {llm_output_str}")
    return None


def main():
    print("Starting relationship inference process...")
    
    # 1. Load metadata
    metadata = load_extracted_metadata()
    if not metadata:
        return
        
    # 2. Format schema for LLM
    schema_details_for_prompt = format_schema_for_llm(metadata)
    # print(f"Schema for LLM:\\n{schema_details_for_prompt}") # For debugging

    # 3. Initialize LLM
    try:
        llm = ChatOpenAI(
            model=LLM_MODEL_NAME,
            base_url=LLM_BASE_URL,
            api_key="not-needed", 
            temperature=0.2 # Slightly higher for creative inference but still structured
        )
        print(f"LLM ({LLM_MODEL_NAME}) initialized successfully for relationship inference.")
    except Exception as e:
        print(f"Error initializing LLM: {e}")
        return

    # 4. Create LangChain chain and invoke
    prompt = ChatPromptTemplate.from_template(RELATIONSHIP_INFERENCE_PROMPT_TEMPLATE)
    chain = prompt | llm | StrOutputParser()
    
    print("Invoking LLM for relationship inference (this may take a moment)...")
    try:
        llm_response_str = chain.invoke({"schema_details": schema_details_for_prompt})
        print(f"LLM raw response:\\n{llm_response_str}")
    except Exception as e:
        print(f"Error invoking LLM chain: {e}")
        return

    # 5. Parse LLM output
    inferred_relationships = parse_llm_json_output(llm_response_str)
    
    if inferred_relationships is not None:
        print(f"Successfully parsed {len(inferred_relationships)} potential relationships from LLM response.")
        # 6. Store relationships
        store_inferred_relationships(inferred_relationships)
    else:
        print("Could not parse relationships from LLM response. No relationships will be stored.")

    print("Relationship inference process finished.")

if __name__ == "__main__":
    main()
