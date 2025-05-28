import mysql.connector
import json
import time
from langchain_openai import ChatOpenAI # Changed from ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# --- Database Connection Details (same as other scripts) ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',      # Your MySQL username
    'password': '', # Your MySQL password
    'database': 'semantic_catalog_db'
}

# --- LLM Configuration ---
# IMPORTANT: Replace 'local-model' with the model identifier if your LM Studio expects one.
# Often, for local OpenAI-compatible servers, this can be a descriptive name or even arbitrary.
LLM_MODEL_NAME = 'gemma-3-4b-it-qat' # Or your specific model in LM Studio
LLM_BASE_URL = 'http://127.0.0.1:1234/v1' # LM Studio OpenAI-compatible endpoint

METADATA_FILE_PATH = "extracted_metadata.json"

# --- Prompt Templates ---
TABLE_PROMPT_TEMPLATE = """
You are a helpful data catalog assistant. Generate a concise, human-readable semantic description 
and relevant tags (as a comma-separated list of single words or short phrases, e.g., customer_info, sales_data, product_details) 
for the following database table. Focus on its purpose and the type of information it stores.

Table Name: {table_name}
Columns (Name, Type, Is_Nullable, Is_Primary_Key, Extra):
{columns_details}
Sample Data (first few rows):
{sample_data}

Respond in the following format, and nothing else:
Description: [Your generated description here]
Tags: [tag1, tag2, tag3]
"""

COLUMN_PROMPT_TEMPLATE = """
You are a helpful data catalog assistant. Generate a concise, human-readable semantic description 
and relevant tags (as a comma-separated list of single words or short phrases) 
for the following database column. Explain what specific information it holds within its table.

Table Name: {table_name}
Column Name: {column_name}
Column Type: {column_type_full}
Column is Nullable: {is_nullable}
Column is Primary Key: {is_primary_key}
Context (other columns in table): {other_column_names}
Sample Data from this column (first few values):
{sample_column_values}

Respond in the following format, and nothing else:
Description: [Your generated description here]
Tags: [tag1, tag2, tag3]
"""

def get_llm_instance():
    """Initializes and returns the LangChain LLM instance for LM Studio."""
    try:
        llm = ChatOpenAI(
            model=LLM_MODEL_NAME,
            base_url=LLM_BASE_URL,
            api_key="not-needed"  # LM Studio typically doesn't require an API key
        )
        # Perform a simple test invocation
        print(f"Attempting to connect to LLM: {LLM_MODEL_NAME} at {LLM_BASE_URL}...")
        llm.invoke("Hello!") 
        print("LLM connection successful.")
        return llm
    except Exception as e:
        print(f"Error initializing or connecting to LLM: {e}")
        print("Please ensure your LM Studio is running, a model is loaded, and the server is started.")
        print("Also, verify the Base URL and Model Name in this script.")
        return None

def parse_llm_output(text_output):
    """Parses the LLM's text output to extract description and tags."""
    description = ""
    tags = []
    try:
        desc_line = next(line for line in text_output.split('\n') if line.startswith("Description:"))
        description = desc_line.replace("Description:", "").strip()
    except StopIteration:
        print("Warning: Could not parse description from LLM output.")

    try:
        tags_line = next(line for line in text_output.split('\n') if line.startswith("Tags:"))
        tags_str = tags_line.replace("Tags:", "").strip()
        if tags_str:
            tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
    except StopIteration:
        print("Warning: Could not parse tags from LLM output.")
    
    if not description and not tags and text_output: # Fallback if parsing fails but got output
        print("Warning: Using full LLM output as description due to parsing error.")
        description = text_output.strip()

    return description, tags


def store_enriched_metadata(object_type, object_name, parent_table_name, tech_metadata, semantic_desc, tags_list):
    """Stores the enriched metadata into the database."""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        sql = """
            INSERT INTO enriched_metadata 
            (object_type, object_name, parent_table_name, technical_metadata, semantic_description, tags, llm_model_used)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            technical_metadata = VALUES(technical_metadata),
            semantic_description = VALUES(semantic_description),
            tags = VALUES(tags),
            llm_model_used = VALUES(llm_model_used),
            generated_at = CURRENT_TIMESTAMP;
        """
        # Convert tech_metadata and tags_list to JSON strings for storage
        tech_metadata_json = json.dumps(tech_metadata)
        tags_json = json.dumps(tags_list)

        cursor.execute(sql, (
            object_type, object_name, parent_table_name, 
            tech_metadata_json, semantic_desc, tags_json, LLM_MODEL_NAME
        ))
        conn.commit()
        print(f"Stored/Updated enriched metadata for: {object_type} - {object_name}")

    except mysql.connector.Error as err:
        print(f"Database error while storing enriched metadata for {object_name}: {err}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Unexpected error storing enriched metadata for {object_name}: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def process_table_metadata(llm, table_name, table_data):
    """Generates and stores enriched metadata for a single table."""
    if table_name in ['enriched_metadata', 'inferred_relationships']:
        print(f"Skipping LLM enrichment for metadata table: {table_name}")
        return
    print(f"\nProcessing table: {table_name}...")
    
    column_details_for_prompt = []
    for col in table_data.get('columns', []):
        details = f"  - {col.get('name','N/A')} (Type: {col.get('column_type','N/A')}, Nullable: {col.get('is_nullable','N/A')}, PK: {col.get('is_primary_key','N/A')}, Extra: {col.get('extra','N/A')})"
        column_details_for_prompt.append(details)

    prompt_input = {
        "table_name": table_name,
        "columns_details": "\n".join(column_details_for_prompt),
        "sample_data": json.dumps(table_data.get('sample_data', []), indent=2)
    }
    
    table_prompt = ChatPromptTemplate.from_template(TABLE_PROMPT_TEMPLATE)
    chain = table_prompt | llm | StrOutputParser()
    
    try:
        print(f"Invoking LLM for table: {table_name}...")
        raw_llm_output = chain.invoke(prompt_input)
        # print(f"LLM Raw Output for table {table_name}:\n{raw_llm_output}") # For debugging
        description, tags = parse_llm_output(raw_llm_output)
        
        if description or tags:
            store_enriched_metadata(
                object_type='table',
                object_name=table_name,
                parent_table_name=None,
                tech_metadata=table_data, # Store all technical details for the table
                semantic_desc=description,
                tags_list=tags
            )
        else:
            print(f"Skipping storage for table {table_name} due to empty description and tags.")

    except Exception as e:
        print(f"Error processing table {table_name} with LLM: {e}")

    # Small delay to avoid overwhelming the local LLM or hitting rate limits if any
    time.sleep(2) 

def process_column_metadata(llm, table_name, column_data, all_column_names, table_sample_data):
    """Generates and stores enriched metadata for a single column."""
    column_name = column_data.get('name', 'N/A')
    if column_name == 'embedding_vector':
        print(f"Skipping LLM enrichment for embedding column: {table_name}.{column_name}")
        return
    print(f"  Processing column: {table_name}.{column_name}...")

    sample_column_values = []
    if table_sample_data:
        for row in table_sample_data:
            if isinstance(row, dict) and column_name in row:
                sample_column_values.append(row[column_name])
            elif isinstance(row, list) and len(row) > all_column_names.index(column_name):
                 # Fallback if sample_data isn't dicts (should be from extractor)
                sample_column_values.append(row[all_column_names.index(column_name)])

    prompt_input = {
        "table_name": table_name,
        "column_name": column_name,
        "column_type_full": column_data.get('column_type', 'N/A'),
        "is_nullable": column_data.get('is_nullable', 'N/A'),
        "is_primary_key": column_data.get('is_primary_key', 'N/A'),
        "other_column_names": ", ".join([c for c in all_column_names if c != column_name]),
        "sample_column_values": json.dumps(sample_column_values[:5], indent=2) # First 5 sample values for this col
    }

    column_prompt = ChatPromptTemplate.from_template(COLUMN_PROMPT_TEMPLATE)
    chain = column_prompt | llm | StrOutputParser()

    try:
        print(f"  Invoking LLM for column: {table_name}.{column_name}...")
        raw_llm_output = chain.invoke(prompt_input)
        # print(f"LLM Raw Output for column {table_name}.{column_name}:\n{raw_llm_output}") # For debugging
        description, tags = parse_llm_output(raw_llm_output)

        if description or tags:
            store_enriched_metadata(
                object_type='column',
                object_name=column_name,
                parent_table_name=table_name,
                tech_metadata=column_data, # Store all technical details for this column
                semantic_desc=description,
                tags_list=tags
            )
        else:
            print(f"Skipping storage for column {table_name}.{column_name} due to empty description and tags.")
            
    except Exception as e:
        print(f"Error processing column {table_name}.{column_name} with LLM: {e}")
    
    time.sleep(1) # Shorter delay for columns

def main():
    print("Starting LLM enrichment process...")
    llm = get_llm_instance()
    if not llm:
        print("LLM not available. Exiting.")
        return

    try:
        with open(METADATA_FILE_PATH, 'r') as f:
            technical_metadata = json.load(f)
    except FileNotFoundError:
        print(f"Error: Metadata file '{METADATA_FILE_PATH}' not found.")
        print("Please run metadata_extractor.py first and ensure the JSON output is saved.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{METADATA_FILE_PATH}'.")
        return

    if not technical_metadata or 'tables' not in technical_metadata:
        print("No table data found in metadata file. Exiting.")
        return

    for table_name, table_data in technical_metadata.get('tables', {}).items():
        process_table_metadata(llm, table_name, table_data)
        
        all_column_names_in_table = [col.get('name','') for col in table_data.get('columns', [])]
        table_sample_data = table_data.get('sample_data', [])
        for column_data in table_data.get('columns', []):
            process_column_metadata(llm, table_name, column_data, all_column_names_in_table, table_sample_data)
            
    print("\nLLM enrichment process finished.")

if __name__ == "__main__":
    main()
