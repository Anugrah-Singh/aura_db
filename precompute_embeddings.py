import mysql.connector
import json
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss # Though not strictly for storing, good to have consistent imports

# --- Database Connection Details ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'semantic_catalog_db'
}

# --- Initialize Sentence Transformer Model ---
MODEL_NAME = 'all-MiniLM-L6-v2'
model = SentenceTransformer(MODEL_NAME)
print(f"Sentence Transformer model '{MODEL_NAME}' loaded.")

def get_all_enriched_data_for_embedding():
    """Fetches id and semantic_description from enriched_metadata for items needing embedding."""
    conn = None
    items_to_embed = []
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Select items that don't have an embedding yet or where it might need update
        # For simplicity, let's assume a column 'embedding_vector' will store the blob
        # And we add a new column 'embedding_model_version'
        # This example will try to embed all for now if 'embedding_vector' is NULL
        # Modify your enriched_metadata table to include:
        # embedding_vector BLOB,
        # embedding_model_version VARCHAR(255)
        
        # Check if 'embedding_vector' column exists, if not, this script won't update.
        # For this script, we assume it exists.
        
        cursor.execute("""
            SELECT id, semantic_description 
            FROM enriched_metadata 
            WHERE semantic_description IS NOT NULL AND TRIM(semantic_description) <> ''
            AND (embedding_vector IS NULL OR embedding_model_version != %s) 
        """, (MODEL_NAME,)) # Only process if embedding is NULL or model changed
        
        items = cursor.fetchall()
        
        for item in items:
            if item['semantic_description'] and item['semantic_description'].strip():
                items_to_embed.append(item)
        
        print(f"Found {len(items_to_embed)} items to embed/re-embed.")
        return items_to_embed
        
    except mysql.connector.Error as err:
        print(f"Database error in get_all_enriched_data_for_embedding: {err}")
        return []
    except Exception as e:
        print(f"Unexpected error in get_all_enriched_data_for_embedding: {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def store_embeddings(item_id: int, embedding: np.ndarray, model_name: str):
    """Stores the generated embedding vector and model version in the database."""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Convert numpy array to bytes for BLOB storage
        embedding_blob = embedding.tobytes()
        
        sql = """
            UPDATE enriched_metadata 
            SET embedding_vector = %s, embedding_model_version = %s
            WHERE id = %s
        """
        cursor.execute(sql, (embedding_blob, model_name, item_id))
        conn.commit()
        # print(f"Stored embedding for item ID {item_id} using model {model_name}.")
        
    except mysql.connector.Error as err:
        print(f"Database error in store_embeddings for item ID {item_id}: {err}")
    except Exception as e:
        print(f"Unexpected error in store_embeddings for item ID {item_id}: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def main():
    print("Starting pre-computation of embeddings...")
    
    # First, ensure the 'enriched_metadata' table has the necessary columns.
    # This should be done manually or via a migration script.
    # ALTER TABLE enriched_metadata ADD COLUMN embedding_vector BLOB;
    # ALTER TABLE enriched_metadata ADD COLUMN embedding_model_version VARCHAR(255);
    # print("Ensure 'embedding_vector BLOB' and 'embedding_model_version VARCHAR(255)' columns exist in 'enriched_metadata' table.")

    items_to_process = get_all_enriched_data_for_embedding()
    
    if not items_to_process:
        print("No items found that require embedding. Exiting.")
        return

    descriptions = [item['semantic_description'] for item in items_to_process] # Corrected typo here
    
    if not descriptions:
        print("No valid descriptions to embed. Exiting.")
        return

    print(f"Generating embeddings for {len(descriptions)} descriptions...")
    embeddings_np = model.encode(descriptions, convert_to_tensor=False, show_progress_bar=True)
    
    print("Storing embeddings in the database...")
    for i, item in enumerate(items_to_process):
        item_id = item['id']
        embedding_vector = embeddings_np[i]
        store_embeddings(item_id, embedding_vector, MODEL_NAME)
        if (i+1) % 10 == 0: # Print progress every 10 items
             print(f"Processed {i+1}/{len(items_to_process)} items.")
    
    print(f"Finished pre-computing and storing embeddings for {len(items_to_process)} items.")

if __name__ == '__main__':
    main()
