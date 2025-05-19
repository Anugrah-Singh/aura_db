from flask import Flask, request, jsonify
import mysql.connector
import json
import numpy as np # Added for FAISS
from sentence_transformers import SentenceTransformer # Added for embeddings
import faiss # Added for vector search
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# --- Database Connection Details (same as other scripts) ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',      # Your MySQL username
    'password': 'Sql@#&50490', # Your MySQL password
    'database': 'semantic_catalog_db'
}

# --- Initialize Sentence Transformer Model ---
MODEL_NAME = 'all-MiniLM-L6-v2' 
model = SentenceTransformer(MODEL_NAME)
print(f"Sentence Transformer model '{MODEL_NAME}' loaded.")

# --- LLM Configuration for Re-ranking ---
LLM_RERANK_MODEL_NAME = 'gemma-3-4b-it-qat' # Your model in LM Studio
LLM_RERANK_BASE_URL = 'http://127.0.0.1:1234/v1' # LM Studio OpenAI-compatible endpoint
llm_reranker = None
try:
    llm_reranker = ChatOpenAI(
        model=LLM_RERANK_MODEL_NAME,
        base_url=LLM_RERANK_BASE_URL,
        api_key="not-needed", # LM Studio typically doesn't require an API key
        temperature=0.1 # Lower temperature for more deterministic output for re-ranking
    )
    print(f"LLM Re-ranker ({LLM_RERANK_MODEL_NAME}) initialized successfully.")
except Exception as e:
    print(f"Error initializing LLM Re-ranker: {e}. Re-ranking will be disabled.")
    llm_reranker = None

# --- Prompt Template for Re-ranking ---
RERANK_PROMPT_TEMPLATE = """
Original User Query: "{user_query}"

I have retrieved the following items based on a semantic search. Each item has an ID and a Description.
Please re-rank these items based on their relevance to the Original User Query.
Provide a comma-separated list of the item IDs in the new order of relevance (most relevant first).
You MUST ONLY output the comma-separated list of IDs. Do not include any other text, titles, or explanations.
For example: 123,45,678

Items to re-rank:
{items_for_reranking}

Re-ranked IDs (comma-separated list ONLY):
"""

# --- Global variables for pre-loaded data and FAISS index ---
FAISS_INDEX = None
ALL_ITEMS_DATA = [] # Stores the full data of items in the FAISS index
ALL_ITEMS_IDS = [] # Stores the database IDs of items in the FAISS index, mapping FAISS index to DB ID

# --- Embedding and Vector Search Functions ---
def get_embeddings(texts: list[str]):
    """Generates embeddings for a list of texts using the loaded Sentence Transformer model."""
    if not texts:
        return []
    embeddings = model.encode(texts, convert_to_tensor=False) # convert_to_tensor=False for numpy array
    return embeddings

def build_faiss_index(embeddings: np.ndarray):
    """Builds a FAISS index from a list of embeddings."""
    if embeddings is None or len(embeddings) == 0:
        return None
    dimension = embeddings.shape[1]
    index = faiss.IndexFlatL2(dimension)  # Using L2 distance
    index.add(embeddings)
    return index

def search_faiss_index(index, query_embedding: np.ndarray, k=10):
    """Searches the FAISS index for the top k similar items."""
    if index is None or query_embedding is None or index.ntotal == 0:
        print("Warning: FAISS index is None, query_embedding is None, or index is empty.")
        return np.array([]), np.array([])
    distances, indices = index.search(query_embedding, k)
    return distances[0], indices[0]


app = Flask(__name__)

def load_and_index_data():
    """Loads data and pre-computed embeddings from DB, then builds FAISS index."""
    global FAISS_INDEX, ALL_ITEMS_DATA, ALL_ITEMS_IDS
    conn = None
    print("Loading pre-computed embeddings and building FAISS index...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Fetch items that have an embedding_vector and the correct model version
        cursor.execute("""
            SELECT id, object_type, object_name, parent_table_name, semantic_description, tags, embedding_vector 
            FROM enriched_metadata 
            WHERE embedding_vector IS NOT NULL AND embedding_model_version = %s
        """, (MODEL_NAME,))
        
        items_with_embeddings = cursor.fetchall()
        
        if not items_with_embeddings:
            print("No pre-computed embeddings found for the current model. FAISS index will be empty.")
            FAISS_INDEX = None
            ALL_ITEMS_DATA = []
            ALL_ITEMS_IDS = []
            return

        loaded_embeddings_list = []
        temp_items_data = []
        temp_items_ids = []

        embedding_dim = model.get_sentence_embedding_dimension() # Get expected dimension

        for item in items_with_embeddings:
            if item['embedding_vector']:
                try:
                    # Deserialize BLOB to numpy array
                    embedding_np = np.frombuffer(item['embedding_vector'], dtype=np.float32)
                    
                    # Ensure the embedding has the correct dimension
                    if embedding_np.shape[0] == embedding_dim:
                        loaded_embeddings_list.append(embedding_np)
                        # Store other data, deserialize tags
                        if item.get('tags') and isinstance(item['tags'], str):
                            try:
                                item['tags'] = json.loads(item['tags'])
                            except (json.JSONDecodeError, TypeError):
                                item['tags'] = []
                        temp_items_data.append(item)
                        temp_items_ids.append(item['id'])
                    else:
                        print(f"Warning: Item ID {item['id']}: embedding dimension mismatch. Expected {embedding_dim}, got {embedding_np.shape[0]}. Skipping.")
                except Exception as e:
                    print(f"Error processing embedding for item ID {item['id']}: {e}. Skipping.")
            
        if not loaded_embeddings_list:
            print("No valid embeddings were loaded. FAISS index will be empty.")
            FAISS_INDEX = None
            ALL_ITEMS_DATA = []
            ALL_ITEMS_IDS = []
            return

        # Convert list of embeddings to a 2D numpy array
        embeddings_matrix = np.array(loaded_embeddings_list).astype('float32')
        
        FAISS_INDEX = build_faiss_index(embeddings_matrix)
        ALL_ITEMS_DATA = temp_items_data

        ALL_ITEMS_IDS = temp_items_ids # This now directly maps FAISS index to original item data

        if FAISS_INDEX:
            print(f"FAISS index built successfully with {FAISS_INDEX.ntotal} items.")
        else:
            print("Failed to build FAISS index.")
            
    except mysql.connector.Error as err:
        print(f"Database error in load_and_index_data: {err}")
    except Exception as e:
        print(f"Unexpected error in load_and_index_data: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('query', '')
    if not query:
        return jsonify({"error": "Query parameter is required"}), 400

    global FAISS_INDEX, ALL_ITEMS_DATA, llm_reranker

    if FAISS_INDEX is None or FAISS_INDEX.ntotal == 0:
        return jsonify({"results": [], "message": "FAISS index is not available or empty."}), 200

    # 1. Get embedding for the query
    query_embedding = get_embeddings([query])
    if len(query_embedding) == 0:
        return jsonify({"error": "Could not generate query embedding."}), 500
    
    query_embedding_np = np.array(query_embedding).astype('float32')

    # 2. Search the FAISS index
    distances, indices = search_faiss_index(FAISS_INDEX, query_embedding_np, k=10)
    
    initial_search_results = []
    if indices.size > 0:
        for i in range(len(indices)):
            faiss_idx = indices[i]
            if 0 <= faiss_idx < len(ALL_ITEMS_DATA):
                result_item = ALL_ITEMS_DATA[faiss_idx].copy() 
                # Remove the embedding_vector as it's bytes and not JSON serializable
                if 'embedding_vector' in result_item:
                    del result_item['embedding_vector']
                result_item['similarity_score'] = float(1 / (1 + distances[i])) 
                initial_search_results.append(result_item)

    # 3. LLM Re-ranking (if enabled and results exist)
    final_search_results = initial_search_results
    if llm_reranker and initial_search_results:
        items_for_reranking_str = ""
        for item in initial_search_results:
            description = item.get('semantic_description', 'No description available.')
            if not isinstance(description, str):
                description = str(description)
            
            # Escape backslashes first, then double quotes to safely include in the prompt string
            processed_description = description.replace('\\\\', '\\\\\\\\').replace('"', '\\\\"')

            # Construct the item line for the prompt, ensuring the description is wrapped in escaped quotes
            item_line = f"ID: {item['id']}, Description: \\\"{processed_description}\\\""
            items_for_reranking_str += item_line + "\\n"

        if items_for_reranking_str:
            rerank_prompt_input = {
                "user_query": query,
                "items_for_reranking": items_for_reranking_str.strip()
            }
            
            rerank_chat_prompt = ChatPromptTemplate.from_template(RERANK_PROMPT_TEMPLATE)
            rerank_chain = rerank_chat_prompt | llm_reranker | StrOutputParser()

            try:
                print(f"Invoking LLM for re-ranking {len(initial_search_results)} items for query: '{query}'...")
                reranked_ids_str = rerank_chain.invoke(rerank_prompt_input)
                print(f"LLM Re-ranker raw output: '{reranked_ids_str}'")

                parsed_ids = []
                if reranked_ids_str and isinstance(reranked_ids_str, str):
                    for id_str_part in reranked_ids_str.split(','):
                        cleaned_id_str = id_str_part.strip()
                        if cleaned_id_str.isdigit():
                            parsed_ids.append(int(cleaned_id_str))
                
                if parsed_ids:
                    original_results_map = {item['id']: item for item in initial_search_results}
                    reranked_results_temp = []
                    processed_in_reranking = set()

                    for item_id in parsed_ids:
                        if item_id in original_results_map:
                            reranked_results_temp.append(original_results_map[item_id])
                            processed_in_reranking.add(item_id)
                    
                    for item in initial_search_results:
                        if item['id'] not in processed_in_reranking:
                            reranked_results_temp.append(item)
                    
                    final_search_results = reranked_results_temp
                    print(f"Search results re-ranked by LLM. New order IDs: {[item['id'] for item in final_search_results]}")
                else:
                    print("LLM did not return valid IDs for re-ranking or output was empty. Using original FAISS order.")

            except Exception as e:
                print(f"Error during LLM re-ranking: {e}. Using original FAISS order.")
        else:
            print("No valid items to send for LLM re-ranking. Using original FAISS order.")

    return jsonify({"results": final_search_results})

if __name__ == '__main__':
    print("Flask API starting with FAISS and Sentence Transformers...")
    load_and_index_data()
    app.run(debug=True, port=5001)
