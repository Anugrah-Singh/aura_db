import streamlit as st
import requests
import json

# API endpoint
API_URL = "http://127.0.0.1:5001/search"

st.set_page_config(layout="wide", page_title="Semantic Data Catalog Search")

st.title("📚 Semantic Data Catalog Search")

st.markdown("""
Enter a natural language query to search the data catalog. 
This demo uses simulated embeddings and vector search.
""")

# Search bar
search_query = st.text_input("Search query", placeholder="e.g., customer information, product sales, order details")

if st.button("Search") or search_query:
    if not search_query:
        st.warning("Please enter a search query.")
    else:
        with st.spinner(f'Searching for: "{search_query}"...'):
            try:
                response = requests.get(API_URL, params={"query": search_query})
                response.raise_for_status()  # Raise an exception for HTTP errors
                search_results = response.json()

                if search_results and "results" in search_results:
                    results_data = search_results["results"]
                    if results_data:
                        st.subheader(f"Found {len(results_data)} results:")
                        for item in results_data:
                            with st.expander(f"**{item.get('object_type', '').capitalize()}: {item.get('object_name', 'N/A')}** (Parent: {item.get('parent_table_name', 'N/A')})"):
                                st.markdown(f"**Description:** {item.get('semantic_description', 'No description available.')}")
                                
                                tags = item.get('tags', [])
                                if isinstance(tags, str): # Handle if tags are still a JSON string
                                    try:
                                        tags = json.loads(tags)
                                    except json.JSONDecodeError:
                                        tags = []
                                
                                if tags:
                                    st.markdown("**Tags:**")
                                    tag_str = ", ".join([f"`{tag}`" for tag in tags])
                                    st.markdown(tag_str)
                                else:
                                    st.markdown("**Tags:** None")
                    else:
                        st.info("No results found for your query.")
                elif search_results and "message" in search_results:
                    st.info(search_results["message"]) 
                elif search_results and "error" in search_results:
                    st.error(f"API Error: {search_results['error']}")
                else:
                    st.error("Received an unexpected response from the API.")

            except requests.exceptions.RequestException as e:
                st.error(f"Error connecting to the search API: {e}")
            except json.JSONDecodeError:
                st.error("Error: Could not decode the response from the API. The API might be down or returning invalid JSON.")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")

# Instructions / Info
st.sidebar.header("About")
st.sidebar.info("""
This application demonstrates a prototype LLM-Powered Semantic Data Catalog and Discovery system.

**Features:**
- Extracts technical metadata from a MySQL database.
- Uses a local LLM (Gemma via LM Studio) to generate semantic descriptions and tags.
- Stores enriched metadata.
- Provides a (simulated) natural language search interface.

**Note:** The search functionality currently uses placeholder embeddings and a basic similarity logic. 
In a production system, this would be replaced with robust sentence transformers and a vector database for true semantic search.
""")
st.sidebar.header("Technologies Used")
st.sidebar.markdown("""
- Python
- MySQL
- LangChain
- OpenAI-compatible LLM (LM Studio)
- Flask (for API)
- Streamlit (for UI)
""")
