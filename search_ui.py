import streamlit as st
import requests
import json

# API endpoint
API_URL = "http://127.0.0.1:5001/search"
INFERRED_REL_API_URL = "http://127.0.0.1:5001/inferred-relationships"

# --- Page Configuration ---
st.set_page_config(
    layout="wide",
    page_title="AuraDB Semantic Catalog",
    page_icon="📚" # Favicon
)

# --- Custom CSS for better styling (Optional) ---
st.markdown("""
<style>
    .stExpander {
        border: 1px solid #e6e6e6;
        border-radius: 0.5rem;
        padding: 0.5rem;
        margin-bottom: 0.5rem;
    }
    .stButton>button {
        border-radius: 0.5rem;
        padding: 0.5em 1em;
    }
    .stTextInput>div>div>input {
        border-radius: 0.5rem;
    }
    .stSpinner > div {
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# --- Header ---
st.image("https://cdn-icons-png.flaticon.com/512/2920/2920349.png", width=75) # New URL
st.title("✨ AuraDB Semantic Catalog ✨")

st.markdown("Discover your data assets with the power of semantics and LLMs. Enter a natural language query below.")

# --- Main Layout ---
# Sidebar for controls and information
with st.sidebar:
    st.header("About AuraDB")
    st.info("""
        AuraDB is an LLM-Powered Semantic Data Catalog. 
        It helps you understand and discover your data assets through natural language.
        """)
    st.markdown("---")
    st.header("Controls")
    show_relationships = st.checkbox("Show Inferred Relationships", value=False)
    st.markdown("---")
    st.header("Technologies")
    st.markdown("""
    - Python, Streamlit, Flask
    - MySQL
    - LangChain, Sentence Transformers, FAISS
    - LLMs (via LM Studio)
    """)

# Main content area
col1, col2 = st.columns([2, 1]) # Search results on left, relationships (optional) on right

with col1:
    st.subheader("🔍 Search the Catalog")
    search_query = st.text_input("Search query", placeholder="e.g., customer information, product sales, order details", label_visibility="collapsed")

    if st.button("Search", type="primary") or search_query:
        if not search_query:
            st.warning("Please enter a search query.")
        else:
            with st.spinner(f'Searching for: "{search_query}"...'):
                try:
                    response = requests.get(API_URL, params={"query": search_query})
                    response.raise_for_status()
                    search_results = response.json()

                    if search_results and "results" in search_results:
                        results_data = search_results["results"]
                        if results_data:
                            st.success(f"Found {len(results_data)} results:")
                            for item in results_data:
                                with st.container():
                                    st.markdown("---")
                                    object_type_icon = "📄" if item.get('object_type') == 'column' else "📦"
                                    parent_info = f"(Table: {item.get('parent_table_name')})" if item.get('parent_table_name') else ""
                                    st.markdown(f"#### {object_type_icon} **{item.get('object_type', '').capitalize()}: {item.get('object_name', 'N/A')}** {parent_info}")
                                    
                                    st.caption(f"Description:")
                                    st.markdown(f"> {item.get('semantic_description', '_No description available._')}")
                                    
                                    tags = item.get('tags', [])
                                    if isinstance(tags, str):
                                        try:
                                            tags = json.loads(tags)
                                        except json.JSONDecodeError:
                                            tags = []
                                    
                                    if tags:
                                        st.caption("Tags:")
                                        tags_html = "".join([f"<span style='background-color: #f0f2f6; color: #333; border-radius: 0.25rem; padding: 0.2rem 0.5rem; margin-right: 0.3rem; font-size: 0.85em;'>{tag}</span>" for tag in tags])
                                        st.markdown(tags_html, unsafe_allow_html=True)
                                    else:
                                        st.caption("Tags: _None_")
                                    st.markdown("<br>", unsafe_allow_html=True) # Add a bit of space
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

with col2:
    if show_relationships:
        st.subheader("🔗 Inferred Relationships")
        st.markdown("Potential relationships inferred by the LLM.")
        try:
            with st.spinner("Fetching inferred relationships..."):
                response = requests.get(INFERRED_REL_API_URL)
                response.raise_for_status()
                data = response.json()
                relationships = data.get("relationships", [])

                if relationships:
                    for rel in relationships:
                        with st.container():
                            st.markdown("---")
                            st.markdown(f"**{rel['source_table']}.{rel['source_column']}**  ➡️  **{rel['target_table']}.{rel['target_column']}**")
                            st.caption(f"Type: `{rel.get('relationship_type', 'N/A')}`")
                            st.markdown(f"**Justification:** {rel.get('justification', '_N/A_')}")
                            # st.caption(f"LLM: `{rel.get('llm_model_version', 'N/A')}` | Discovered: {rel.get('created_at', 'N/A')}")
                            st.markdown("<br>", unsafe_allow_html=True)
                elif "error" in data:
                    st.error(f"API Error fetching relationships: {data['error']}")
                else:
                    st.info("No inferred relationships found or the API returned an empty list.")

        except requests.exceptions.RequestException as e:
            st.error(f"Error connecting to the API for inferred relationships: {e}")
        except json.JSONDecodeError:
            st.error("Error: Could not decode the response from the inferred relationships API.")
        except Exception as e:
            st.error(f"An unexpected error occurred while fetching inferred relationships: {e}")

# Footer (Optional)
st.markdown("---")
st.markdown("<p style='text-align: center; color: grey;'>AuraDB Semantic Catalog © 2025</p>", unsafe_allow_html=True)
