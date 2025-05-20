# LLM-Powered Semantic Data Catalog and Discovery System

## Project Overview

This project implements an LLM-Powered Semantic Data Catalog and Discovery system. It extracts technical metadata from a MySQL database, enriches it with LLM-generated semantic descriptions and tags, pre-computes embeddings for these descriptions, and provides a natural language search interface. The system also infers potential new relationships within the database schema using an LLM.

## Features

*   **Metadata Extraction:** Extracts schema (tables, columns, types, keys) and sample data from a MySQL database.
*   **LLM-Powered Enrichment:** Uses a Large Language Model (LLM) to generate human-readable semantic descriptions and relevant tags for tables and columns.
*   **Embedding Generation:** Pre-computes embeddings for semantic descriptions using Sentence Transformers.
*   **Vector Search:** Utilizes FAISS for efficient similarity search based on embeddings.
*   **Natural Language Search:** Provides a Streamlit-based UI for users to search the catalog using natural language queries.
*   **LLM Re-ranking:** Employs an LLM to re-rank initial search results for improved relevance.
*   **Relationship Inference:** Uses an LLM to analyze the schema and infer potential new relationships between tables.
*   **API & UI:** Flask API serves search results and inferred relationships; Streamlit UI provides user interaction.

## Technologies Used

*   **Python 3.x**
*   **MySQL:** For storing technical and enriched metadata.
*   **LangChain:** Framework for developing applications powered by LLMs.
*   **Sentence Transformers:** For generating embeddings.
*   **FAISS:** For efficient similarity search on vectors.
*   **Flask:** Web framework for the search API.
*   **Streamlit:** For building the interactive search UI.
*   **LM Studio (or other OpenAI-compatible LLM server):** For serving the LLM.

## Workspace Structure

```
.
├── database_setup.py         # Sets up the MySQL database schema and initial data.
├── metadata_extractor.py     # Extracts technical metadata from the database.
├── extracted_metadata.json   # Output of metadata_extractor.py.
├── llm_enrichment.py         # Enriches extracted metadata using an LLM.
├── precompute_embeddings.py  # Generates and stores embeddings for enriched metadata.
├── relationship_inferer.py   # Infers potential relationships in the schema using an LLM.
├── search_api.py             # Flask API for search and relationship retrieval.
├── search_ui.py              # Streamlit UI for interacting with the catalog.
└── README.md                 # This file.
```

## Setup Instructions

1.  **Prerequisites:**
    *   Python 3.8+
    *   MySQL Server installed and running.
    *   LM Studio (or a similar tool) running with an OpenAI-compatible API endpoint. Download and run a model like `gemma-3-4b-it-qat` (or any preferred model).

2.  **Clone the Repository (if applicable) or Set Up Project Files:**
    Ensure all the Python scripts listed above are in your project directory.

3.  **Create a Python Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

4.  **Install Python Dependencies:**
    Create a `requirements.txt` file with the following content:
    ```txt
    mysql-connector-python
    langchain
    langchain-openai
    sentence-transformers
    faiss-cpu  # Or faiss-gpu if you have a compatible GPU
    flask
    streamlit
    numpy
    ```
    Then install them:
    ```bash
    pip install -r requirements.txt
    ```

5.  **Configure Database Connection:**
    Update the `DB_CONFIG` dictionary in the following files with your MySQL credentials:
    *   `database_setup.py`
    *   `metadata_extractor.py`
    *   `llm_enrichment.py`
    *   `precompute_embeddings.py`
    *   `relationship_inferer.py`
    *   `search_api.py`

    Example `DB_CONFIG`:
    ```python
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'your_mysql_user',
        'password': 'your_mysql_password',
        'database': 'semantic_catalog_db' # This DB will be created by database_setup.py
    }
    ```

6.  **Configure LLM Connection:**
    Update `LLM_BASE_URL` and `LLM_MODEL_NAME` in `llm_enrichment.py`, `relationship_inferer.py`, and `search_api.py` if your LM Studio (or other LLM server) uses a different endpoint or model name. The default is set for LM Studio running locally:
    ```python
    LLM_MODEL_NAME = 'gemma-3-4b-it-qat' # Or your specific model in LM Studio
    LLM_BASE_URL = 'http://127.0.0.1:1234/v1' # LM Studio OpenAI-compatible endpoint
    ```

## How to Run

Execute the scripts in the following order:

1.  **Set up the database:**
    ```bash
    python database_setup.py
    ```
    This script creates the necessary tables and populates them with some dummy data.

2.  **Extract technical metadata:**
    ```bash
    python metadata_extractor.py
    ```
    This script connects to your database, extracts schema information and sample data, and saves it to `extracted_metadata.json`.

3.  **Enrich metadata with LLM:**
    Ensure your LLM server (e.g., LM Studio) is running and accessible.
    ```bash
    python llm_enrichment.py
    ```
    This script reads `extracted_metadata.json`, uses the LLM to generate semantic descriptions and tags, and stores this enriched data in the `enriched_metadata` table.

4.  **Pre-compute embeddings:**
    ```bash
    python precompute_embeddings.py
    ```
    This script generates embeddings for the semantic descriptions stored in `enriched_metadata` and updates the table with these embeddings.

5.  **Infer relationships:**
    Ensure your LLM server is running.
    ```bash
    python relationship_inferer.py
    ```
    This script uses the LLM to analyze the schema (from `extracted_metadata.json`) and infer potential relationships, storing them in the `inferred_relationships` table.

6.  **Start the Search API:**
    ```bash
    python search_api.py
    ```
    This Flask application will start (typically on port 5001). It loads the embeddings, builds a FAISS index, and provides endpoints for search and relationship retrieval. Keep this terminal running.

7.  **Run the Search UI:**
    Open a new terminal.
    ```bash
    streamlit run search_ui.py
    ```
    This will start the Streamlit application (typically on port 8501) and open it in your web browser. You can now use the UI to search the catalog and view inferred relationships.

## Scripts Overview

*   **`database_setup.py`**: Initializes the MySQL database schema (`semantic_catalog_db`) and populates it with sample tables (`Customers`, `Products`, `Orders`, `Order_Items`) and data. Also creates tables for `enriched_metadata` and `inferred_relationships`.
*   **`metadata_extractor.py`**: Connects to the MySQL database, inspects its schema (tables, columns, data types, primary keys, foreign keys), fetches sample data for each table, and saves this information into `extracted_metadata.json`.
*   **`llm_enrichment.py`**: Loads `extracted_metadata.json`. For each table and column, it prompts an LLM (via LM Studio) to generate a semantic description and relevant tags. This enriched information is then stored in the `enriched_metadata` table in the database.
*   **`precompute_embeddings.py`**: Fetches the semantic descriptions from the `enriched_metadata` table. It uses a Sentence Transformer model (e.g., `all-MiniLM-L6-v2`) to generate vector embeddings for these descriptions and stores them back into the `enriched_metadata` table.
*   **`relationship_inferer.py`**: Takes the schema information from `extracted_metadata.json`, formats it for an LLM, and prompts the LLM to infer potential relationships between tables/columns that might not be explicitly defined by foreign keys. These inferred relationships are stored in the `inferred_relationships` table.
*   **`search_api.py`**: A Flask-based API.
    *   On startup, it loads all enriched metadata and pre-computed embeddings from the database.
    *   Builds a FAISS index for efficient similarity search.
    *   Provides a `/search` endpoint that takes a user query, generates its embedding, searches the FAISS index, optionally re-ranks results with an LLM, and returns relevant metadata.
    *   Provides an `/inferred-relationships` endpoint to retrieve all inferred relationships.
*   **`search_ui.py`**: A Streamlit web application that provides a user interface for:
    *   Entering natural language search queries.
    *   Displaying search results (tables, columns with their descriptions and tags).
    *   Displaying inferred relationships from the database.

## Potential Future Enhancements

*   **Incremental Updates:** Implement mechanisms to update the catalog incrementally as the source database schema changes, rather than full re-processing.
*   **More Sophisticated Re-ranking:** Explore more advanced re-ranking strategies or models.
*   **User Feedback Loop:** Allow users to validate or correct LLM-generated descriptions, tags, and inferred relationships.
*   **Knowledge Graph Integration:** Store and visualize metadata and relationships as a knowledge graph.
*   **Advanced Data Profiling:** Include more detailed data profiling statistics in the metadata.
*   **Support for More Data Sources:** Extend the system to support other database types (e.g., PostgreSQL, SQL Server) or data formats (e.g., CSV, Parquet).
*   **UI Enhancements:** Add features like filtering, sorting, and more detailed views of metadata.
*   **Security and Access Control:** Implement proper security measures if deployed in a production environment.
