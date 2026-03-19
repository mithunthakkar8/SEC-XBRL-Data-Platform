import psycopg2
from sentence_transformers import SentenceTransformer
import numpy as np
from typing import List

# Database configuration (replace with your actual credentials)
db_config = {
        'dbname': 'finhub',
        'user': 'finhub_admin',
        'password': 'pass@123',
        'host': 'localhost',
        'port': '5432'
    }

# Define batch size and model for Sentence-Transformers
BATCH_SIZE = 32
MODEL_NAME = 'all-MiniLM-L6-v2'  # You can change the model name if you prefer another one

# Initialize the model
model = SentenceTransformer(MODEL_NAME)

def get_standard_labels_from_db() -> List[str]:
    """Fetch all standard labels from the database."""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT standard_label
            FROM xbrl.standard_label_embeddings
            WHERE vector_embeddings IS NULL
            LIMIT 1000;  # Adjust the number as needed for batching
        """)
        rows = cur.fetchall()
        return [row[0] for row in rows]
    finally:
        cur.close()
        conn.close()

def update_embeddings_in_db(labels: List[str]):
    """Generate embeddings for the provided labels and update them in the database."""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    try:
        # Generate embeddings for the batch of labels
        embeddings = model.encode(labels)

        # Update the database with the embeddings
        for i, label in enumerate(labels):
            embedding = embeddings[i].tolist()  # Convert the embedding to a list format for DB insertion
            cur.execute("""
                UPDATE xbrl.standard_label_embeddings
                SET vector_embeddings = %s
                WHERE standard_label = %s
            """, (embedding, label))

        # Commit the changes
        conn.commit()
        print(f"Updated {len(labels)} rows in the database.")

    finally:
        cur.close()
        conn.close()

def process_labels_in_batches():
    """Fetch labels, generate embeddings, and update them in batches."""
    while True:
        # Step 1: Fetch standard labels from the database
        labels = get_standard_labels_from_db()

        if not labels:
            print("All labels processed or no labels left to process.")
            break

        # Step 2: Update embeddings for the current batch
        print(f"Processing batch of {len(labels)} labels...")
        update_embeddings_in_db(labels)

if __name__ == "__main__":
    # Run the script to process and update embeddings in batches
    process_labels_in_batches()
