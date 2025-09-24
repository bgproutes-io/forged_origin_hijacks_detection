import psycopg2
from dotenv import load_dotenv
import os

# === Load environment variables from .env ===
load_dotenv()

DB_NAME = os.getenv("DFOH_DB_NAME")
DB_USER = os.getenv("DFOH_DB_USER")
DB_PASSWORD = os.getenv("DFOH_DB_PWD")
DB_HOST = os.getenv("DFOH_DB_HOST", "localhost")
DB_PORT = os.getenv("DFOH_DB_PORT", "5432")

def create_tables():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Create inference_summary table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inference_summary (
            id SERIAL PRIMARY KEY,
            asn1 BIGINT NOT NULL,
            asn2 BIGINT NOT NULL,
            classification VARCHAR(3) NOT NULL,
            confidence_level SMALLINT NOT NULL,
            num_legit_inf INT NOT NULL,
            num_susp_inf INT NOT NULL,
            num_paths INT NOT NULL,
            attackers BIGINT[] NOT NULL,
            victims BIGINT[] NOT NULL,
            hijack_types INT[] NOT NULL,
            is_origin_rpki_valid BOOLEAN NOT NULL,
            is_recurrent BOOLEAN NOT NULL,
            observed_at TIMESTAMP NOT NULL,
            is_local BOOLEAN NOT NULL
        );
    """)

    # Create inference_details table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inference_details (
            id SERIAL PRIMARY KEY,
            inference_id INTEGER NOT NULL REFERENCES inference_summary(id) ON DELETE CASCADE,
            model_id INT NOT NULL,
            num_legit_paths INT NOT NULL,
            num_susp_paths INT NOT NULL
        );
    """)

    # Create new_link table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS new_link (
            id SERIAL PRIMARY KEY,
            asn1 BIGINT NOT NULL,
            asn2 BIGINT NOT NULL,
            as_path TEXT NOT NULL,
            observed_at TIMESTAMP NOT NULL,
            prefix CIDR NOT NULL,
            peer_ip INET,
            peer_asn BIGINT NOT NULL,
            is_recurrent BOOLEAN NOT NULL,
            inference_id INTEGER REFERENCES inference_summary(id) ON DELETE SET NULL
        );
    """)

    print("✅ All tables created successfully.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_tables()

