import os
import io
from tqdm import tqdm
from sqlalchemy import create_engine, text


def init_database(db_url):
    """Executes a SQL file to initialize the database schema."""
    sql_file_path = os.path.join(os.path.dirname(__file__), "sql", "init_schema.sql")

    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file not found at {sql_file_path}")
        return

    try:
        engine = create_engine(db_url)
        with engine.begin() as conn:
            with open(sql_file_path, "r") as file:
                sql_script = file.read()
                conn.execute(text(sql_script))
        print(f"Successfully initialized database schema.")
    except Exception as e:
        print(f"Database Initialization Error: {e}")


def copy_to_sql_with_progress(df, table_name, engine, chunksize=100000):
    """
    Uses PostgreSQL's native COPY command which is 10-100x faster than standard pandas to_sql.
    Includes a tqdm progress bar for monitoring.
    """
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        with tqdm(total=len(df), desc=f"Uploading {table_name}", unit="rows") as pbar:
            for i in range(0, len(df), chunksize):
                chunk = df.iloc[i : i + chunksize]
                buffer = io.StringIO()
                # Use \N for nulls so Postgres COPY interprets them correctly
                chunk.to_csv(buffer, index=False, header=False, na_rep="\\N")
                buffer.seek(0)

                columns = ",".join([f'"{col}"' for col in chunk.columns])
                sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV NULL '\\N'"
                cursor.copy_expert(sql, buffer)

                pbar.update(len(chunk))

        raw_conn.commit()
        cursor.close()
    except Exception as e:
        raw_conn.rollback()
        raise e
    finally:
        raw_conn.close()


def upload_to_postgres(df, table_name, db_url):
    """Uploads a pandas DataFrame to a PostgreSQL database."""
    try:
        engine = create_engine(db_url)
        copy_to_sql_with_progress(df, table_name, engine, chunksize=100000)
    except Exception as e:
        if "UniqueViolation" in str(e) or "duplicate key" in str(e):
            print("Upload Skipped: Data for this date already exists.")
        else:
            print(f"Database Upload Error: {str(e)[:200]}")
