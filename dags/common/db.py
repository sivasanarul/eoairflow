import psycopg2

def condition_met(query, db_config):
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0] == 0
    finally:
        conn.close()
