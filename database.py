import psycopg2
from data.config_constants import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE


conn = psycopg2.connect(user=DB_USER,
                        password=DB_PASSWORD,
                        host=DB_HOST,
                        port=DB_PORT,
                        database=DB_DATABASE
                        )
cur = conn.cursor()


def load_csv_to_db(path_to_csv, target_table):
    with open(path_to_csv, "r") as csv_file:
        next(csv_file)
        cur.copy_from(csv_file, target_table, sep="|")
        conn.commit()


# def load_table_to_csv(path_to_new_csv, source_table):
#     with open(path_to_new_csv, "w") as csv_file:
#         cur.copy_to(csv_file, source_table, sep="|")
#         conn.commit()


def load_sql_to_csv(sql, file):
    with open(file, "w") as csv_file:
        cur.copy_expert(sql, csv_file)
        conn.commit()


def query_exec(query):
    cur.execute(query)
    conn.commit()
