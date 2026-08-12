import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Carga los datos crudos desde S3 hacia las tablas de staging usando COPY."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transforma los datos de staging e inserta en las tablas de hechos y dimensiones."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            config.get("CLUSTER", "HOST"),
            config.get("CLUSTER", "DB_NAME"),
            config.get("CLUSTER", "DB_USER"),
            config.get("CLUSTER", "DB_PASSWORD"),
            config.get("CLUSTER", "DB_PORT"),
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
