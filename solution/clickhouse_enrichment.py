import clickhouse_connect
import psycopg
import os
import logging

logging.basicConfig(level=logging.INFO)


POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "postgres")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PW = os.environ.get("CLICKHOUSE_PW", "default")


class ClickhouseEnricher:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, username=CLICKHOUSE_USER, password=CLICKHOUSE_PW
        )

    def create_tables(self):
        """Create defined tables in Clickhouse."""
        with open("clickhouse_schema.sql", "r") as file:
            statements = file.read().split(";")
        for statement in statements:
            sql = statement.strip()
            if sql:
                try:
                    logging.info(f"Executing SQL: {sql}")
                    self.client.command(sql + ";")
                except Exception as e:
                    logging.error(f"Error executing SQL: {sql} - {e}")
                    continue

    def connect_to_postgres(self):
        """Connect to PostgreSQL database."""
        conn = psycopg.connect(
            f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} user={POSTGRES_USER}",
            autocommit=False,
        )
        return conn

    def fetch_campaign_stats(self) -> list:
        """Fetch campaign statistics from PostgreSQL."""
        query = """
            SELECT 
                c.id,
                c.name, 
                c.advertiser_id,
                a.name AS advertiser_name,
                c.bid,
                c.budget,
                COUNT(DISTINCT i.id) AS count_impressions,
                COUNT(DISTINCT cl.id) AS count_clicks
            FROM 
                campaign c
            LEFT JOIN 
                clicks cl ON c.id = cl.campaign_id
            LEFT JOIN 
                impressions i ON i.campaign_id = c.id
            LEFT JOIN
                advertiser a ON c.advertiser_id = a.id
            GROUP BY 
                c.id, advertiser_name; 
            """

        with self.connect_to_postgres() as conn:
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()

        logging.info(f"Fetched {len(data)} rows from PostgreSQL.")
        return data

    def fetch_daily_click_stats(self) -> list:
        """Fetch daily click statistics from PostgreSQL."""
        query = """
            SELECT 
                campaign_id,
                created_at::date as date,
                COUNT(DISTINCT id) AS count_clicks
            FROM 
                clicks
            GROUP BY 
                campaign_id, date;
        """

        with self.connect_to_postgres() as conn:
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()

        logging.info(f"Fetched {len(data)} rows from PostgreSQL.")
        return data

    def fetch_daily_impression_stats(self) -> list:
        """Fetch daily impression statistics from PostgreSQL."""
        query = """
            SELECT 
                campaign_id,
                created_at::date as date,
                COUNT(DISTINCT id) AS count_impressions
            FROM 
                impressions
            GROUP BY 
                campaign_id, date;
        """

        with self.connect_to_postgres() as conn:
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()

        logging.info(f"Fetched {len(data)} rows from PostgreSQL.")
        return data

    def insert_into_clickhouse(self, data, table, columns):
        """
        Insert campaign statistics into Clickhouse.
        Args:
            data (list): Data to insert.
            table (str): Clickhouse table name.
            columns (list): List of column names.
        """
        self.client.insert(table, data, column_names=columns)
        logging.info(f"Inserted {len(data)} rows into Clickhouse table {table}.")

    def enrich(self):
        """Enrich data from PostgreSQL and insert into Clickhouse."""

        campaign_stats = self.fetch_campaign_stats()
        self.insert_into_clickhouse(
            campaign_stats,
            "campaign_stats",
            [
                "campaign_id",
                "campaign_name",
                "advertiser_id",
                "advertiser_name",
                "bid",
                "budget",
                "count_impressions",
                "count_clicks",
            ],
        )

        daily_click_stats = self.fetch_daily_click_stats()
        self.insert_into_clickhouse(
            daily_click_stats, "daily_clicks", ["campaign_id", "date", "count_clicks"]
        )

        daily_impression_stats = self.fetch_daily_impression_stats()
        self.insert_into_clickhouse(
            daily_impression_stats,
            "daily_impressions",
            ["campaign_id", "date", "count_impressions"],
        )
        logging.info("Enrichment process completed.")


if __name__ == "__main__":
    enricher = ClickhouseEnricher()
    enricher.create_tables()
    enricher.enrich()
