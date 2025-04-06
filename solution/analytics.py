import clickhouse_connect
import logging
import os

from tabulate import tabulate

logging.basicConfig(level=logging.INFO)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PW = os.environ.get("CLICKHOUSE_PW", "default")


class ClickhouseAnaliser:
    def __init__(self, date_start: str = None, date_end: str = None, campaign_ids: list = None):
        self.client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, username=CLICKHOUSE_USER, password=CLICKHOUSE_PW
        )
        self.date_start = date_start
        self.date_end = date_end
        self.campaign_ids = campaign_ids

    def show_ctr(self):
        """Show CTR for all or selected campaigns."""
        query = """
            SELECT 
                campaign_id,
                campaign_name, 
                count_clicks / count_impressions AS ctr
            FROM
                campaign_stats
        """

        if self.campaign_ids:
            ids = ", ".join(map(str, self.campaign_ids))
            query += f" WHERE campaign_id IN ({ids})"
        query += " ORDER BY campaign_id"
        result = self.client.query(query)
        print("CTR for each campaign:")
        print(tabulate(result.result_rows, headers=result.column_names, tablefmt="psql"))

    def show_daily_stats(self):
        """
        Show daily impressions and clicks for all or selected campaigns.
        """

        query = """
            SELECT 
                c.campaign_id,
                c.date,
                i.count_impressions,
                c.count_clicks,
            FROM
                daily_clicks c
            JOIN
                daily_impressions i ON c.campaign_id = i.campaign_id AND c.date = i.date 
        """

        if self.date_start or self.date_end or self.campaign_ids:
            query += " WHERE "
            conditions = []
            if self.date_start:
                conditions.append(f"c.date >= '{self.date_start}'")
            if self.date_end:
                conditions.append(f"c.date <= '{self.date_end}'")
            if self.campaign_ids:
                ids = ", ".join(map(str, self.campaign_ids))
                conditions.append(f"c.campaign_id IN ({ids})")
            query += " AND ".join(conditions)
        query += " ORDER BY c.campaign_id, c.date"

        result = self.client.query(query)
        print("Daily impressions and clicks:")
        print(tabulate(result.result_rows, headers=result.column_names, tablefmt="psql"))

    def show_campaigns_costs(self):
        """Show campaign costs to identifiy ones over budget."""
        query = """
            SELECT 
                campaign_id,
                campaign_name, 
                bid, 
                bid * (count_impressions / 1000) AS actual_cost,
                count_impressions,
                budget,
                (actual_cost / budget) * 100 AS percentage_budget_spent,
                CASE 
                    WHEN actual_cost > budget THEN 'Over Budget'
                    ELSE 'Within Budget'
                END AS budget_status
            FROM
                campaign_stats
        """

        if self.campaign_ids:
            ids = ", ".join(map(str, self.campaign_ids))
            query += f" WHERE campaign_id IN ({ids})"
        query += " ORDER BY percentage_budget_spent DESC"

        result = self.client.query(query)
        count_over_budget = sum(1 for row in result.result_rows if row[-1] == "Over Budget")
        if count_over_budget > 0:
            print(f"There are {count_over_budget} campaigns over budget")
        print(f"Total campaigns: {len(result.result_rows)}")
        print("Costs for each campaign:")
        print(
            tabulate(
                result.result_rows, tablefmt="psql", headers=result.column_names, floatfmt=".2f"
            )
        )

    def analyse_campaigns(self):
        """Analyse campaigns."""
        if self.date_start and self.date_end:
            print(f"Date range for daily statistics: {self.date_start} to {self.date_end}")
        elif self.date_start:
            print(f"Date start for daily statistics: {self.date_start}")
        elif self.date_end:
            print(f"Date end for daily statistics: {self.date_end}")
        else:
            print("No date range specified for daily statistics.")
        if self.campaign_ids:
            print(f"Campaign IDs for analysis: {self.campaign_ids}")
        else:
            print("Analysing all campaigns.")
        print("\n")
        self.show_ctr()
        print("\n")
        self.show_daily_stats()
        print("\n")
        self.show_campaigns_costs()
        print("\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analyse campaigns.")
    parser.add_argument("--date_start", type=str, help="Start date for analysis (YYYY-MM-DD)")
    parser.add_argument("--date_end", type=str, help="End date for analysis (YYYY-MM-DD)")
    parser.add_argument(
        "--campaign_ids", nargs="+", type=int, help="List of campaign IDs to analyse"
    )

    args = parser.parse_args()

    analyser = ClickhouseAnaliser(args.date_start, args.date_end, args.campaign_ids)
    analyser.analyse_campaigns()
