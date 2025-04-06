## Content

* **clickhouse_schema.sql** - Tables definition for analytical ClickHouse DB
* **clickhouse_enrichment.py** - Pipeline for enrichment of ClickHouse DB with data from available operational PostgreSQL DB
* **analytics.py** - Script containing and executing KPI queries

## ClickHouse DB schema

Schema contains three tables, which are used to calculate required KPIs. Since it is an analytical DB, it is denormalized for better readability and easier and faster OLAP queries, avoiding commonly-used multiple joins. A drawback is a presence of some redundant columns, which requires additional storage space and would be a problem for an OLTP DB.

**campaign_stats** - Information on campaign level. It is mainly based on 'campaign' table of PostgreSQL DB. Advertiser name is added for better readability. It contains aggregated values for impressions and clicks per campaign, to easily calculate needed KPIs.

**impressions** - Impressions data aggregated on daily basis to query required KPIs.

**clicks** - Same as above, but for clicks.

## Setup

1. Set up DB instances and populate PostgreSQL DB, as described in the task. Please, use the **docker-compose.yaml** from this fork, as it has a adjustment: ClickHouse DB is initialized with default user and password, the same values are used for enrichments. Passwordless authentication was not possible from used Python solution and will throw an error.

2. (Only to use with other DB instances) If needed, adjust values of environmental variables for Postgre and ClickHouse credentials in the Dockerfile.

3. Navigate to 'solution' directory and build a docker image: `docker build . -t adtech`


4. Run the container. `--network="host"` argument is needed to access local DB instances.
`docker run -it --network="host" adtech`

5. To run ClickHouse enrichment pipeline: `python clickhouse_enrichment.py`

6. To run analytical queries: `python analytics.py`
This script has 3 optional arguments:
--campaign_ids - to analyze only specific campaigns, e.g.`python analytics.py --campaign_ids 22 33 34`
--date_start - Start date for analysis (YYYY-MM-DD)
--date_end - End date for analysis (YYYY-MM-DD)
Date arguments will apply only to fetch daily clicks and impressions, they will not affect other queries. Example:
`python analytics.py --date_start 2025-04-03 --date_end 2025-04-04`


## KPIs
Specified in 'analytics.py'

1. CTR - fetches CTR values for each campaign as clicks / impressions.

2. Daily clicks and impressions for each campaign. 

3. Cost and budget information. Additional query, fetching occurred campaign costs. It is assumed, that bid is defined as amount paid for 1000 impressions, as it is not specified in the task. Query also returns percentages of the budgets spend, and identifies campaigns, which are over their budgets.
