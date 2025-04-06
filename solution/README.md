## Content

* clickhouse_schema.sql - Tables definition for analytical ClickHouse DB
* clickhouse_enrichment.py - Pipeline for enrichment of ClickHouse DB with data from available operational PostgreSQL DB
* analytics.py - Script containing and executing KPI querries

## ClickHouse DB schema

Schema contains three tables, which are used to calculate required KPIs. Since it is an analytical DB, it is denormalized for better readability and easier and faster OLAP queries performance, avoiding commonly-used multiple JOINs. A drawback is presence of some redundant columns, which requires additional storage space.

### campaign_stats
Information on campaign level, mainly  on 'campaign' table of PostgreSQL DB. Advertiser name is added for better readability. It contains aggregated values for impressions and clicks per campaign, to easily calculate needed KPIs.

### impressions
Impressions data aggregated on daily basis to query required KPIs.

### clicks
Same as above, but for clicks.

## Setup

1. Set up DB instances and populate PostgreSQL DB, as described in the task. The docker-compose file has minor adjustments: ClickHouse DB is initialized with default user and password, the same values are used for enrichments. Passwordless authentication was not possible from used Python solution.

2. (Not needed for using test setup) If needed, adjust values of environmental variables for Postgre and ClickHouse credentials in the Dockerfile.

3. In your terminal navigate to 'solution' directory and build a docker image:
`sh
docker build . -t adtech
`






