DROP TABLE IF EXISTS campaign_stats;
CREATE TABLE campaign_stats
(
    campaign_id UInt32 NOT NULL,
    campaign_name String,
    advertiser_id UInt32 NOT NULL,
    advertiser_name String,
    bid Float32 NOT NULL,
    budget Float32 NOT NULL,
    count_impressions UInt32 NOT NULL,
    count_clicks UInt32 NOT NULL,
)
ENGINE = MergeTree()
PRIMARY KEY (campaign_id);

DROP TABLE IF EXISTS daily_clicks;
CREATE TABLE daily_clicks
(
    campaign_id UInt32 NOT NULL,
    date Date NOT NULL,
    count_clicks UInt32 NOT NULL,
)
ENGINE = MergeTree()
PRIMARY KEY (campaign_id, date);

DROP TABLE IF EXISTS daily_impressions;
CREATE TABLE daily_impressions
(
    campaign_id UInt32 NOT NULL,
    date Date NOT NULL,
    count_impressions UInt32 NOT NULL,
)
ENGINE = MergeTree()
PRIMARY KEY (campaign_id, date);