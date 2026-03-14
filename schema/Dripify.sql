CREATE TABLE `dim_date` (
  `date_id` int PRIMARY KEY COMMENT 'YYYYMMDD',
  `date` date UNIQUE NOT NULL,
  `year` int,
  `quarter` int,
  `month` int,
  `month_name` varchar(20),
  `week_of_year` int,
  `iso_week` int,
  `week_start_date` date,
  `week_end_date` date,
  `day_of_month` int,
  `day_of_week` int,
  `day_name` varchar(20),
  `is_weekend` boolean,
  `is_holiday` boolean,
  `holiday_name` varchar(50)
);

CREATE TABLE `dim_dripify_account` (
  `account_id` int PRIMARY KEY AUTO_INCREMENT,
  `external_account_id` varchar(255),
  `account_name` varchar(255)
);

CREATE TABLE `dim_dripify_campaign` (
  `campaign_id` int PRIMARY KEY AUTO_INCREMENT,
  `account_id` int NOT NULL,
  `external_campaign_id` varchar(255),
  `campaign_name` varchar(255),
  `status` varchar(50),
  `start_date_id` int,
  `end_date_id` int
);

CREATE TABLE `fact_dripify_campaign_daily` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `campaign_id` int NOT NULL,
  `account_id` int NOT NULL,
  `date_id` int NOT NULL,
  `invites_sent` int,
  `invites_accepted` int,
  `messages_sent` int,
  `messages_responded` int,
  `profiles_viewed` int,
  `acceptance_rate` decimal(8,4),
  `response_rate` decimal(8,4)
);

CREATE TABLE `fact_dripify_campaign_rank_weekly` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `week_start_date_id` int NOT NULL COMMENT 'date_id where date = week_start_date',
  `account_id` int NOT NULL,
  `campaign_id` int NOT NULL,
  `metric_name` varchar(50) NOT NULL COMMENT 'response_rate | invites_accepted | positive replies | negative replies',
  `metric_value` decimal(18,4) NOT NULL,
  `rank_desc` int NOT NULL COMMENT '1..10 => Top 10 campaigns',
  `rank_asc` int NOT NULL COMMENT '1..10 => Bottom 10 campaigns'
);

ALTER TABLE `dim_dripify_campaign` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_dripify_account` (`account_id`);

ALTER TABLE `dim_dripify_campaign` ADD FOREIGN KEY (`start_date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `dim_dripify_campaign` ADD FOREIGN KEY (`end_date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_dripify_campaign_daily` ADD FOREIGN KEY (`campaign_id`) REFERENCES `dim_dripify_campaign` (`campaign_id`);

ALTER TABLE `fact_dripify_campaign_daily` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_dripify_account` (`account_id`);

ALTER TABLE `fact_dripify_campaign_daily` ADD FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_dripify_campaign_rank_weekly` ADD FOREIGN KEY (`week_start_date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_dripify_campaign_rank_weekly` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_dripify_account` (`account_id`);

ALTER TABLE `fact_dripify_campaign_rank_weekly` ADD FOREIGN KEY (`campaign_id`) REFERENCES `dim_dripify_campaign` (`campaign_id`);
