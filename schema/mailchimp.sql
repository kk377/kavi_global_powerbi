CREATE TABLE `dim_user` (
  `user_id` int PRIMARY KEY,
  `user_name` varchar(255),
  `user_email` varchar(255)
);

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

CREATE TABLE `dim_mailchimp_audience` (
  `audience_id` int PRIMARY KEY AUTO_INCREMENT,
  `mailchimp_list_id` varchar(50) UNIQUE NOT NULL COMMENT 'Mailchimp list_id',
  `audience_name` varchar(255),
  `created_at` datetime
);

CREATE TABLE `dim_mailchimp_campaign` (
  `campaign_id` int PRIMARY KEY AUTO_INCREMENT,
  `mailchimp_campaign_id` varchar(50) UNIQUE NOT NULL COMMENT 'Mailchimp campaign_id',
  `campaign_name` varchar(255),
  `campaign_type` varchar(50) COMMENT 'regular | plaintext | absplit | rss | variate | etc.',
  `send_time` datetime,
  `subject_line` varchar(255),
  `from_name` varchar(255),
  `reply_to` varchar(255),
  `archive_url` varchar(500)
);

CREATE TABLE `fact_mailchimp_audience_monthly` (
  `audience_monthly_id` int PRIMARY KEY AUTO_INCREMENT,
  `month_start_date_id` int NOT NULL COMMENT 'date_id where date = first day of month',
  `user_id` int NOT NULL,
  `audience_id` int NOT NULL,
  `subscribers_total` int COMMENT 'end-of-month subscriber count (if available)',
  `subscribers_netchange` int COMMENT 'monthly net change',
  `new_subscribers` int,
  `unsubscribes` int,
  `cleaned` int COMMENT 'cleaned addresses (bounces etc.)'
);

CREATE TABLE `fact_mailchimp_campaign_monthly` (
  `campaign_monthly_id` int PRIMARY KEY AUTO_INCREMENT,
  `month_start_date_id` int NOT NULL COMMENT 'date_id where date = first day of month (usually send month)',
  `user_id` int NOT NULL,
  `audience_id` int NOT NULL,
  `campaign_id` int NOT NULL,
  `emails_sent` int,
  `opens_total` int,
  `unique_opens` int,
  `clicks_total` int,
  `unique_clicks` int,
  `bounces_hard` int,
  `bounces_soft` int,
  `unsubscribes` int,
  `abuse_reports` int,
  `open_rate` decimal(9,6) COMMENT 'unique_opens / emails_delivered',
  `click_rate` decimal(9,6) COMMENT 'unique_clicks / emails_delivered',
  `click_to_open_rate` decimal(9,6) COMMENT 'unique_clicks / unique_opens'
);

CREATE TABLE `fact_mailchimp_campaign_rank_monthly` (
  `campaign_rank_monthly_id` int PRIMARY KEY AUTO_INCREMENT,
  `month_start_date_id` int NOT NULL COMMENT 'date_id where date = first day of month',
  `user_id` int NOT NULL,
  `audience_id` int NOT NULL,
  `campaign_id` int NOT NULL,
  `metric_name` varchar(50) NOT NULL COMMENT 'open_rate | click_rate | cto_rate | unique_opens | unique_clicks | unsubscribes | emails_sent',
  `metric_value` decimal(18,4) NOT NULL,
  `rank_desc` int NOT NULL COMMENT '1..10 => Top 10',
  `rank_asc` int NOT NULL COMMENT '1..10 => Bottom 10'
);

ALTER TABLE `fact_mailchimp_audience_monthly` ADD FOREIGN KEY (`month_start_date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_mailchimp_audience_monthly` ADD FOREIGN KEY (`user_id`) REFERENCES `dim_user` (`user_id`);

ALTER TABLE `fact_mailchimp_audience_monthly` ADD FOREIGN KEY (`audience_id`) REFERENCES `dim_mailchimp_audience` (`audience_id`);

ALTER TABLE `fact_mailchimp_campaign_monthly` ADD FOREIGN KEY (`month_start_date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_mailchimp_campaign_monthly` ADD FOREIGN KEY (`user_id`) REFERENCES `dim_user` (`user_id`);

ALTER TABLE `fact_mailchimp_campaign_monthly` ADD FOREIGN KEY (`audience_id`) REFERENCES `dim_mailchimp_audience` (`audience_id`);

ALTER TABLE `fact_mailchimp_campaign_monthly` ADD FOREIGN KEY (`campaign_id`) REFERENCES `dim_mailchimp_campaign` (`campaign_id`);

ALTER TABLE `fact_mailchimp_campaign_rank_monthly` ADD FOREIGN KEY (`month_start_date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_mailchimp_campaign_rank_monthly` ADD FOREIGN KEY (`user_id`) REFERENCES `dim_user` (`user_id`);

ALTER TABLE `fact_mailchimp_campaign_rank_monthly` ADD FOREIGN KEY (`audience_id`) REFERENCES `dim_mailchimp_audience` (`audience_id`);

ALTER TABLE `fact_mailchimp_campaign_rank_monthly` ADD FOREIGN KEY (`campaign_id`) REFERENCES `dim_mailchimp_campaign` (`campaign_id`);
