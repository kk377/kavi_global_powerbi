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

CREATE TABLE `dim_linkedin_account` (
  `account_id` int PRIMARY KEY AUTO_INCREMENT,
  `external_account_id` varchar(255),
  `account_name` varchar(255),
  `account_url` varchar(500)
);

CREATE TABLE `dim_linkedin_content` (
  `content_id` int PRIMARY KEY AUTO_INCREMENT,
  `external_content_id` varchar(255),
  `title` varchar(500),
  `description` text,
  `url` varchar(1000)
);

CREATE TABLE `fact_linkedin_account_daily` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `account_id` int NOT NULL,
  `date_id` int NOT NULL,
  `followers_total` int,
  `followers_net_change` int,
  `profile_views` int
);

CREATE TABLE `fact_linkedin_post_daily` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `account_id` int NOT NULL,
  `content_id` int NOT NULL,
  `date_id` int NOT NULL,
  `impressions` int,
  `reach` int,
  `clicks` int,
  `likes` int,
  `comments` int,
  `shares` int,
  `reposts` int,
  `engagements_total` int COMMENT 'recommended = likes + comments + shares + reposts + clicks',
  `profile_views_from_post` int
);

CREATE TABLE `fact_linkedin_post_rank_weekly` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `week_start_date_id` int NOT NULL COMMENT 'date_id where date = week_start_date',
  `account_id` int NOT NULL,
  `content_id` int NOT NULL,
  `metric_name` varchar(50) NOT NULL COMMENT 'impressions | engagements_total | engagement_rate | clicks | reach',
  `metric_value` decimal(18,4) NOT NULL,
  `rank_desc` int NOT NULL COMMENT '1..10 => Top 10',
  `rank_asc` int NOT NULL COMMENT '1..10 => Bottom 10'
);

ALTER TABLE `fact_linkedin_account_daily` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_linkedin_account` (`account_id`);

ALTER TABLE `fact_linkedin_account_daily` ADD FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_linkedin_post_daily` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_linkedin_account` (`account_id`);

ALTER TABLE `fact_linkedin_post_daily` ADD FOREIGN KEY (`content_id`) REFERENCES `dim_linkedin_content` (`content_id`);

ALTER TABLE `fact_linkedin_post_daily` ADD FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_linkedin_post_rank_weekly` ADD FOREIGN KEY (`week_start_date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_linkedin_post_rank_weekly` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_linkedin_account` (`account_id`);

ALTER TABLE `fact_linkedin_post_rank_weekly` ADD FOREIGN KEY (`content_id`) REFERENCES `dim_linkedin_content` (`content_id`);

ALTER TABLE `dim_linkedin_content` ADD FOREIGN KEY (`external_content_id`) REFERENCES `dim_linkedin_content` (`content_id`);
