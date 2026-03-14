CREATE TABLE `fact_facebook_page_daily` (
  `page_id` int PRIMARY KEY AUTO_INCREMENT,
  `date_id` int NOT NULL,
  `user_id` int NOT NULL,
  `page_followers_total` bigint,
  `page_followers_netchange` int,
  `profile_views` int
);

CREATE TABLE `fact_facebook_post_daily` (
  `post_id` int PRIMARY KEY AUTO_INCREMENT,
  `date_id` int NOT NULL,
  `user_id` int NOT NULL,
  `page_id` int NOT NULL,
  `post_impressions` bigint,
  `post_reach` int,
  `post_clicks` int,
  `post_reactions_total` int,
  `post_comments` varchar(255),
  `post_shares` int,
  `post_engagements_total` int
);

CREATE TABLE `fact_facebook_post_rank_weekly` (
  `post_id` int PRIMARY KEY AUTO_INCREMENT,
  `week_start_date_id` int NOT NULL COMMENT 'date_id where date = week_start_date',
  `user_id` int NOT NULL,
  `page_id` int NOT NULL,
  `metric_name` varchar(50) NOT NULL COMMENT 'impressions | engagements_total | engagement_rate | clicks | reach',
  `metric_value` decimal(18,4) NOT NULL,
  `rank_desc` int NOT NULL COMMENT '1..10 => Top 10',
  `rank_asc` int NOT NULL COMMENT '1..10 => Bottom 10'
);

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

ALTER TABLE `fact_facebook_page_daily` ADD FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_facebook_page_daily` ADD FOREIGN KEY (`user_id`) REFERENCES `dim_user` (`user_id`);

ALTER TABLE `fact_facebook_post_daily` ADD FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_facebook_post_daily` ADD FOREIGN KEY (`user_id`) REFERENCES `dim_user` (`user_id`);

ALTER TABLE `fact_facebook_post_daily` ADD FOREIGN KEY (`page_id`) REFERENCES `fact_facebook_page_daily` (`page_id`);

ALTER TABLE `fact_facebook_post_rank_weekly` ADD FOREIGN KEY (`week_start_date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_facebook_post_rank_weekly` ADD FOREIGN KEY (`user_id`) REFERENCES `dim_user` (`user_id`);

ALTER TABLE `fact_facebook_post_rank_weekly` ADD FOREIGN KEY (`page_id`) REFERENCES `fact_facebook_page_daily` (`page_id`);
