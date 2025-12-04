CREATE TABLE `dim_date` (
  `date_key` int PRIMARY KEY,
  `date_value` date NOT NULL,
  `day` int,
  `month` int,
  `year` int
);

CREATE TABLE `dim_linkedin_account` (
  `account_id` int PRIMARY KEY,
  `external_account_id` varchar(255),
  `account_name` varchar(255),
  `account_url` varchar(500)
);

CREATE TABLE `dim_linkedin_content` (
  `content_id` int PRIMARY KEY,
  `external_content_id` varchar(255),
  `title` varchar(500),
  `description` text,
  `url` varchar(1000)
);

CREATE TABLE `fact_linkedin_post_daily` (
  `date_key` int NOT NULL,
  `account_id` int NOT NULL,
  `content_id` int NOT NULL,
  `impressions` int,
  `clicks` int,
  `likes` int,
  `comments` int,
  `shares` int,
  `engagement` int,
  `reach` int,
  `follower_count` int,
  `profile_views_from_post` int,
  PRIMARY KEY (`date_key`, `account_id`, `content_id`)
);

CREATE TABLE `fact_linkedin_account_daily` (
  `date_key` int NOT NULL,
  `account_id` int NOT NULL,
  `follower_count` int,
  `follower_change` int,
  `profile_views` int,
  PRIMARY KEY (`date_key`, `account_id`)
);

ALTER TABLE `fact_linkedin_post_daily` ADD FOREIGN KEY (`date_key`) REFERENCES `dim_date` (`date_key`);

ALTER TABLE `fact_linkedin_post_daily` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_linkedin_account` (`account_id`);

ALTER TABLE `fact_linkedin_post_daily` ADD FOREIGN KEY (`content_id`) REFERENCES `dim_linkedin_content` (`content_id`);

ALTER TABLE `fact_linkedin_account_daily` ADD FOREIGN KEY (`date_key`) REFERENCES `dim_date` (`date_key`);

ALTER TABLE `fact_linkedin_account_daily` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_linkedin_account` (`account_id`);
