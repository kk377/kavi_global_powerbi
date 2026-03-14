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

CREATE TABLE `dim_channel` (
  `channel_id` varchar(255) PRIMARY KEY,
  `channel_name` varchar(255),
  `category` varchar(255),
  `created_at` date,
  `country` varchar(2),
  `subscriber_count` bigint,
  `total_videos` int,
  `is_verified` boolean,
  `custom_url` varchar(100),
  `description` text,
  `banner_url` varchar(500)
);

CREATE TABLE `dim_video` (
  `video_id` varchar(255) PRIMARY KEY,
  `channel_id` varchar(255) NOT NULL,
  `title` varchar(200),
  `description` text,
  `publish_date` date,
  `duration_seconds` int,
  `tags` text,
  `thumbnail_url` varchar(500),
  `language` varchar(10),
  `is_monetized` boolean,
  `category_id` varchar(255),
  `privacy_status` varchar(20),
  `is_live` boolean
);

CREATE TABLE `dim_content_length` (
  `content_length_category` varchar(255) PRIMARY KEY COMMENT 'short | medium | long (or your own bins)',
  `definition` varchar(255)
);

CREATE TABLE `fact_youtube_video_daily` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `video_id` varchar(255) NOT NULL,
  `channel_id` varchar(255) NOT NULL,
  `date_id` int NOT NULL,
  `content_length_category` varchar(255),
  `views` bigint,
  `unique_views` int,
  `watch_time_hours` decimal(12,2),
  `avg_view_duration` decimal(8,2),
  `impression` int,
  `likes` int,
  `comments` int,
  `shares` int,
  `engagements_total` int COMMENT 'likes + comments + shares (and optionally other interactions if available)',
  `retention_rate` decimal(5,4),
  `subscribers_gained` int,
  `is_live` boolean
);

CREATE TABLE `fact_youtube_channel_daily` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `channel_id` varchar(255) NOT NULL,
  `date_id` int NOT NULL,
  `channel_views` bigint,
  `channel_watch_time_hours` decimal(12,2),
  `channel_subscribers_gained` int,
  `channel_subscribers_lost` int,
  `channel_subscribers_net_change` int,
  `channel_videos_published` int
);

CREATE TABLE `fact_youtube_video_rank_weekly` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `video_id` varchar(255) NOT NULL,
  `channel_id` varchar(255) NOT NULL,
  `week_start_date_id` int NOT NULL COMMENT 'date_id where date = week_start_date',
  `metric_name` varchar(50) NOT NULL COMMENT 'views | watch_time_hours | engagements_total | engagement_rate | impression_ctr | retention_rate | subscribers_gained',
  `metric_value` decimal(18,4) NOT NULL,
  `rank_desc` int NOT NULL COMMENT '1..10 => Top 10',
  `rank_asc` int NOT NULL COMMENT '1..10 => Bottom 10'
);

ALTER TABLE `dim_video` ADD FOREIGN KEY (`channel_id`) REFERENCES `dim_channel` (`channel_id`);

ALTER TABLE `fact_youtube_video_daily` ADD FOREIGN KEY (`video_id`) REFERENCES `dim_video` (`video_id`);

ALTER TABLE `fact_youtube_video_daily` ADD FOREIGN KEY (`channel_id`) REFERENCES `dim_channel` (`channel_id`);

ALTER TABLE `fact_youtube_video_daily` ADD FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_youtube_video_daily` ADD FOREIGN KEY (`content_length_category`) REFERENCES `dim_content_length` (`content_length_category`);

ALTER TABLE `fact_youtube_channel_daily` ADD FOREIGN KEY (`channel_id`) REFERENCES `dim_channel` (`channel_id`);

ALTER TABLE `fact_youtube_channel_daily` ADD FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date_id`);

ALTER TABLE `fact_youtube_video_rank_weekly` ADD FOREIGN KEY (`video_id`) REFERENCES `dim_video` (`video_id`);

ALTER TABLE `fact_youtube_video_rank_weekly` ADD FOREIGN KEY (`channel_id`) REFERENCES `dim_channel` (`channel_id`);

ALTER TABLE `fact_youtube_video_rank_weekly` ADD FOREIGN KEY (`week_start_date_id`) REFERENCES `dim_date` (`date_id`);
