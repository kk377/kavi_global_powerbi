CREATE TABLE `dim_date` (
  `date_key` int PRIMARY KEY,
  `date_value` date NOT NULL,
  `day` int,
  `month` int,
  `year` int
);

CREATE TABLE `dim_dripify_account` (
  `account_id` int PRIMARY KEY,
  `external_account_id` varchar(255),
  `account_name` varchar(255)
);

CREATE TABLE `fact_dripify_account_daily` (
  `date_key` int NOT NULL,
  `account_id` int NOT NULL,
  `invites_sent` int,
  `invites_accepted` int,
  `messages_sent` int,
  `messages_responded` int,
  `positive_replies` int,
  `negative_replies` int,
  `meetings_booked` int,
  `profiles_viewed` int,
  PRIMARY KEY (`date_key`, `account_id`)
);

ALTER TABLE `fact_dripify_account_daily` ADD FOREIGN KEY (`date_key`) REFERENCES `dim_date` (`date_key`);

ALTER TABLE `fact_dripify_account_daily` ADD FOREIGN KEY (`account_id`) REFERENCES `dim_dripify_account` (`account_id`);
