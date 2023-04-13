-- Kevin Hitt
-- ISM 6562 - M1.6 Assignment: Hive/Pig/Impala

--*******************************************
--Load this data in HDFS

-- (terminal) Show HDFS directory 
-- hdfs dfs -ls /user/cloudera/data/

-- (terminal) Show HDFS file
-- hdfs dfs -cat /user/cloudera/data/output/cricket_data_final9_merged.csv

-- (formatted dates prior to upload)

SELECT * FROM `default`.`crick3t` LIMIT 15;

--*******************************************
--Run the following query in Hive or Impala
--Total number of ODI played during the time frame
SELECT COUNT(*)
FROM cricket


SELECT count(*)
FROM crick3t 
WHERE match_date
BETWEEN '1960-01-01'
    AND '1990-01-01';

--Total number of Teams
SELECT count(team_name) FROM
(
  SELECT team1 AS team_name FROM crick3t
  UNION
  SELECT team2 AS team_name FROM crick3t
) crick3t

--*******************************************
--Run the following analysis
--Which team won the most ODI during the time-frame? 
-- (think of ODI as the Olympics of cricket held every four years)
SELECT winner, COUNT(*) AS num_wins 
FROM crick3t
WHERE scorecard LIKE 'ODI%' 
GROUP BY winner 
ORDER BY num_wins 
DESC LIMIT 1;

--Which team won the most ODI during each decade 71-80, 81-90, 91-2000, 2001-2010, 2011-2018?
SELECT
  decade_till_2018,
  winner,
  odi_wins
FROM
  (
    SELECT
      FLOOR(YEAR(match_date) / 10) * 10 AS decade_till_2018,
      winner,
      COUNT(*) AS odi_wins,
      ROW_NUMBER() OVER (PARTITION BY FLOOR(YEAR(match_date) / 10) * 10 ORDER BY COUNT(*) DESC) AS rank
    FROM
      crick3t
    GROUP BY
      decade_till_2018,
      winner
  ) subquery
WHERE
  rank = 1
ORDER BY
  decade_till_2018 DESC



