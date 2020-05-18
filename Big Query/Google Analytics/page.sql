-- pimary page 
SELECT
  hits.page.pagePath
FROM
`lennar-45456.15202023.ga_sessions_*` AS GA,
 UNNEST(GA.hits) AS hits
 WHERE
and  _TABLE_SUFFIX BETWEEN '20200501' AND '20200518'
GROUP BY
  hits.page.pagePath


-- hit number for each URL 
SELECT
hits.page.pagePath,
COUNT(*) AS pageviews
FROM
`lennar-45456.15202023.ga_sessions_*` AS GA,
UNNEST(GA.hits) AS hits
WHERE
_TABLE_SUFFIX BETWEEN '20200101' AND '20200518' 
AND
hits.type = 'PAGE'
GROUP BY
hits.page.pagePath
ORDER BY
pageviews DESC

-- unique pageview per (session + visitStartTime)
SELECT
  pagepath,
  COUNT(*) AS pageviews,
  COUNT(DISTINCT session_id) AS unique_pageviews
FROM (
  SELECT
    hits.page.pagePath,
    CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id
  FROM
    `lennar-45456.15202023.ga_sessions_*` AS GA,
    UNNEST(GA.hits) AS hits
  WHERE
    hits.type = 'PAGE'
    and  _TABLE_SUFFIX BETWEEN '20200501' AND '20200518')
GROUP BY
  pagePath
ORDER BY
  pageviews DESC

-- Average time on page 
-- SUBQUERY 1:  sessionid, url, hit_time, end_time 
SELECT
fullVisitorId,
visitStartTime,
pagePath,
hit_time,
LEAD(hit_time) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time) AS next_pageview
FROM (
  SELECT
    fullVisitorId,
    visitStartTime,
    hits.page.pagePath,
    hits.time / 1000 AS hit_time
  FROM
    `lennar-45456.15202023.ga_sessions_*` AS GA, 
    UNNEST(GA.hits) AS hits
  WHERE
    hits.type = 'PAGE'
    and  _TABLE_SUFFIX BETWEEN '20200501' AND '20200518')

-- SUBQUERY2: 
SELECT
  fullVisitorId,
  visitStartTime,
  hits.page.pagePath,
  MAX(IF(hits.isInteraction IS NOT NULL,
      hits.time,
      0)) OVER (PARTITION BY fullVisitorId, visitStartTime) as last_interaction
FROM
  'bigquery-public-data.google_analytics_sample.ga_sessions_20160801',
  UNNEST(hits) AS hits
WHERE
  hits.type = 'PAGE'

-- COMBIME SUBQUERY1,2
SELECT
  fullVisitorId,
  visitStartTime,
  pagePath,
  hit_time,
  type,
  isExit,
  last_interaction,
  LEAD(hit_time) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time) AS next_pageview
FROM (
  SELECT
    fullVisitorId,
    visitStartTime,
    pagePath,
    hit_time,
    type,
    isExit,
    last_interaction
  FROM (
    SELECT
      fullVisitorId,
      visitStartTime,
      hits.page.pagePath,
      hits.type,
      hits.isExit,
      hits.time / 1000 AS hit_time,
      MAX(IF(hits.isInteraction IS NOT NULL,
          hits.time / 1000,
          0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS last_interaction
    FROM
      'bigquery-public-data.google_analytics_sample.ga_sessions_20160801' AS GA,
      UNNEST(GA.hits) AS hits)
  WHERE
    type = 'PAGE')

-- CALCUALTE TIME PER PAGE 
-- we compute the time on page as the difference between the timestamp of the last interaction hit minus the timestamp of the pageview hit.
SELECT
  fullVisitorId,
  visitStartTime,
  pagePath,
  hit_time,
  type,
  isExit,
  CASE
    WHEN isExit IS NOT NULL THEN last_interaction - hit_time -- if exist, count based on last interation time 
    ELSE next_pageview - hit_time -- if not exist, calculate based on next page 
  END AS time_on_page
FROM (
  SELECT
    fullVisitorId,
    visitStartTime,
    pagePath,
    hit_time,
    type,
    isExit,
    last_interaction,
    LEAD(hit_time) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time) AS next_pageview -- session next 
  FROM (
    SELECT
      fullVisitorId,
      visitStartTime,
      pagePath,
      hit_time,
      type,
      isExit,
      last_interaction
    FROM (
      SELECT
        fullVisitorId,
        visitStartTime,
        hits.page.pagePath,
        hits.type,
        hits.isExit,
        hits.time / 1000 AS hit_time,
        MAX(IF(hits.isInteraction IS NOT NULL,
            hits.time / 1000,
            0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS last_interaction -- session last interaction time 
      FROM
        `lennar-45456.15202023.ga_sessions_*` AS GA, 
        UNNEST(GA.hits) AS hits
        WHERE
        hits.type = 'PAGE'
        and  _TABLE_SUFFIX BETWEEN '20200501' AND '20200518'))
        )

-- Entrance 
-- If this hit was the first pageview or screenview hit of a session, this is set to true.
SELECT
  pagePath,
  SUM(entrances) AS entrances
FROM (
  SELECT
    hits.page.pagePath,
    CASE
      WHEN hits.isEntrance IS NOT NULL THEN 1
      ELSE 0
    END AS entrances
  FROM
    'bigquery-public-data.google_analytics_sample.ga_sessions_20160801' AS GA,
    UNNEST(GA.hits) AS hits)
GROUP BY
  pagePath
ORDER BY
  entrances DESC

-- Bounces 
-- Bounces are attributed to the first interaction hit in a session in which there is exactly one interaction event.
SELECT
  fullVisitorId,
  visitStartTime,
  pagePath,
  CASE
    WHEN hitNumber = first_interaction THEN bounces
    ELSE 0
  END AS bounces
FROM (
    SELECT
    fullVisitorId,
    visitStartTime,
    hits.page.pagePath,
    totals.bounces,
    hits.hitNumber,
    MIN(IF(hits.isInteraction IS NOT NULL,
        hits.hitNumber,
        0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction
  FROM
    'bigquery-public-data.google_analytics_sample.ga_sessions_20160801' AS GA,
    UNNEST(GA.hits) AS hits)

-- Sessions are attributed to the first hit (interaction or not) in a session where there is at least one interaction event.
SELECT
    fullVisitorId,
    visitStartTime,
    pagePath,
    CASE
    WHEN hitNumber = first_hit THEN visits
    ELSE 0
    END AS sessions
FROM (
    SELECT
    fullVisitorId,
    visitStartTime,
    hits.page.pagePath,
    totals.visits,
    hits.hitNumber,
    MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_hit
  FROM
    'bigquery-public-data.google_analytics_sample.ga_sessions_20160801' AS GA,
    UNNEST(GA.hits) AS hits)
-- bounce rate 
select
pagePath,
bounces,
sessions,
CASE
    WHEN sessions = 0 THEN 0
    ELSE bounces / sessions
  END AS bounce_rate
  from (
SELECT
  pagePath,
  SUM(bounces) AS bounces,
  SUM(sessions) AS sessions
FROM (
  SELECT
    fullVisitorId,
    visitStartTime,
    pagePath,
    CASE
      WHEN hitNumber = first_interaction THEN bounces
      ELSE 0
    END AS bounces,
    CASE
      WHEN hitNumber = first_hit THEN visits
      ELSE 0
    END AS sessions
  FROM (
    SELECT
      fullVisitorId,
      visitStartTime,
      hits.page.pagePath,
      totals.bounces,
      totals.visits,
      hits.hitNumber,
      MIN(IF(hits.isInteraction IS NOT NULL,
          hits.hitNumber,
          0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction,
      MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_hit
    FROM
      'bigquery-public-data.google_analytics_sample.ga_sessions_20160801' AS GA,
      UNNEST(GA.hits) AS hits))
GROUP BY
  pagePath)
ORDER BY
  sessions DESC

  -- %Exit/ pageviews 
  SELECT
  pagePath,
  pageviews,
  exits,
  CASE
    WHEN pageviews = 0 THEN 0
    ELSE exits / pageviews
  END AS exit_rate
FROM (
  SELECT
    pagepath,
    COUNT(*) AS pageviews,
    SUM(exits) AS exits
  FROM (
    SELECT
      hits.page.pagePath,
      CASE
        WHEN hits.isExit IS NOT NULL THEN 1
        ELSE 0
      END AS exits
    FROM
      'bigquery-public-data.google_analytics_sample.ga_sessions_20160801' AS GA,
      UNNEST(GA.hits) AS hits
    WHERE
      hits.type = 'PAGE')
  GROUP BY
    pagePath)
ORDER BY
  pageviews DESC