# Big Query Notes 
1. **Standard vs Legacy**: all the examples below are standard 
   - **Standard**: preferred used in big query 
   - **Legacy**: like normal database
2. **Some Syntax** 
   - **Date** 
      - Unit: DAY, WEEK, ISOWEEK, MONTH, QUARTER, YEAR 
      - Current day: CURRENT_DATE() 
      - **DATE_SUB**: Subtracts a specified time interval from a DATE.
        ```
        SELECT DATE_SUB(DATE "2008-12-25", INTERVAL 5 DAY) as five_days_ago;
        ```
      - **_TABLE_SUFFIX**: Specific date range using _TABLE_SUFFIX
        ```
        -- _TABLE_SUFFIX BETWEEN date1 AND date2 , date1 < date2 
        SELECT
        date,
        SUM(totals.visits) AS visits,
        FROM `database_name.session_name.ga_sessions_*`
        WHERE
        _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 3 month)) -- date1
        AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) -- date2 
        ```
      - **DATE_TRUNC**: Format date 
      ```SELECT DATE_TRUNC(DATE '2008-12-25', MONTH) as month;```
   - **Create Temporary Table for later use** 
     ```With t_table as (
        select col1, col2 from sample_table1
        union 
        select col1, col2 from sample_table2) 
        select col1, col2 from t_table 
     ```
 3. **Google Analytic Specific**
    - All table being store like ga_sessions_[date], best way to approach it is using **ga_sessions_*** with _TABLE_SUFFIX
      ```
      SELECTcol1 
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE
      _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
      ```
    - Session vs hint 
      - Session level: a series of action/visit
      - hint: action 
      ```SELECT
        col
        FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) as hits ```
    - **Bounce rate: the percentage of visits with a single pageview**
      ```
      SELECT
      trafficSource.source AS source,
      COUNT ( trafficSource.source ) AS total_visits,
      SUM ( totals.bounces ) AS total_no_of_bounces, 
      SUM ( totals.bounces )/COUNT ( trafficSource.source ) AS bounce_date
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE
      _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
      GROUP BY
      source 
      order by total_visits desc
      ```
   - **Average number of product pageviews by purchaser type/schedule type (purchasers vs non-purchasers)**
     ```
     #standardSQL
      SELECT
      (SUM (total_transactions_per_user) / COUNT(fullVisitorId) ) AS avg_total_transactions_per_user
      FROM (
      SELECT
      fullVisitorId,
      SUM (totals.transactions) AS total_transactions_per_user
      FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE
      _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
      AND totals.transactions IS NOT NULL
      GROUP BY
      fullVisitorId )
     ```
   - **What is the average amount of money/time spent per session in July 2017?**
      ```
      SELECT
      ( SUM(total_transactionrevenue_per_user) / SUM(total_visits_per_user) ) AS
      avg_revenue_by_user_per_visit
      FROM (
      SELECT
      fullVisitorId,
      SUM( totals.visits ) AS total_visits_per_user,
      SUM( totals.transactionRevenue ) AS total_transactionrevenue_per_user
      FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE
      _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
      AND
      totals.visits > 0
      AND totals.transactions >= 1
      AND totals.transactionRevenue IS NOT NULL
      GROUP BY
      fullVisitorId )```
  - **What is the sequence of pages viewed?**
      ```
      SELECT
      fullVisitorId,
      visitId,
      visitNumber, -- The session number for this user. If this is the first session, then this is set to 1.
      hits.hitNumber AS hitNumber, -- The sequenced hit number. For the first hit of each session, this is set to 1.
      hits.page.pagePath AS pagePath 
      FROM
      `lennar-45456.15202023.ga_sessions_*`,
      UNNEST(hits) as hits
      WHERE
      _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
      AND
      hits.type="PAGE"
      ORDER BY
      fullVisitorId,
      visitId,
      visitNumber,
      hitNumber
    ```
     
   
