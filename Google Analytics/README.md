# Google Analytic Common metrics 
## Replicating The Google Analytics All Pages Report In BigQuery
1. **Page**
    - page dimension or URL is stored in [hit.page.pagePath]
      ```
      SELECT
      hits.page.pagePath
      FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS GA,
      UNNEST(GA.hits) AS hits
      WHERE
      _TABLE_SUFFIX BETWEEN '20200101' AND '20200518'
      GROUP BY
      hits.page.pagePath
      ```
    - **Pageview(hit level)**: how many hit for each page 
      ```
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
      ```
    - **Unique pageview(session + visitStartTime)** 
      - visitedID: Session level. However this identifier has some issues when a session ends a midnight. 
      - We can use fullVisitorId + visitStartTime as unique indentify for session 
      ```
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
      and _TABLE_SUFFIX BETWEEN '20200101' AND '20200518')
      GROUP BY
      pagePath
      ORDER BY
      pageviews DESC
      ```
2. **Total Time on Page**
    - For any pageview that is an exit, we compute the time on page as the difference between the timestamp of the last interaction 
    hit minus the timestamp of the pageview hit.
    - If a page is an exit page and it’s the only page of a session, and there are no interaction events, then this page is considered a bounce and does not contribute to total time on page. Put more simply,
    **bounces do not affect the avg time on page metric**.
    - milliseconds
3. **Entrances**: If this hit was the first pageview or screenview hit of a session, this is set to true.
4. **%Bounce Rate**: #bounce/total_session
5. **%Exist**: #exist/pageview 

    
