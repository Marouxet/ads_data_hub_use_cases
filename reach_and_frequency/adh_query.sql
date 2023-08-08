
/*
Yahoo - Reach and Frequency Analysis

# NAME OF THE TABLE:
# NOTE: naming convention: <name-initials><use-case-name>_<query-version>_<startdate>_<enddate>_<runversion>

# ma_freqAnalysis_CM_Yahoo_FF_01

*/



WITH imp_u_clicks_u_acts AS (

  (SELECT
    event.advertiser_id as advertiser_id,
    event.campaign_id as campaign_id,
    event.site_id as site_id,
    event.event_time as impression_time,
    0 as event_time,
    'imp' AS interaction_type,
    user_id AS user_id,
    event.dv360_billable_cost_usd as cost_in_usd,
    0 as revenue,
  FROM
    `adh.cm_dt_impressions`
  WHERE
  user_id != '0' AND user_id IS NOT NULL 
  
  AND event.advertiser_id IN (8867627,5561340,6518610,5667309)  
 
    
 )
  
  UNION ALL


  (SELECT
    event.advertiser_id as advertiser_id,
    event.campaign_id as campaign_id,
    event.site_id as site_id,
    UNIX_MICROS(CURRENT_TIMESTAMP()) as impression_time,
    event.event_time  as event_time,
    'click' AS interaction_type,
    user_id AS user_id,
    event.dv360_billable_cost_usd as cost_in_usd,
    0 as revenue,
   FROM
    `adh.cm_dt_clicks`
   WHERE
    user_id != '0' AND user_id IS NOT NULL 
     AND event.advertiser_id IN (8867627,5561340,6518610,5667309)  
    

 )

  UNION ALL

  (SELECT
    event.advertiser_id as advertiser_id,
    event.campaign_id as campaign_id,
    event.site_id as site_id,
    UNIX_MICROS(CURRENT_TIMESTAMP()) as impression_time,
    event.event_time  as event_time,
    'conversion' AS interaction_type,
    user_id,
    0 AS cost_in_usd,
    event.total_conversions_revenue as revenue,
  FROM
    `adh.cm_dt_activities_attributed`
   WHERE
    user_id != '0' AND user_id IS NOT NULL
      AND event.advertiser_id IN (8867627,5561340,6518610,5667309)  
    AND event.activity_id IN ( 
                              14164242,14026244,13994434,  # Mail
                              10152471,10151064,10109576,10152556,10152426,10151073,10145521,10147455,10954814,10980963,10825886,10109555,10111991,10111964,10144672,10149976,10147416,12007839,12033912,10826090,1024953,10825892,10144696,10151025,12438674,12431999,12436304,12431203,13031985,10147461,12916298,12948669,13488893,12166853,11181236,13434542,12513672,13619765,13620836,13589743, # Suscriptions
                              4714716, 4713522, 4713921,4713919,4714117,4713920,4713718,4714116, # Fantasy Football
                              3498451#, 3498530 # Daily Fantasy - TBC
                              )  
  )

),

imp_u_clicks_u_acts_with_dates AS (

  SELECT
    *,
    MIN(impression_time) OVER(PARTITION BY advertiser_id,campaign_id,user_id) as min_impression_time
  FROM 
    imp_u_clicks_u_acts

),

user_level_data AS (
  SELECT
    advertiser_id,
    campaign_id,
    site_id,
    user_id,
    SUM(IF(interaction_type = 'imp', 1, 0)) AS impressions,
    SUM(IF(interaction_type = 'click', 1, 0)) AS clicks,
    SUM(IF(interaction_type = 'conversion', 1, 0)) AS conversions,
    SUM(IF(interaction_type = 'click' AND event_time >= min_impression_time, 1, 0)) AS clicks_filtered,
    SUM(IF(interaction_type = 'conversion' AND event_time >= min_impression_time, 1, 0)) AS conversions_filtered,

    SUM(cost_in_usd) as cost_in_usd,
    SUM(revenue) AS revenue,

      
  FROM
    imp_u_clicks_u_acts_with_dates
  GROUP BY
    1, 2, 3,4
)

# FINAL TABLE PROSP

SELECT
  advertiser_id,
  campaign_id,
  site_id,
  impressions AS frequency,

  # NOTE: If there are a small number of conversion and you want to avoid the ADH privacy check you can group the last frequencies
  # To run this, comment the line above impressions AS frequency

  # ----- uncomment this code -----
  #CASE 
  #  WHEN impressions >= 205 THEN 205
  #  ELSE impressions 
  #END AS frequency,  
  # ----- uncomment this code -----

  COUNT(*) AS reach,  # it is the same that count(user_id) The result is #user*frequency -> COUNT(user_id) AS reach_user,
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SUM(conversions) as total_conversions,
  SUM(clicks_filtered) AS total_clicks_filtered,
  SUM(conversions_filtered) as total_conversions_filtered,
  SUM(revenue) as total_revenue,
  SUM(cost_in_usd) as total_cost,


FROM
  user_level_data
GROUP BY
  1, 2, 3,4
ORDER BY
  1,2,3 ASC

