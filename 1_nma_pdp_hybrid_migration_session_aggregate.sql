
-- In this script, we used PID to check spillover, used nma_linker_id to join with segment table and get all event after activation


-- set parameters

use warehouse ECOM_LG_WH;
use database EDLDB_DEV;
use schema ecom_sandbox;

SET data_date = '{{ ds }}';
SET exp_name = '';


BEGIN;

-- get current date activations
CREATE OR REPLACE local temp TABLE ecom_sandbox.daily_activation AS
SELECT
    event_action                    AS experiment,
    event_label                     AS variation,
    event_category                  AS experiment_tool,
    session_date_est AS session_date,
    a.session_id,
    dataset AS device_category,
    CHANNEL_GROUPING_LAST_NON_DIRECT_LAST_CHANNEL AS channel,
    has_active_autoship AS active_autoship_flag,
    new_customer_flag,
    -- defalt exp_user_id as pid
    a.personalization_id AS EXP_USER_ID,
    a.personalization_id AS personalization_id,
    CUSTOMER_ID,
    nma_linker_id,
    event_id,
    event_timestamp,
    event_name,
    dw_site_id
FROM
    segment.segment_nma_hits_unified AS a
WHERE 
  session_date_est = $data_date
  AND dataset IN ('android', 'ios')
  AND event_action = $exp_name
  AND event_name IN ('Experiment Activated',
                     'Experiment Seen')
  AND EXP_USER_ID IS NOT NULL
;
    
    
  
CREATE OR REPLACE local temp TABLE ecom_sandbox.running_exp_activations AS
SELECT
    --*,
    experiment,
    variation,
    experiment_tool,
    session_date,
    session_id,
    device_category,
    channel,
    active_autoship_flag,
    new_customer_flag,
    EXP_USER_ID,
    personalization_id,
    customer_id,
    nma_linker_id,
    event_id,
    event_timestamp,
    event_name,
    Same_Session_Spillover_Flag,
    CID_Spillover_Flag,
    PID_Spillover_Flag,
    CASE 
        WHEN count_if(Spillover_Flag)OVER(partition BY experiment, exp_user_id) > 0 
            THEN True 
            ELSE False 
            END AS Spillover_Flag,
    event_rank,
    dw_site_id,
    case
        when dw_site_id = 10 then 'united states'
        when dw_site_id = 60 then 'canada'
        else 'other'
        end as dw_site_name
FROM 
    (
    SELECT 
        *,
        CASE
            WHEN COUNT(DISTINCT variation)over(partition BY experiment, session_id) > 1
                THEN True
            ELSE False
            END AS Same_Session_Spillover_Flag,
        CASE
            WHEN COUNT(DISTINCT variation)over(partition BY personalization_id, experiment) > 1
                THEN True
            ELSE False
            END AS PID_Spillover_Flag,
        CASE
            WHEN COUNT(DISTINCT variation)over(partition BY customer_id, experiment) > 1
                THEN True
            ELSE False
            END AS CID_Spillover_Flag,
        CASE
            WHEN Same_Session_Spillover_Flag
                OR  PID_Spillover_Flag
                OR  CID_Spillover_Flag
                THEN True
            ELSE False
            END AS Spillover_Flag,
        dense_rank()over(partition BY EXP_USER_ID, experiment ORDER BY event_timestamp,
            event_name) AS event_rank
    FROM
        (
        SELECT 
            experiment,
            variation,
            experiment_tool,
            session_date,
            session_id,
            device_category,
            channel,
            active_autoship_flag,
            new_customer_flag,
            EXP_USER_ID,
            personalization_id,
            customer_id,
            nma_linker_id,
            event_id,
            event_timestamp,
            event_name,
            dw_site_id
        FROM 
            ECOM_SANDBOX.PCA_CLICKSTREAM_PDP_HYBRID_MIGRATION_EXPERIMENT_ACTIVATION
        WHERE 
            session_date < $data_date and 
            experiment = $exp_name
        UNION ALL
        SELECT * FROM ecom_sandbox.daily_activation
        )
    )
;


CREATE OR REPLACE local temp TABLE ecom_sandbox.daily_event AS
-- get merch sales
WITH financial_metrics AS (
    SELECT
        order_id
        ,sum(order_line_total_price) as merch_sales
        ,sum(order_line_quantity) as units
    from
        ecom.order_line
    where
        order_placed_dttm_est::date between ($data_date-1) and ($data_date+1) -- just trying to account for any backend synchronization issues
        and business_channel_name ilike any ('%ios%', '%android%', '%web%')
    group by
        1
)
SELECT 
    experiment,
	variation,
    a.*
    --please add merch_sales,units, event_type to the daliy_event table
FROM (
	SELECT 
        session_date_est AS session_date,
		session_id,
		dataset AS device_category,
        CHANNEL_GROUPING_LAST_NON_DIRECT_LAST_CHANNEL AS channel,
        has_active_autoship AS active_autoship_flag,
        new_customer_flag,
        nma_linker_id,
		personalization_id AS PID,
		TRY_CAST(customer_id AS INT) AS customer_id,
		event_id,
		event_timestamp,
		event_name,
		event_category,
		event_action,
		event_label,
		row_type,
		product_id,
		product_merch_classification1,
		product_merch_classification2,
		order_id,
		revenue_amount,
		product_quantity,
		page_type,
		properties: products [0].sourceView AS source_view,
        dw_site_id,
        case
            when dw_site_id = 10 then 'united states'
            when dw_site_id = 60 then 'canada'
            else 'other'
            end as dw_site_name,
        order_type,
        product_price,
        screen_name,
        path_name,
        widget_description,
        is_entrance,
        is_exit,
        is_bounce
	FROM segment.segment_nma_hits_products_union_unified
	WHERE session_date = $data_date
	) AS a
INNER JOIN (
	SELECT 
        event_timestamp,
		experiment,
		variation,
		nma_linker_id
	FROM ecom_sandbox.running_exp_activations
	WHERE NOT Spillover_Flag AND event_rank = 1
	) AS b
	ON a.nma_linker_id = b.nma_linker_id AND timediff(milliseconds, a.event_timestamp, b.event_timestamp) < 1000
LEFT JOIN
    financial_metrics f
    on a.order_id = f.order_id
;

-- Aggregation

CREATE OR REPLACE local temp TABLE ecom_sandbox.nma_exp_before_final as
SELECT
    session_date,
    EXPERIMENT,
    Variation,
    device_category,
    channel,
    new_customer_flag,
    active_autoship_flag,
    PID,
    nma_linker_id, -- For unique visitors in experiments do we use the anon pid/ person_id / nma_linker_id?
    session_id,
    COUNT(DISTINCT 
           CASE
              WHEN event_action = 'purchase' AND order_id IS NOT NULL 
              AND row_type <> 'product'
              THEN order_id
           END) AS Orders,
    COUNT(DISTINCT 
           CASE
              WHEN (variation = false AND 
                    screen_name ilike any ('product%detail%', 'pdp', 'pdp-atc-upsell') and event_type = 'screen') 
              OR 
                   (variation = true AND 
                   ((event_category = 'eec' and event_action = 'detail' and event_label IN ('In Stock','Out of Stock')) or page_type = 'pdp'))
                 THEN event_id
               END) AS pdp_hits,

   COUNT(DISTINCT
          CASE
              WHEN (variation = false AND 
                    screen_name ilike any ('product%detail%', 'pdp', 'pdp-atc-upsell') and event_name = 'Product Added'
                    and (not coalesce(properties:event_label::string, '') ilike '%add%ToAutoship%') 
                    and (not coalesce(properties:event_action::string, '') ilike '%add%ToAutoship%')) 
              OR 
                    (variation = true AND 
                    page_type = 'pdp' and event_category = 'eec' and event_action = 'addToCart')
              THEN event_id
          END) AS atc_hits_pdp,         
   COUNT(DISTINCT
          CASE
              WHEN (variation = false AND 
                    screen_name ilike any ('product%detail%', 'pdp', 'pdp-atc-upsell') and event_name = 'Product Added'
                    and (not coalesce(properties:event_label::string, '') ilike '%add%ToAutoship%') 
                    and (not coalesce(properties:event_action::string, '') ilike '%add%ToAutoship%')
                    and widget_description is null) 
              OR 
                    (variation = true AND 
                    page_type = 'pdp' and event_category = 'eec' and event_action = 'addToCart')
              THEN event_id
          END) AS atc_hits_buybox,
   COUNT(DISTINCT
          CASE
              WHEN (variation = false AND 
                    screen_name ilike any ('pdp', 'product%detail%', 'pdp-atc-upsell') and is_bounce) 
              OR 
                    (variation = true AND 
                    (page_type = 'pdp' or path_name ilike '%/dp/%') and is_bounce)
              THEN event_id
          END) AS pdp_bounce_hits, 
   COUNT(DISTINCT
          CASE
              WHEN (variation = false AND 
                    screen_name ilike any ('pdp', 'product%detail%', 'pdp-atc-upsell') and is_exit) 
              OR 
                    (variation = true AND 
                    (page_type = 'pdp' or path_name ilike '%/dp/%') and is_exit)
              THEN event_id
          END) AS pdp_exit_hits, 
    SUM(
            CASE
                WHEN event_action = 'purchase' 
                    and row_type <> 'product' -- non-product info rows to prevent double counting when joining with order_line
                    AND order_id IS NOT NULL
                    THEN merch_sales
                ELSE 0
                END) AS total_merch_sales,
    SUM(
            CASE
                WHEN event_action = 'purchase' 
                    and row_type <> 'product' -- non-product info rows to prevent double counting when joining with order_line
                    AND order_id IS NOT NULL
                    THEN units
                ELSE 0
                END) AS total_units
FROM
    ecom_sandbox.daily_event
WHERE session_date = $data_date
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13;


-- calculate funnel
CREATE OR REPLACE local temp TABLE ecom_sandbox.nma_exp_funnel_steps AS
SELECT
    session_date,
    funnel_step,
    session_id,
    EXPERIMENT,
    Variation
FROM
    (select * from 
    ecom_sandbox.daily_event
    where (screen_name ilike any ('product%detail%', 'pdp', 'pdp-atc-upsell') and event_name = 'Product Added')
       or (page_type = 'pdp' and event_name = 'Product Added' and event_action ilike any ('addToCart','addToAutoship', 'addOnceToAutoship'))
       or (screen_name ilike any ('product%detail%', 'pdp', 'pdp-atc-upsell') and event_type = 'screen')
       or ((event_category = 'eec' and event_action = 'detail' and event_label IN ('In Stock','Out of Stock')) or page_type = 'pdp')
    )
    match_recognize (partition BY session_date, session_id, EXPERIMENT, Variation ORDER BY event_timestamp 
    MEASURES 
    classifier AS funnel_step 
    one row per match 
    PATTERN(PDP ATC) 
    DEFINE PDP AS 
        (variation = false AND 
        screen_name ilike any ('product%detail%', 'pdp', 'pdp-atc-upsell') and event_type = 'screen') 
        OR 
        (variation = true AND 
        ((event_category = 'eec' and event_action = 'detail' and event_label IN ('In Stock','Out of Stock')) or page_type = 'pdp'))
       , 
       ATC AS 
        (variation = false AND 
        screen_name ilike any ('product%detail%', 'pdp', 'pdp-atc-upsell') and event_name = 'Product Added') 
        OR 
        (variation = true AND 
        page_type = 'pdp' and event_name = 'Product Added' and event_action ilike any ('addToCart','addToAutoship', 'addOnceToAutoship')
WHERE
        funnel_step = 'ATC' and session_date = $data_date;


--  aggregate funnel success to the session level

CREATE OR REPLACE local temp TABLE nma_exp_funnel_agg AS
SELECT
    session_date,
    session_id,
    experiment,
    variation,
    COUNT(DISTINCT
          CASE
              WHEN funnel_step='ATC'
                  THEN session_id
              END ) pdp_success_session_flag
FROM
    ecom_sandbox.sfw_exp_funnel_steps
GROUP BY
    1,2,3,4;


-- combine sessions and funnel success
create or replace local temp table session_aggregate as
SELECT
    experiment,
    variation,
    session_date,
    session_id,
    PID,
    nma_linker_id,
    device_category,
    new_customer_flag,
    active_autoship_flag,
    channel,
    total_merch_sales,
    total_units,
    atc_hits_pdp,
    case when pdp_hits > 0 then true else false end pdp_session_flag,
    orders,
    case when pdp_success_session_flag > 0 then true else false end AS pdp_success_session_flag,
    case when atc_hits_pdp > 0 then true else false end atc_pdp_session_flag,
    case when atc_hits_buybox > 0 then true else false end atc_pdp_buybox_session_flag,   
    case when pdp_bounce_hits > 0 then true else false end pdp_bounce_session_flag,
    case when pdp_exit_hits > 0 then true else false end pdp_exit_session_flag
FROM
    (
        SELECT
            a.*,
            NVL(pdp_success_session_flag, 0) as pdp_success_session_flag
        FROM
            ecom_sandbox.nma_exp_before_final a
                LEFT JOIN
            ecom_sandbox.nma_exp_funnel_agg b
            ON
                        a.session_date=b.session_date
                    AND a.session_id=b.session_id
                    AND a.experiment = b.experiment
                    AND a.variation = b.variation
        ORDER BY
            a.session_id) AS final;        

DELETE
FROM
    ECOM_SANDBOX.PCA_CLICKSTREAM_PDP_HYBRID_MIGRATION_EXPERIMENT_ACTIVATION
WHERE
        session_date = $data_date;

DELETE
FROM
    ECOM_SANDBOX.PCA_CLICKSTREAM_PDP_HYBRID_MIGRATION_EXPERIMENT_EVENT
WHERE
        session_date = $data_date;


DELETE
FROM
    ECOM_SANDBOX.PCA_CLICKSTREAM_PDP_HYBRID_MIGRATION_EXPERIMENT_SESSION_AGGREGATE
WHERE
        session_date = $data_date;

      
        
insert into ECOM_SANDBOX.PCA_CLICKSTREAM_PDP_HYBRID_MIGRATION_EXPERIMENT_ACTIVATION
select * from ecom_sandbox.running_exp_activations;


insert into ECOM_SANDBOX.PCA_CLICKSTREAM_PDP_HYBRID_MIGRATION_EXPERIMENT_EVENT
select * from ecom_sandbox.daily_event;


INSERT INTO ECOM_SANDBOX.PCA_CLICKSTREAM_PDP_HYBRID_MIGRATION_EXPERIMENT_SESSION_AGGREGATE
select * from ecom_sandbox.session_aggregate;



COMMIT;
