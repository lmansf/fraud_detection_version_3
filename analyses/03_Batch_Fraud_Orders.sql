-- Add variable for batch size
DECLARE OR REPLACE VARIABLE var_batch_size INT;

-- Set batch size (number of rows to insert per run)
SET VARIABLE var_batch_size = 100; -- Change this value as needed

-- Create a sequence of numbers for batch generation
DROP TEMPORARY TABLE IF EXISTS batch_seq;

CREATE TEMPORARY TABLE batch_seq AS
SELECT posexplode(sequence(1, var_batch_size)) AS (batch_idx, seq_num);

-- Insert batch of orders
INSERT INTO `realtime-fraud-detection`.01_bronze.raw_orders
WITH 
batch_orders AS (
    SELECT 
        seq_num AS batch_row_num
    FROM batch_seq
),
random_restaurants AS (
    SELECT 
        batch_row_num,
        restaurant_id
    FROM (
        SELECT 
            bo.batch_row_num,
            r.restaurant_id,
            row_number() OVER (PARTITION BY bo.batch_row_num ORDER BY rand()) AS rn
        FROM batch_orders bo
        CROSS JOIN `realtime-fraud-detection`.00_reference.restaurants r
    )
    WHERE rn = 1
),
random_customers AS (
    SELECT 
        batch_row_num,
        customer_id
    FROM (
        SELECT 
            bo.batch_row_num,
            c.customer_id,
            row_number() OVER (PARTITION BY bo.batch_row_num ORDER BY rand()) AS rn
        FROM batch_orders bo
        CROSS JOIN `realtime-fraud-detection`.00_reference.customers_2 c
    )
    WHERE rn = 1
),
random_order_types AS (
    SELECT 
        batch_row_num,
        order_type
    FROM (
        SELECT 
            bo.batch_row_num,
            ot.order_type,
            row_number() OVER (PARTITION BY bo.batch_row_num ORDER BY rand()) AS rn
        FROM batch_orders bo
        CROSS JOIN (SELECT 'delivery' AS order_type) ot
    )
    WHERE rn = 1
),
random_payment_methods AS (
    SELECT 
        batch_row_num,
        payment_method
    FROM (
        SELECT 
            bo.batch_row_num,
            pm.payment_method,
            row_number() OVER (PARTITION BY bo.batch_row_num ORDER BY rand()) AS rn
        FROM batch_orders bo
        CROSS JOIN (SELECT 'card' AS payment_method) pm
    )
    WHERE rn = 1
),
random_order_statuses AS (
    SELECT 
        batch_row_num,
        order_status
    FROM (
        SELECT 
            bo.batch_row_num,
            os.order_status,
            row_number() OVER (PARTITION BY bo.batch_row_num ORDER BY rand()) AS rn
        FROM batch_orders bo
        CROSS JOIN (SELECT 'delivered' AS order_status UNION ALL SELECT 'completed') os
    )
    WHERE rn = 1
),
random_titles AS (
    SELECT 
        batch_row_num,
        title
    FROM (
        SELECT 
            bo.batch_row_num,
            t.title,
            row_number() OVER (PARTITION BY bo.batch_row_num ORDER BY rand()) AS rn
        FROM batch_orders bo
        CROSS JOIN (SELECT 'Mr' AS title UNION ALL SELECT 'Mrs' UNION ALL SELECT 'Miss' UNION ALL SELECT 'Ms' UNION ALL SELECT 'Dr') t
    )
    WHERE rn = 1
),
random_suffixes AS (
    SELECT 
        batch_row_num,
        suffix
    FROM (
        SELECT 
            bo.batch_row_num,
            s.suffix,
            row_number() OVER (PARTITION BY bo.batch_row_num ORDER BY rand()) AS rn
        FROM batch_orders bo
        CROSS JOIN (SELECT 'Sr' AS suffix UNION ALL SELECT 'Jr' UNION ALL SELECT 'III' UNION ALL SELECT 'Esq' UNION ALL SELECT 'MD' UNION ALL SELECT 'PHD' UNION ALL SELECT 'DDS') s
    )
    WHERE rn = 1
),
random_num_items AS (
    SELECT
        r.batch_row_num,
        r.restaurant_id,
        FLOOR(1 + rand() * mi_count) AS num_items
    FROM random_restaurants r
    JOIN (
        SELECT restaurant_id, COUNT(*) as mi_count
        FROM `realtime-fraud-detection`.00_reference.menu_items
        GROUP BY restaurant_id
    ) mi ON mi.restaurant_id = r.restaurant_id
),
random_order_ids AS (
    SELECT
        batch_row_num,
        CONCAT(
            'ORD-', 
            DATE_FORMAT(getdate(), 'yyMMdd'), 
            '-', 
            FLOOR(RAND() * 900000) + 100000 + batch_row_num
        ) AS order_id
    FROM batch_orders
),
batch_context AS (
    SELECT
        o.batch_row_num,
        o.order_id,
        r.restaurant_id,
        c.customer_id,
        n.num_items,
        rot.order_type,
        rpm.payment_method,
        ros.order_status,
        rt.title,
        rs.suffix
    FROM random_order_ids o
    JOIN random_restaurants r ON o.batch_row_num = r.batch_row_num
    JOIN random_customers c ON o.batch_row_num = c.batch_row_num
    JOIN random_num_items n ON o.batch_row_num = n.batch_row_num
    JOIN random_order_types rot ON o.batch_row_num = rot.batch_row_num
    JOIN random_payment_methods rpm ON o.batch_row_num = rpm.batch_row_num
    JOIN random_order_statuses ros ON o.batch_row_num = ros.batch_row_num
    JOIN random_titles rt ON o.batch_row_num = rt.batch_row_num
    JOIN random_suffixes rs ON o.batch_row_num = rs.batch_row_num
),
cte_menu_items AS (
    SELECT 
        bc.batch_row_num,
        mi.*
    FROM batch_context bc
    JOIN `realtime-fraud-detection`.00_reference.menu_items mi ON mi.restaurant_id = bc.restaurant_id
),
cte_selected_items AS (
    SELECT 
        batch_row_num,
        item_id,
        restaurant_id,
        name,
        category,
        price,
        ingredients,
        is_vegetarian,
        spice_level,
        row_number() OVER (PARTITION BY batch_row_num ORDER BY rand()) AS rn
    FROM cte_menu_items
),
cte_final_items AS (
    SELECT csi.*
    FROM cte_selected_items csi
    JOIN batch_context bc ON csi.batch_row_num = bc.batch_row_num
    WHERE csi.rn <= bc.num_items
),
aggregated_items AS (
    SELECT
        batch_row_num,
        collect_list(named_struct('restaurant_id',restaurant_id, 'name', name,'price', price,'is_vegetarian', is_vegetarian)) AS items,
        sum(price) AS total_price
    FROM cte_final_items
    GROUP BY batch_row_num
),
customer_details AS (
    SELECT
        bc.batch_row_num,
        ct.street,
        ct.city,
        ct.country,
        ct.state,
        ct.zip_code,
        ct.phone,
        ct.mobile,
        ct.email_address,
        ct.card_number,
        ct.credit_card_exp_year,
        ct.credit_card_exp_month,
        ct.cvv
    FROM batch_context bc
    JOIN `realtime-fraud-detection`.00_reference.customers_2 ct ON ct.customer_id = bc.customer_id
)
SELECT 
    bc.order_id as order_id,
    timestamp(getdate()) as timestamp,
    bc.restaurant_id as restaurant_id,
    bc.customer_id as customer_id,
    bc.order_type as random_order_type,
    ai.items as items,
    round(ai.total_price, 2) * round(random()/50*10+1,0) as total_amount,
    bc.payment_method as payment_method,
    bc.order_status as order_status,
    timestamp(getdate()) as created_at,
    bc.title as title,
    bc.suffix as suffix,
    cd.street as street,
    cd.city as city,
    cd.country as country,
    cd.state as state,
    cd.zip_code as zip_code,
    cd.phone as phone_number,
    cd.mobile as mobile,
    cd.email_address as email_address,
    cd.card_number as card_number,
    cd.credit_card_exp_year as credit_card_exp_year,
    cd.credit_card_exp_month as credit_card_exp_month,
    round(random()*100,0) as cvv,
    round(1-(random()+0.49),0) as card_match,
    CAST(1 AS INT) as is_fraud
FROM batch_context bc
JOIN aggregated_items ai ON bc.batch_row_num = ai.batch_row_num
JOIN customer_details cd ON bc.batch_row_num = cd.batch_row_num;