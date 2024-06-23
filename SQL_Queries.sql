/*-----------------Q1-------------------*/
SELECT
    s.supplier_id,
    s.supplier_name,
    YEAR(d.order_date) AS sales_year,
    QUARTER(d.order_date) AS sales_quarter,
    MONTH(d.order_date) AS sales_month,
    SUM(f.sale) AS total_sales
FROM
    sales_fact f
JOIN
    date_dimension d ON f.date_id = d.date_id
JOIN
    supplier_dimension s ON f.supplier_id = s.supplier_id
GROUP BY
    s.supplier_id, s.supplier_name, sales_year, sales_quarter, sales_month
WITH ROLLUP;

/*==========================Q2====================================*/
SELECT
    p.product_id,
    p.product_name,
    EXTRACT(MONTH FROM d.order_date) AS sales_month,
    SUM(f.sale) AS total_sales
FROM
    sales_fact f
JOIN
    date_dimension d ON f.date_id = d.date_id
JOIN
    product_dimension p ON f.product_id = p.product_id
JOIN
    supplier_dimension s ON f.supplier_id = s.supplier_id
WHERE
    s.supplier_name = 'DJI' AND EXTRACT(YEAR FROM d.order_date) = 2019
GROUP BY
    ROLLUP (p.product_id, p.product_name, sales_month)
HAVING
    p.product_id IS NOT NULL
ORDER BY
    p.product_id, sales_month;
    

    /*=================================Q4======================================*/

SELECT
    f.product_id,
    p.product_name,
    SUM(CASE WHEN EXTRACT(QUARTER FROM d.order_date) = 1 THEN f.sale ELSE 0 END) AS Q1_sales,
    SUM(CASE WHEN EXTRACT(QUARTER FROM d.order_date) = 2 THEN f.sale ELSE 0 END) AS Q2_sales,
    SUM(CASE WHEN EXTRACT(QUARTER FROM d.order_date) = 3 THEN f.sale ELSE 0 END) AS Q3_sales,
    SUM(CASE WHEN EXTRACT(QUARTER FROM d.order_date) = 4 THEN f.sale ELSE 0 END) AS Q4_sales,
    SUM(f.sale) AS total_yearly_sales
FROM
    sales_fact f
JOIN
    date_dimension d ON f.date_id = d.date_id
JOIN
    product_dimension p ON f.product_id = p.product_id
WHERE
    EXTRACT(YEAR FROM d.order_date) = 2019
GROUP BY
    f.product_id, p.product_name
ORDER BY
    f.product_id;

    /*=================================Q5======================================*/
SELECT
    f.product_id,
    p.product_name,
    d.order_date,
    f.sale AS daily_sale,
    AVG(f.sale) OVER (PARTITION BY f.product_id) AS avg_daily_sale
FROM
    sales_fact f
JOIN
    date_dimension d ON f.date_id = d.date_id
JOIN
    product_dimension p ON f.product_id = p.product_id
WHERE
    f.sale > (1.5 * AVG(f.sale) OVER (PARTITION BY f.product_id)) -- detecting oulierers for sales
ORDER BY
    f.product_id, d.order_date;
    
    /*=================================Q7======================================*/
SELECT
    s.store_id,
    p.product_id,
    EXTRACT(MONTH FROM d.order_date) AS sales_month,
    SUM(f.sale) AS store_product_monthly_sales
FROM
    sales_fact f
JOIN
    date_dimension d ON f.date_id = d.date_id
JOIN
    store_dimension s ON f.store_id = s.store_id
JOIN
    product_dimension p ON f.product_id = p.product_id
WHERE
    s.store_name = 'Tech Haven' -- Specify the store name
    AND p.product_id = 101 -- Specify the product ID
GROUP BY
    s.store_id, p.product_id, sales_month
ORDER BY
    sales_month;

    /*=================================Q10======================================*/

SELECT
    c.customer_id,
    c.customer_name,
    COUNT(DISTINCT f.product_id) AS unique_products_purchased,
    SUM(f.sale) AS total_sales_2019
FROM
    sales_fact f
JOIN
    customer_dimension c ON f.customer_id = c.customer_id
JOIN
    date_dimension d ON f.date_id = d.date_id
WHERE
    EXTRACT(YEAR FROM d.order_date) = 2019
GROUP BY
    c.customer_id, c.customer_name
ORDER BY
    total_sales_2019 DESC, unique_products_purchased DESC
LIMIT 5;

    /*=================================Q9======================================*/
CREATE VIEW SUPPLIER_PERFORMANCE_VW AS
SELECT
    s.supplier_id,
    s.supplier_name,
    EXTRACT(YEAR FROM d.order_date) AS sales_year,
    EXTRACT(MONTH FROM d.order_date) AS sales_month,
    SUM(f.sale) AS monthly_sales
FROM
    sales_fact f
JOIN
    date_dimension d ON f.date_id = d.date_id
JOIN
    supplier_dimension s ON f.supplier_id = s.supplier_id
GROUP BY
    s.supplier_id, s.supplier_name, sales_year, sales_month;
    
    /*=================================Q3======================================*/
SELECT
    f.product_id,customer_dimensiondate_dimensiondate_dimensionmaster_data
    p.product_name,
    COUNT(*) AS weekend_sales_count
FROM
    sales_fact f
JOIN
    date_dimension d ON f.date_id = d.date_id
JOIN
    product_dimension p ON f.product_id = p.product_id
WHERE
    DAYOFWEEK(d.order_date) IN (1, 7) -- 1 is Sun and 7 is Sat
GROUP BY
    f.product_id, p.product_name
ORDER BY
    weekend_sales_count DESC
LIMIT 5;


    /*=================================Q6======================================*/
CREATE VIEW STOREANALYSIS_VW AS
SELECT
    s.store_id,
    p.product_id,
    SUM(f.sale) AS store_total
FROM
    sales_fact f
JOIN
    store_dimension s ON f.store_id = s.store_id
JOIN
    product_dimension p ON f.product_id = p.product_id
GROUP BY
    s.store_id, p.product_id;


