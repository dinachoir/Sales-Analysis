--1--
CREATE TABLE olist_customers (
	customer_id varchar(50) PRIMARY KEY,
	customer_unique_id varchar(50) NOT NULL,
	customer_zip_code varchar(5) NOT NULL,
	customer_city varchar(50) NOT NULL,
	customer_state varchar(2) NOT NULL
);
COPY olist_customers
FROM 'D:\Projects\Olist\olist_customers_dataset.csv'
WITH (FORMAT CSV, HEADER);

--2--
CREATE TABLE olist_order_items (
	order_id varchar(50) NOT NULL,
	order_item_id varchar(2) NOT NULL,
	product_id varchar(50) NOT NULL,
	seller_id varchar(50) NOT NULL,
	shipping_limit_date timestamp with time zone NOT NULL,
	price numeric(8,2) NOT NULL,
	freight_value numeric(8,2) NOT NULL
);
COPY olist_order_items
FROM 'D:\Projects\Olist\olist_order_items_dataset.csv'
WITH (FORMAT CSV, HEADER);

--3--
CREATE TABLE olist_order_payments (
	order_id varchar(50) NOT NULL,
	payment_sequential int NOT NULL,
	payment_type varchar(20) NOT NULL,
	payment_installment int NOT NULL,
	payment_value numeric(8,2) NOT NULL
);
COPY olist_order_payments
FROM 'D:\Projects\Olist\olist_order_payments_dataset.csv'
WITH (FORMAT CSV, HEADER);

--4--
CREATE TABLE olist_orders (
	order_id varchar(50),
	customer_id varchar(50),
	order_status varchar(15) NOT NULL,
	order_purchase_time timestamp with time zone NOT NULL,
	order_approved_time timestamp with time zone,
	order_delivered_carrier_date timestamp with time zone,
	order_delivered_customer_date timestamp with time zone,
	order_estimated_delivery_date timestamp with time zone NOT NULL,
	CONSTRAINT order_key PRIMARY KEY (order_id, customer_id)
);
COPY olist_orders
FROM 'D:\Projects\Olist\olist_orders_dataset.csv'
WITH (FORMAT CSV, HEADER);

--5--
CREATE TABLE olist_products (
	product_id varchar(50) PRIMARY KEY,
	product_category_name varchar(50),
	product_name_length int,
	product_description_length int,
	product_photo_qty int,
	product_weight_g int,
	product_length_cm int,
	product_height_cm int,
	product_width_cm int
);
COPY olist_products
FROM 'D:\Projects\Olist\olist_products_dataset.csv'
WITH (FORMAT CSV, HEADER);

--6--
CREATE TABLE olist_sellers (
	seller_id varchar(50) PRIMARY KEY,
	seller_zip_code varchar(5) NOT NULL,
	seller_city varchar(50) NOT NULL,
	seller_state varchar(2) NOT NULL
);
COPY olist_sellers
FROM 'D:\Projects\Olist\olist_sellers_dataset.csv'
WITH (FORMAT CSV, HEADER);

--7--
CREATE TABLE olist_geolocation (
	geolocation_zip_code varchar(5) NOT NULL,
	geolocation_lat numeric(18,15) NOT NULL,
	geolocation_lng numeric(18,15) NOT NULL,
	geolocation_city varchar(50) NOT NULL,
	geolocation_state varchar(2) NOT NULL
);
COPY olist_geolocation
FROM 'D:\Projects\Olist\olist_geolocation_dataset.csv'
WITH (FORMAT CSV, HEADER);

---------------------------------------------------------
--Repairing ZIP codes
SELECT length(customer_zip_code), count(*) AS length_count
FROM olist_customers
GROUP BY length(customer_zip_code)
ORDER BY length(customer_zip_code);

SELECT customer_state, count(*) AS customer_state_count
FROM olist_customers
WHERE length(customer_zip_code) < 5
GROUP BY customer_state
ORDER BY customer_state;

UPDATE olist_customers
SET customer_zip_code = '0' || customer_zip_code
WHERE length(customer_zip_code)= 4;


SELECT length(seller_zip_code), count(*) AS length_count
FROM olist_sellers
GROUP BY length(seller_zip_code)
ORDER BY length(seller_zip_code);

SELECT seller_state, count(*) AS seller_state_count
FROM olist_sellers
WHERE length(seller_zip_code) < 5
GROUP BY seller_state
ORDER BY seller_state;

UPDATE olist_sellers
SET seller_zip_code = '0' || seller_zip_code
WHERE length(seller_zip_code)= 4;


SELECT length(geolocation_zip_code), count(*) AS length_count
FROM olist_geolocation
GROUP BY length(geolocation_zip_code)
ORDER BY length(geolocation_zip_code);

SELECT geolocation_state, count(*) AS geolocation_state_count
FROM olist_geolocation
WHERE length(geolocation_zip_code) < 5
GROUP BY geolocation_state
ORDER BY geolocation_state;

UPDATE olist_geolocation
SET geolocation_zip_code = '0' || geolocation_zip_code
WHERE length(geolocation_zip_code)= 4;

--Counting customers by state and city
SELECT 	customer_state, 
		customer_city,
		count(customer_id) customer_count
FROM olist_customers
GROUP BY customer_state, customer_city
ORDER BY customer_state, customer_city;

SELECT 	customer_state, 
		count(customer_id) customer_count
FROM olist_customers
GROUP BY customer_state
ORDER BY customer_count DESC; --(The most customers are in SP state)

SELECT 	customer_city,
		count(customer_id) customer_count
FROM olist_customers
WHERE customer_state = 'SP'
GROUP BY customer_city
ORDER BY customer_count DESC; --(The most customers are in SP state, Sao Paulo City)
				
--Counting sellers by state and city
SELECT 	seller_state, 
		seller_city,
		count(seller_id) seller_count
FROM olist_sellers
GROUP BY seller_state, seller_city
ORDER BY seller_state, seller_city;

SELECT 	seller_state, 
		count(seller_id) seller_count
FROM olist_sellers
GROUP BY seller_state
ORDER BY seller_count DESC; --(The most sellers are in SP state)

SELECT 	seller_city,
		count(seller_id) seller_count
FROM olist_sellers
WHERE seller_state = 'SP'
GROUP BY seller_city
ORDER BY seller_count DESC; --(The most sellers are in SP state, Sao Paulo City)

--Counting highest sales product category
SELECT op.product_category_name, count(ooi.product_id)
FROM olist_order_items ooi JOIN olist_products op
	ON ooi.product_id = op.product_id
GROUP BY op.product_category_name
ORDER BY count(ooi.product_id) DESC;

--Counting product category sold by each seller
SELECT ooi.seller_id, op.product_category_name, count(ooi.product_id)
FROM olist_order_items ooi JOIN olist_products op
	ON ooi.product_id = op.product_id
GROUP BY ooi.seller_id, op.product_category_name
ORDER BY ooi.seller_id, count(ooi.product_id) DESC;

--Determine minimum, maximum, average and median price of each product 
SELECT	product_id, max(price), min(price),
		round(avg(price),2) AS average_price,
		percentile_cont(.5) WITHIN GROUP (ORDER BY price) AS median
FROM olist_order_items
GROUP BY product_id
HAVING max(price) <> min(price)
ORDER BY product_id

--Determine minimum, maximum, average and median price of each category 
SELECT 	op.product_category_name, max(ooi.price), min(ooi.price), 
		round(avg(ooi.price),2) AS average_price,
		percentile_cont(.5) WITHIN GROUP (ORDER BY ooi.price) AS median
FROM olist_order_items ooi JOIN olist_products op
	ON ooi.product_id = op.product_id
GROUP BY op.product_category_name
HAVING max(ooi.price) <> min(ooi.price)
ORDER BY op.product_category_name;

--Who is the top 10th best-selling seller?
SELECT 	ooi.seller_id, os.seller_state, count(ooi.product_id),
		rank () OVER (ORDER BY count(ooi.product_id) DESC)
FROM olist_order_items ooi JOIN olist_products op
	ON ooi.product_id = op.product_id
		JOIN olist_sellers os
			ON ooi.seller_id = os.seller_id
GROUP BY ooi.seller_id, os.seller_state
LIMIT 10;

---Calculate what decile each seller would be in compared to other sellers 
---based on total sales amount in 2016-2017 period
WITH total_seller_sales_2016_2017 AS (
		SELECT seller_id, sum(price) AS total_sales_amount
		FROM olist_order_items ooi JOIN olist_orders oo
			ON ooi.order_id = oo.order_id
		WHERE oo.order_purchase_time >='2016-01-01'
			AND oo.order_purchase_time <'2018-01-01'
			AND oo.order_status != 'canceled'
		GROUP BY 1
	)
SELECT *,
NTILE (100) OVER (ORDER BY total_sales_amount)
FROM total_seller_sales_2016_2017;

---Calculate what decile each seller would be in compared to other sellers 
---based on total sales amount in 2017-2018 period
WITH total_seller_sales_2017_2018 AS (
		SELECT seller_id, sum(price) AS total_sales_amount
		FROM olist_order_items ooi JOIN olist_orders oo
			ON ooi.order_id = oo.order_id
		WHERE oo.order_purchase_time >='2017-01-01'
			AND oo.order_purchase_time <'2019-01-01'
			AND oo.order_status != 'canceled'
		GROUP BY 1
	)
SELECT *,
NTILE (100) OVER (ORDER BY total_sales_amount)
FROM total_seller_sales_2017_2018;

--What types of payments do customers often use?
SELECT payment_type, count(*)
FROM olist_order_payments
WHERE payment_type != 'not_defined' --not defined payment type is canceled order
GROUP BY payment_type
ORDER BY count(*) DESC

--The most frequent hour customers do purchase order
SELECT	payments.payment_type AS payment_type,
		orders.order_purchase_time AS purchase_time
	INTO payment_purchase_time
FROM olist_orders orders JOIN olist_order_payments payments
	ON orders.order_id = payments.order_id
WHERE payment_type != 'not_defined';
	
SELECT *
FROM crosstab ('SELECT 	payment_type,
			   			date_part(''hour'', purchase_time),
			   			count(*)
			    FROM payment_purchase_time
			    GROUP BY payment_type,
			   			date_part(''hour'', purchase_time)
			    ORDER BY payment_type',
			   
			   'SELECT	hour
			    FROM generate_series(1,24) hour'
			  )
AS	(payment_type varchar(20),
	 at_1 numeric(10,0),
	 at_2 numeric(10,0),
	 at_3 numeric(10,0),
	 at_4 numeric(10,0),
	 at_5 numeric(10,0),
	 at_6 numeric(10,0),
	 at_7 numeric(10,0),
	 at_8 numeric(10,0),
	 at_9 numeric(10,0),
	 at_10 numeric(10,0),
	 at_11 numeric(10,0),
	 at_12 numeric(10,0),
	 at_13 numeric(10,0),
	 at_14 numeric(10,0),
	 at_15 numeric(10,0),
	 at_16 numeric(10,0),
	 at_17 numeric(10,0),
	 at_18 numeric(10,0),
	 at_19 numeric(10,0),
	 at_20 numeric(10,0),
	 at_21 numeric(10,0),
	 at_22 numeric(10,0),
	 at_23 numeric(10,0),
	 at_24 numeric(10,0)
);
	 
--What percentage of customer delivery order punctuality?
SELECT COUNT(order_delivered_customer_date) FROM olist_orders
WHERE order_delivered_customer_date IS NOT NULL; --(96476)

SELECT COUNT(order_delivered_customer_date) FROM olist_orders
WHERE order_delivered_customer_date IS NOT NULL
	AND order_delivered_customer_date <= order_estimated_delivery_date; --(88649)

SELECT 88649.0/96476.0 AS delivery_puctuality_rate; --(91.89%)

ALTER TABLE olist_orders ADD COlUMN delivery_punctuality varchar(20);
SELECT * FROM olist_orders;

UPDATE olist_orders
SET delivery_punctuality = (
	CASE 	WHEN order_delivered_customer_date < order_estimated_delivery_date THEN 'In Time'
			WHEN order_delivered_customer_date = order_estimated_delivery_date THEN 'On Time'
			WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 'Late'
			ELSE NULL
			END);

--Geospatial Analysis
---Represent the geographic points for every customer and seller
CREATE TEMP TABLE customer_points AS (
	SELECT oc.customer_id AS customer_id, point(og.geolocation_lng, og.geolocation_lat) AS lng_lat_point
	FROM olist_customers oc JOIN olist_geolocation og
		ON oc.customer_zip_code = og.geolocation_zip_code
		AND oc.customer_city = og.geolocation_city
		AND oc.customer_state = og.geolocation_state
);

CREATE TEMP TABLE seller_points AS (
	SELECT os.seller_id AS seller_id, point(og.geolocation_lng, og.geolocation_lat) AS lng_lat_point
	FROM olist_sellers os JOIN olist_geolocation og
		ON os.seller_zip_code = og.geolocation_zip_code
		AND os.seller_city = og.geolocation_city
		AND os.seller_state = og.geolocation_state
);
SELECT * FROM seller_points;

---Calculate the distance for each customer and every possible seller
CREATE TEMP TABLE customer_seller_distance AS (
	SELECT customer_id, seller_id, 
			cp.lng_lat_point <> sp.lng_lat_point AS distance
	FROM customer_points cp CROSS JOIN seller_points sp
);

---Identify the closest seller for each customer
CREATE TEMP TABLE closest_sellers AS (
	SELECT DISTINCT ON (customer_id)
		customer_id,
		seller_id,
		distance
	FROM customer_seller_distance
	ORDER BY customer_seller_distance
);

---Find the average and median distances to a seller for our customers
SELECT	AVG(distance) AS avg_distance,
		PERCENTILE_DISC(.5) WITHIN GROUP (ORDER BY distance) AS median_distance
FROM closest_sellers;


--Analyzing sales
---Total sales per day and cumulative sum of sales
SELECT 	oo.order_purchase_time::DATE AS transaction_date, 
		count(*) AS total_sales
	INTO sales_daily
FROM olist_order_items ooi JOIN olist_orders oo
	ON ooi.order_id = oo.order_id
WHERE oo.order_purchase_time >='2016-01-01'
	AND oo.order_purchase_time <'2019-01-01'
	AND oo.order_status != 'canceled'
GROUP BY 1
ORDER BY 1;

SELECT *, sum(total_sales) OVER (ORDER BY transaction_date)
	INTO daily_sales_growth
FROM sales_daily;

---Compute a 7-day lag daily sales growth
SELECT *, lag(sum, 7) OVER (ORDER BY transaction_date) 
	INTO sales_daily_lag 
FROM daily_sales_growth;

SELECT * FROM sales_daily_lag;

---Determine sales growth compared to the previous week
SELECT *, (sum-lag)/lag AS volume 
	INTO sales_lag_volume
FROM sales_daily_lag;

SELECT * FROM sales_lag_volume;


---The rolling 30-day average for the daily number of sales
WITH daily_sales AS (
	SELECT oo.order_purchase_time::DATE AS transaction_date, count(*) AS total_orders
	FROM olist_order_items ooi JOIN olist_orders oo
		ON ooi.order_id = oo.order_id
	WHERE oo.order_status != 'canceled'
	GROUP BY 1
	),

	 moving_average_calculation_30 AS (
	SELECT transaction_date, total_orders,
			AVG(total_orders) OVER (ORDER BY transaction_date ROWS BETWEEN 30
				PRECEDING and CURRENT ROW) AS sales_moving_average,
			ROW_NUMBER() OVER (ORDER BY transaction_date) as row_number
	FROM daily_sales
	ORDER BY 1)

SELECT 	transaction_date,
		CASE WHEN row_number>=30 THEN sales_moving_average ELSE NULL END
   			AS sales_moving_average_30
FROM moving_average_calculation_30
WHERE transaction_date>='2016-09-04'
AND transaction_date<'2019-01-01';


--Analyzing sales growth by voucher-payment using rate
SELECT 	oo.customer_id AS customer_id,
		oo.order_id AS order_id,
		oo.order_purchase_time AS transaction_date,
		oop.payment_type AS payment_type
	INTO voucher_payment_sales
FROM olist_orders oo JOIN olist_order_payments oop
	ON oo.order_id = oop.order_id
	AND oo.order_status != 'canceled'
WHERE payment_type = 'voucher'
ORDER BY transaction_date;

SELECT * FROM voucher_payment_sales;

---Count unique orders paid by voucher
SELECT COUNT(DISTINCT (order_id)) FROM voucher_payment_sales; --(3772 orders)

---Count unique customers who made a purchase
SELECT COUNT(DISTINCT (customer_id)) FROM voucher_payment_sales;

---Calculate the percentage of voucher-payment order
SELECT 	oo.customer_id AS customer_id,
		oo.order_id AS order_id,
		oo.order_purchase_time AS transaction_date,
		oop.payment_type AS payment_type
	INTO all_payment_sales
FROM olist_orders oo JOIN olist_order_payments oop
	ON oo.order_id = oop.order_id
	AND oo.order_status != 'canceled'
ORDER BY transaction_date;

SELECT COUNT(DISTINCT (order_id)) FROM all_payment_sales; --(98815 orders)

SELECT 3772.0/98815.0 AS voucher_payment_rate; --(3.82% of orders have used voucher as payment type)


--Analyzing the performance of the voucher marketing campaign



