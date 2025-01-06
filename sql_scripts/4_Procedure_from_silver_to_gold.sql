create or replace procedure gold_layer.from_silver_to_gold()
language sql
as $$
		--sales overview	
	merge into gold_layer.sales_overview as target
	using (
	    select 
		    concat(extract(year from order_date), '-', lpad(extract(month from order_date)::text, 2, '0')) as time_period,
		    sum(quantity * list_price * (1 - discount)) as total_revenue, 
		    count(distinct order_id_sk) as total_orders,
		    sum(quantity * list_price * (1 - discount)) / count(distinct order_id_sk) as avg_order_value,
		    count(distinct customer_id_sk) as unique_customers,
		    sum(quantity * list_price * (discount)) as total_discounts,
		    sum(quantity * list_price) as gross_revenue,
		    avg(shipped_date::date - order_date::date) as avg_delivery_time
		from silver_layer.fact_orders
		group by time_period
	) as source
	on target.time_period = source.time_period
	when matched then
	    update set
	        total_revenue = source.total_revenue,
	        total_orders = source.total_orders,
	        avg_order_value = source.avg_order_value,
	        unique_customers = source.unique_customers,
	        total_discounts = source.total_discounts,
	        gross_revenue = source.gross_revenue,
	        avg_delivery_time = source.avg_delivery_time
	when not matched then
	    insert (time_period, total_revenue, total_orders, avg_order_value, unique_customers, total_discounts, gross_revenue, avg_delivery_time)
	    values (source.time_period, source.total_revenue, source.total_orders, source.avg_order_value, source.unique_customers, source.total_discounts, source.gross_revenue, source.avg_delivery_time);
	
	   
	
		
		-- customer segmentation
	merge into gold_layer.customer_segmentation as target
	using (
		select
			dc.customer_id_sk,
			customer_full_name,
			sum(quantity * list_price * (1 - discount)) as total_revenue,
			count(distinct order_id_sk) as total_orders,
			max(order_date) as last_order_date,
			sum(fo.quantity * fo.list_price * (1 - fo.discount)) / count(distinct fo.order_id_sk) as avg_revenue_per_order,
			sum(fo.quantity) / count(distinct fo.order_id_sk) as avg_order_size,
			(
		        select ds.store_name
		        from silver_layer.fact_orders fo
		        join silver_layer.dimension_stores ds on fo.store_id_sk = ds.store_id_sk
		        where fo.customer_id_sk = dc.customer_id_sk
		        group by ds.store_name
		        order by sum(fo.quantity) desc
		        limit 1
		    ) as favorite_store,
		    (
		        select dp.product_name
		        from silver_layer.fact_orders fo
		        join silver_layer.dimension_products dp on fo.product_id_sk = dp.product_id_sk
		        where fo.customer_id_sk = dc.customer_id_sk
		        group by dp.product_name
		        order by sum(fo.quantity) desc
		        limit 1
		    ) as favorite_product
		from silver_layer.fact_orders fo 
		join silver_layer.dimension_customers dc on fo.customer_id_sk = dc.customer_id_sk 
		group by dc.customer_id_sk
		) as source
	on target.customer_id_sk = source.customer_id_sk
	when matched then
	    update set
	        customer_full_name = source.customer_full_name,
	        total_revenue = source.total_revenue,
	        total_orders = source.total_orders,
	        last_order_date = source.last_order_date,
	        avg_revenue_per_order = source.avg_revenue_per_order,
	        avg_order_size = source.avg_order_size,
	        favorite_store = source.favorite_store,
	        favorite_product = source.favorite_product
	when not matched then
	    insert (customer_id_sk, customer_full_name, total_revenue, total_orders, last_order_date, avg_revenue_per_order, avg_order_size, favorite_store, favorite_product)
	    values (source.customer_id_sk, source.customer_full_name, source.total_revenue, source.total_orders, source.last_order_date, source.avg_revenue_per_order, source.avg_order_size, source.favorite_store, source.favorite_product);
		
		
		
		
		
		--staff_performance_analysis	
	merge into gold_layer.staff_performance_analysis as target
	using (
		select 
		    st.staff_id_sk,
		    st.staff_full_name,
		    coalesce(sum(fo.quantity), 0) as total_sales_quantity,
		    coalesce(sum(fo.quantity * fo.list_price * (1 - fo.discount)), 0) as total_sales_revenue,
		    count(distinct fo.order_id_sk) as orders_handled,
		    coalesce(sum(fo.quantity * fo.list_price * (1 - fo.discount)) / count(distinct fo.order_id_sk), 0) as revenue_per_order,
		    count(distinct fo.customer_id_sk) as customer_count,
		    coalesce(avg(fo.discount), 0) as average_discount_given
		from silver_layer.fact_orders fo
		right join silver_layer.dimension_staffs st on fo.staff_id_sk = st.staff_id_sk
		group by st.staff_id_sk
		) as source
	on target.staff_id_sk = source.staff_id_sk
	when matched then
	    update set
	        staff_full_name = source.staff_full_name,
	        total_sales_quantity = source.total_sales_quantity,
	        total_sales_revenue = source.total_sales_revenue,
	        orders_handled = source.orders_handled,
	        revenue_per_order = source.revenue_per_order,
	        customer_count = source.customer_count,
	        average_discount_given = source.average_discount_given
	when not matched then
	    insert (staff_id_sk, staff_full_name, total_sales_quantity, total_sales_revenue, orders_handled, revenue_per_order, customer_count, average_discount_given)
	    values (source.staff_id_sk, source.staff_full_name, source.total_sales_quantity, source.total_sales_revenue, source.orders_handled, source.revenue_per_order, source.customer_count, source.average_discount_given);
		   
		   
	   
	   
		--store_product_analysis
	merge into gold_layer.store_product_analysis as target
	using (
		select 
		    dp.product_id_sk,
		    dp.product_name,
		    dp.category_name,
		    dp.brand_name,
		    sum(fo.quantity) as total_quantity,
		    sum(fo.quantity * fo.list_price * (1 - fo.discount)) as total_revenue,
		    avg(fo.list_price * (1 - fo.discount)) as avg_price,
		    avg(fo.discount) as avg_discount,
		    count(case when fo.discount > 0 then 1 end) as discounted_sales,
		    count(distinct fo.order_id_sk) as total_orders,
		    count(distinct fo.customer_id_sk) as unique_customers,
		    sum(fo.quantity * fo.list_price * (1 - fo.discount)) / nullif(count(distinct fo.order_id_sk), 0) as avg_revenue_per_order,
		    sum(fo.quantity) / nullif(count(distinct fo.order_id_sk), 0) as avg_items_per_order,
		    ds.store_id_sk,
		    ds.store_name,
		    ds.store_city,
		    ds.store_state,
		    ds.store_zip_code
		from silver_layer.fact_orders fo
		join silver_layer.dimension_stores ds on fo.store_id_sk = ds.store_id_sk
		join silver_layer.dimension_products dp on fo.product_id_sk = dp.product_id_sk
		group by ds.store_id_sk, dp.product_id_sk
		) as source
	on target.product_id_sk = source.product_id_sk and target.store_id_sk = source.store_id_sk
	when matched then
	    update set
	        product_id_sk = source.product_id_sk,
	        product_name = source.product_name,
	        category_name = source.category_name,
	        brand_name = source.brand_name,
	        total_quantity = source.total_quantity,
	        total_revenue = source.total_revenue,
	        avg_price = source.avg_price,
	        avg_discount = source.avg_discount,
	        discounted_sales = source.discounted_sales,
	        total_orders = source.total_orders,
	        unique_customers = source.unique_customers,
	        avg_revenue_per_order = source.avg_revenue_per_order,
	        avg_items_per_order = source.avg_items_per_order,
	        store_id_sk = source.store_id_sk,
	        store_name = source.store_name,
	        store_city = source.store_city,
	        store_state = source.store_state,
	        store_zip_code = source.store_zip_code
	when not matched then
	    insert (product_id_sk, product_name, category_name, brand_name, total_quantity, total_revenue, avg_price, avg_discount, discounted_sales, total_orders, unique_customers, avg_revenue_per_order, avg_items_per_order, store_id_sk, store_name, store_city, store_state, store_zip_code)
	    values (source.product_id_sk, source.product_name, source.category_name, source.brand_name, source.total_quantity, source.total_revenue, source.avg_price, source.avg_discount, source.discounted_sales, source.total_orders, source.unique_customers, source.avg_revenue_per_order, source.avg_items_per_order, source.store_id_sk, source.store_name, source.store_city, source.store_state, source.store_zip_code);
$$;	   
