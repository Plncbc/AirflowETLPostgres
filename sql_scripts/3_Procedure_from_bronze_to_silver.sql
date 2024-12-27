create or replace procedure silver_layer.from_bronze_to_silver()
language sql
as $$
	--dimension products
	insert into silver_layer.dimension_products (product_id_nk, product_name, brand_name, category_name, model_year, list_price)
	select 
		product_id, 
		product_name, 
		brand_name, 
		category_name, 
		model_year, 
		list_price 
	from bronze_layer.products p 
	join bronze_layer.brands b on b.brand_id = p.brand_id 
	join bronze_layer.categories c on c.category_id = p.category_id
	where not exists (
		select 1
		from silver_layer.dimension_products
		where dimension_products.product_id_nk = p.product_id
	);
	
	
	
	--dimension customers
	insert into silver_layer.dimension_customers (customer_id_nk, customer_full_name, customer_first_name, customer_last_name, customer_phone, customer_email, customer_street, customer_city, customer_state, customer_zip_code)
	select 
		customer_id,  
		concat(customer_first_name, ' ', customer_last_name),
		customer_first_name,
		customer_last_name,
		case 
			when customer_phone is not null then repeat('*', length(customer_phone) - 3) || right(customer_phone, 3)
			else customer_phone
		end,
		customer_email,
		customer_street, 
		customer_city, 
		customer_state, 
		customer_zip_code 
	from bronze_layer.customers
	where not exists (
		select 1
		from silver_layer.dimension_customers
		where silver_layer.dimension_customers.customer_id_nk = bronze_layer.customers.customer_id
	);
	
	
	
	--dimension stores
	insert into silver_layer.dimension_stores (store_id_nk, store_name, store_phone, store_email, store_street, store_city, store_state, store_zip_code)
	select *
	from bronze_layer.stores
	where not exists (
		select 1
		from silver_layer.dimension_stores
		where silver_layer.dimension_stores.store_id_nk = bronze_layer.stores.store_id
	);
	
	
	
	--dimension staffs
	insert into silver_layer.dimension_staffs (staff_id_nk, staff_full_name, staff_first_name, staff_last_name, staff_email, staff_phone, staff_active)
	select staff_id, concat(staff_first_name, ' ',staff_last_name), staff_first_name, staff_last_name, staff_email, staff_phone, cast(staff_active as boolean)
	from bronze_layer.staffs
	where not exists (
		select 1
		from silver_layer.dimension_staffs
		where silver_layer.dimension_staffs.staff_id_nk = bronze_layer.staffs.staff_id
	);
	
	
	
	--fact_orders
	with orders as(
		select 
			o.order_id, 
			item_id, 
			customer_id, 
			order_status, 
			order_date, 
			required_date, 
			shipped_date, 
			quantity, 
			list_price, 
			discount, 
			store_id, 
			staff_id, 
			product_id
		from bronze_layer.orders o 
		join bronze_layer.order_items oi 
		on o.order_id = oi.order_id
	)
	insert into silver_layer.fact_orders 
		select 
			o.order_id,
			o.order_id, 
			order_status, 
			order_date, 
			required_date, 
			shipped_date, 
			item_id, 
			quantity, 
			o.list_price, 
			discount,
			coalesce(customer_id_nk, -1), customer_id_nk, 
			coalesce(store_id_nk, -1), store_id_nk, 
			coalesce(staff_id_nk, -1), staff_id_nk, 
			coalesce(product_id_nk, -1), product_id_nk
		from orders o
		left join silver_layer.dimension_customers dc on dc.customer_id_nk = o.customer_id
		left join silver_layer.dimension_products dp on dp.product_id_nk = o.product_id
		left join silver_layer.dimension_staffs ds on ds.staff_id_nk = o.staff_id
		left join silver_layer.dimension_stores dstore on dstore.store_id_nk = o.store_id;
	
		
	--обновление связей с dimensions
	update silver_layer.fact_orders
	set customer_id_sk = coalesce(
			(select dc.customer_id_sk
			from silver_layer.fact_orders
			join silver_layer.dimension_customers dc on dc.customer_id_nk = fact_orders.customer_id_nk)
		,-1)
	where customer_id_sk = -1;
	
	update silver_layer.fact_orders
	set product_id_sk = coalesce(
			(select dp.product_id_sk
			from silver_layer.fact_orders
			join silver_layer.dimension_products dp on dp.product_id_nk = fact_orders.product_id_nk)
		,-1)
	where product_id_sk = -1;
	
	update silver_layer.fact_orders
	set staff_id_sk = coalesce(
			(select ds.staff_id_sk
			from silver_layer.fact_orders
			join silver_layer.dimension_staffs ds on ds.staff_id_nk = fact_orders.staff_id_nk)
		,-1)
	where staff_id_sk = -1;
	
	update silver_layer.fact_orders
	set store_id_sk = coalesce(
			(select ds.store_id_sk
			from silver_layer.fact_orders
			join silver_layer.dimension_stores ds on ds.store_id_nk = fact_orders.store_id_nk)
		,-1)
	where store_id_sk = -1;
	
	
	--дедупликация
	delete 
		from silver_layer.fact_orders
	where order_id_sk in (select order_id_sk
		from (select *, row_number() over (partition by order_id_nk, customer_id_nk, store_id_nk, staff_id_nk, product_id_nk) as rw
			from silver_layer.fact_orders)
		where rw > 1);
$$;
