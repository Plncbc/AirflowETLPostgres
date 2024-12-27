create or replace procedure bronze_layer.from_external_to_bronze()
language sql
as $$

	    --brands
	insert into bronze_layer.brands
	select *
	from raw.foreign_brands
	where not exists (
		select 1
		from bronze_layer.brands
		where brands.brand_id = raw.foreign_brands.brand_id
	);
	
	
	--categories
	insert into bronze_layer.categories
	select *
	from raw.foreign_categories
	where not exists (
		select 1
		from bronze_layer.categories
		where categories.category_id = raw.foreign_categories.category_id
	);
	
	
	--customers
	insert into bronze_layer.customers
	select 
		customer_id,
		customer_first_name,
		customer_last_name,
		case 
			when customer_phone = 'null' then null
			when customer_phone = 'NULL' then null
			else customer_phone
		end,
		customer_email,
		customer_street,
		customer_city,
		customer_state,
		customer_zip_code
	from raw.foreign_customers
	where not exists (
		select 1
		from bronze_layer.customers
		where customers.customer_id = raw.foreign_customers.customer_id
	);
	
	
	
	--products
	insert into bronze_layer.products
	select *
	from raw.foreign_products
	where not exists (
		select 1
		from bronze_layer.products
		where products.product_id = raw.foreign_products.product_id
	);
	
	
	--stores
	insert into bronze_layer.stores
	select *
	from raw.foreign_stores
	where not exists (
		select 1
		from bronze_layer.stores
		where stores.store_id = raw.foreign_stores.store_id
	);
	
	
	--staffs
	insert into bronze_layer.staffs
	select *
	from raw.foreign_staffs
	where not exists (
		select 1
		from bronze_layer.staffs
		where staffs.staff_id = raw.foreign_staffs.staff_id
	);
	
	
	--orders
	insert into bronze_layer.orders
	select *
	from raw.foreign_orders
	where not exists (
		select 1
		from bronze_layer.orders
		where orders.order_id = raw.foreign_orders.order_id
	);
	
	
	--order_items
	insert into bronze_layer.order_items
	select *
	from raw.foreign_order_items as foi
	where not exists (
	    select 1 
	    from bronze_layer.order_items as oi
	    where oi.order_id = foi.order_id and oi.item_id = foi.item_id
	);
$$;
