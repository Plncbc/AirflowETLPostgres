create schema if not exists raw;

	--external table
	create extension if not exists file_fdw schema raw;
	create server if not exists file_server foreign data wrapper file_fdw;
	
	--brands
	create foreign table if not exists raw.foreign_brands (
		brand_id int,
		brand_name varchar (255)
	) server file_server
	options (
	    filename '/db/brands.csv',
	    format 'csv',
	    header 'true',
	    delimiter ','
	);
	
	
	--categories
	create foreign table if not exists raw.foreign_categories (
		category_id int,
		category_name varchar (255)
	) server file_server
	options (
	    filename '/db/categories.csv',
	    format 'csv',
	    header 'true',
	    delimiter ','
	);
	
	
	--customers
	create foreign table if not exists raw.foreign_customers (
		customer_id int,
		customer_first_name varchar (255),
		customer_last_name varchar (255),
		customer_phone varchar (25),
		customer_email varchar (255),
		customer_street varchar (255),
		customer_city varchar (50),
		customer_state varchar (25),
		customer_zip_code varchar (5)
	) server file_server
	options (
	    filename '/db/customers.csv',
	    format 'csv',
	    header 'true',
	    delimiter ','
	);
	
	
	--products
	create foreign table if not exists raw.foreign_products (
		product_id int,
		product_name varchar (255),
		brand_id int,
		category_id int,
		model_year int,
		list_price decimal (10, 2)
	) server file_server
	options (
	    filename '/db/products.csv',
	    format 'csv',
	    header 'true',
	    delimiter ','
	);
	
	
	--stores
	create foreign table if not exists raw.foreign_stores (
		store_id int,
		store_name varchar (255),
		store_phone varchar (25),
		store_email varchar (255),
		store_street varchar (255),
		store_city varchar (255),
		store_state varchar (10),
		store_zip_code varchar (5)
	) server file_server
	options (
	    filename '/db/stores.csv',
	    format 'csv',
	    header 'true',
	    delimiter ','
	);
	
	
	--staffs
	create foreign table if not exists raw.foreign_staffs (
		staff_id int,
		staff_first_name varchar (255),
		staff_last_name varchar (255),
		staff_email varchar (255),
		staff_phone varchar (25),
		staff_active int,
		store_id int,
		manager_id int
	) server file_server
	options (
	    filename '/db/staffs.csv',
	    format 'csv',
	    header 'true',
	    delimiter ','
	);
	
	
	--foreign_orders
	create foreign table if not exists raw.foreign_orders (
		order_id int,
		customer_id int,
		order_status int,
		order_date date,
		required_date date,
		shipped_date date,
		store_id int,
		staff_id int
	) server file_server
	options (
	    filename '/db/orders.csv',
	    format 'csv',
	    header 'true',
	    delimiter ','
	);
	
	
	--order_items
	create foreign table if not exists raw.foreign_order_items(
		order_id int,
		item_id int,
		product_id int,
		quantity int,
		list_price decimal (10, 2),
		discount decimal (4, 2)
	) server file_server
	options (
	    filename '/db/order_items.csv',
	    format 'csv',
	    header 'true',
	    delimiter ','
	);
-----------------------------------------------



--bronze_layer
create schema if not exists bronze_layer;

	--brands
	create table if not exists bronze_layer.brands (
		brand_id int primary key,
		brand_name varchar (255) not null
	);
	
	
	
	--categories
	create table if not exists bronze_layer.categories (
		category_id int primary key,
		category_name varchar (255) not null
	);
	
	
	
	--customers
	create table if not exists bronze_layer.customers (
		customer_id int primary key,
		customer_first_name varchar (255) not null,
		customer_last_name varchar (255) not null,
		customer_phone varchar (25),
		customer_email varchar (255) not null,
		customer_street varchar (255),
		customer_city varchar (50),
		customer_state varchar (25),
		customer_zip_code varchar (5)
	);
	
	
	
	--products
	create table if not exists bronze_layer.products (
		product_id int primary key,
		product_name varchar (255) not null,
		brand_id int not null,
		category_id int not null,
		model_year int not null,
		list_price decimal (10, 2) not null,
		foreign key (category_id) 
	        references bronze_layer.categories (category_id) 
	        on delete cascade on update cascade,
		foreign key (brand_id) 
	        references bronze_layer.brands (brand_id) 
	        on delete cascade on update cascade
	);
	
	
	
	--stores
	create table if not exists bronze_layer.stores (
		store_id int primary key,
		store_name varchar (255) not null,
		phone varchar (25),
		email varchar (255),
		street varchar (255),
		city varchar (255),
		state varchar (10),
		zip_code varchar (5)
	);
	
	
	
	--staffs
	create table if not exists bronze_layer.staffs (
		staff_id int primary key,
		staff_first_name varchar (255) not null,
		staff_last_name varchar (255) not null,
		staff_email varchar (255) not null unique,
		staff_phone varchar (25),
		staff_active int not null,
		store_id int not null,
		manager_id int,
		foreign key (store_id) 
	        references bronze_layer.stores (store_id) 
	        on delete cascade on update cascade,
		foreign key (manager_id) 
	        references bronze_layer.staffs (staff_id) 
	        on delete no action on update no action
	);
	
	
	--orders
	create table if not exists bronze_layer.orders (
		order_id int primary key,
		customer_id int,
		order_status int not null,
		order_date date not null,
		required_date date not null,
		shipped_date date,
		store_id int not null,
		staff_id int not null,
		foreign key (customer_id) 
	        references bronze_layer.customers (customer_id) 
	        on delete cascade on update cascade,
		foreign key (store_id) 
	        references bronze_layer.stores (store_id) 
	        on delete cascade on update cascade,
		foreign key (staff_id) 
	        references bronze_layer.staffs (staff_id) 
	        on delete no action on update no action
	);
	
	
	
	--order_items
	create table if not exists bronze_layer.order_items(
		order_id int,
		item_id int,
		product_id int not null,
		quantity int not null,
		list_price decimal (10, 2) not null,
		discount decimal (4, 2) not null default 0,
		primary key (order_id, item_id),
		foreign key (order_id) 
	        references bronze_layer.orders (order_id) 
	        on delete cascade on update cascade,
		foreign key (product_id) 
	        references bronze_layer.products (product_id) 
	        on delete cascade on update cascade
	);
------------------------------------------------------------


--silver_layer
create schema if not exists silver_layer;

	--dimension products
	create table if not exists silver_layer.dimension_products (
		product_id_sk serial primary key,
		product_id_nk int unique not null,
		product_name varchar (255) not null,
		brand_name varchar (255) not null,
		category_name varchar (255) not null,
		model_year int not null,
		list_price decimal (10, 2) not null
	);
	
	
	
	--dimension customers
	create table if not exists silver_layer.dimension_customers (
		customer_id_sk serial primary key,
		customer_id_nk int unique not null,
		customer_full_name varchar (255) not null,
		customer_first_name varchar (255) not null,
		customer_last_name varchar (255) not null,
		customer_phone varchar (25),
		customer_email varchar (255) not null,
		customer_street varchar (255),
		customer_city varchar (255),
		customer_state varchar (25),
		customer_zip_code varchar (5)
	);
	
	
	
	--dimension stores
	create table if not exists silver_layer.dimension_stores (
		store_id_sk serial primary key,
		store_id_nk int unique not null,
		store_name varchar (255) not null,
		store_phone varchar (255),
		store_email varchar (255),
		store_street varchar (255),
		store_city varchar (255),
		store_state varchar (10),
		store_zip_code varchar (5)
	);
	
	
	
	--dimension staffs
	create table if not exists silver_layer.dimension_staffs (
		staff_id_sk serial primary key,
		staff_id_nk int unique not null,
		staff_full_name varchar (255) not null,
		staff_first_name varchar (255) not null,
		staff_last_name varchar (255) not null,
		staff_email varchar (255) not null unique,
		staff_phone varchar (25),
		staff_active boolean not null
	);
	
	
	
	--fact_orders
	create table if not exists silver_layer.fact_orders (
		order_id_sk int not null default -1,
		order_id_nk int not null,
		order_status int not null,
		order_date date not null,
		required_date date not null,
		shipped_date date, 
		item_id int,
		quantity int not null,
		list_price decimal (10, 2) not null,
		discount decimal (4, 2) not null default 0,
		customer_id_sk int not null default -1,
		customer_id_nk int not null,
		store_id_sk int not null default -1,
		store_id_nk int not null,
		staff_id_sk int not null default -1,
		staff_id_nk int not null,
		product_id_sk int not null default -1,
		product_id_nk int not null
	);
---------------------------------------------------------


--gold_layer
create schema if not exists gold_layer;

	--sales overview
	create table if not exists gold_layer.sales_overview (
	    time_period varchar(7),
	    total_revenue decimal(15, 2),
	    total_orders int,
	    avg_order_value decimal(10, 2),
	    unique_customers int,
	    total_discounts decimal(15, 2),
	    gross_revenue decimal(15, 2),
	    avg_delivery_time decimal(10, 2)
	);
	
	
	
	-- customer segmentation
	create table if not exists gold_layer.customer_segmentation (
		customer_id_sk int not null,
	    customer_full_name varchar(255),
	    total_revenue decimal(15, 2),
	    total_orders int,
	    last_order_date date,
	    avg_revenue_per_order decimal(10, 2),
	    avg_order_size decimal(10, 2),
	    favorite_store varchar (255),
	    favorite_product varchar (255)
	);
	
	
	
	--staff_performance_analysis
	create table if not exists gold_layer.staff_performance_analysis (
	    staff_id_sk int,
	    staff_full_name varchar(255),
	    total_sales_quantity int,
	    total_sales_revenue decimal(15, 2),
	    orders_handled int,
	    revenue_per_order decimal(10, 2),
	    customer_count int,
	    average_discount_given decimal(10, 2)
	);
	
	
	
	--store_product_analysis
	create table if not exists gold_layer.store_product_analysis (
		product_id_sk int,
		product_name varchar(255),
		category_name varchar(255),
		brand_name varchar(255),
		total_quantity int,
		total_revenue decimal(15, 2),
		avg_price decimal(10, 2),
		avg_discount decimal(10, 2),
		discounted_sales int,
		total_orders int,
		avg_revenue_per_order decimal(10, 2),
		avg_items_per_order decimal(10, 2),
		store_id_sk int,
		store_name varchar(255),
		store_city varchar(255),
		store_state varchar(25),
		store_zip_code varchar(25)
	);
