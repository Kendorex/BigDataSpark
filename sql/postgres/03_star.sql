
CREATE TABLE IF NOT EXISTS public.dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id BIGINT NOT NULL UNIQUE
        REFERENCES public.mock_data(source_row_id),
    source_customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS public.dim_seller (
    seller_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id BIGINT NOT NULL UNIQUE
        REFERENCES public.mock_data(source_row_id),
    source_seller_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS public.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id BIGINT NOT NULL UNIQUE
        REFERENCES public.mock_data(source_row_id),
    source_product_id INTEGER,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(12,2),
    product_quantity INTEGER,
    pet_category TEXT,
    product_weight NUMERIC(10,2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3,1),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE IF NOT EXISTS public.dim_store (
    store_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT
);

CREATE TABLE IF NOT EXISTS public.dim_supplier (
    supplier_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER
);


CREATE TABLE IF NOT EXISTS public.map_store (
    source_row_id BIGINT PRIMARY KEY
        REFERENCES public.mock_data(source_row_id),
    store_key BIGINT NOT NULL
        REFERENCES public.dim_store(store_key)
);

CREATE TABLE IF NOT EXISTS public.map_supplier (
    source_row_id BIGINT PRIMARY KEY
        REFERENCES public.mock_data(source_row_id),
    supplier_key BIGINT NOT NULL
        REFERENCES public.dim_supplier(supplier_key)
);

CREATE TABLE IF NOT EXISTS public.fact_sales (
    sale_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_id BIGINT NOT NULL UNIQUE
        REFERENCES public.mock_data(source_row_id),
    customer_key BIGINT NOT NULL
        REFERENCES public.dim_customer(customer_key),
    seller_key BIGINT NOT NULL
        REFERENCES public.dim_seller(seller_key),
    product_key BIGINT NOT NULL
        REFERENCES public.dim_product(product_key),
    store_key BIGINT NOT NULL
        REFERENCES public.dim_store(store_key),
    supplier_key BIGINT NOT NULL
        REFERENCES public.dim_supplier(supplier_key),
    date_key INTEGER NOT NULL
        REFERENCES public.dim_date(date_key),
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12,2)
);

