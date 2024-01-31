-- Create stores table
CREATE TABLE IF NOT EXISTS stores (store_id int PRIMARY KEY, type text, size int);

-- Create sales table
CREATE TABLE IF NOT EXISTS sales (store_id int, dept_id int, date date, weekly_sales decimal, is_holiday boolean, PRIMARY KEY (store_id, dept_id, date));

-- Create features table
CREATE TABLE IF NOT EXISTS features (store_id int, date date, temperature decimal, fuel_price decimal, markdown1 decimal, markdown2 decimal, markdown3 decimal, markdown4 decimal, markdown5 decimal, cpi decimal, unemployment decimal, is_holiday boolean, PRIMARY KEY (store_id, date));

-- Insert into stores
INSERT INTO stores (store_id, type, size) VALUES (?, ?, ?);

-- Insert into sales
INSERT INTO sales (store_id, dept_id, date, weekly_sales, is_holiday) VALUES (?, ?, ?, ?, ?);

-- Insert into features
INSERT INTO features (store_id, date, temperature, fuel_price, markdown1, markdown2, markdown3, markdown4, markdown5, cpi, unemployment, is_holiday) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- Sales
SELECT * FROM sales;

-- Stores
SELECT store_id, type, size FROM stores;

-- Features
SELECT store_id, date, temperature, fuel_price, unemployment FROM features;