from cassandra.cluster import Cluster
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Path to the queries file
queries_file_path = 'queries.cql'

# Function to read a query from a file
def read_query(file_path, query_name):
    with open(file_path, 'r') as file:
        queries = file.read().split(';')
        for query in queries:
            if query.strip().startswith('-- ' + query_name):
                return query.split('-- ' + query_name)[-1].strip()
    return None

# Connect to the Cassandra cluster
cluster = Cluster(['localhost'])
session = cluster.connect('retail_data')

# Function to execute CQL query and return results as a DataFrame
def query_to_df(query):
    rows = session.execute(query)
    return pd.DataFrame(list(rows))

# Get queries from file
total_sales_query = read_query(queries_file_path, 'Total sales per store')
stores_query = read_query(queries_file_path, 'Stores')
sales_query = read_query(queries_file_path, 'Sales')
features_query = read_query(queries_file_path, 'Features')

# Execute queries
stores_df = query_to_df(stores_query)
sales_df = query_to_df(sales_query)
features_df = query_to_df(features_query)

# Aggregate using Pandas
merged_df = pd.merge(sales_df, stores_df, on='store_id', how='left')
merged_df = pd.merge(merged_df, features_df, on=['store_id', 'date'], how='left')
total_sales_per_store_df = sales_df.groupby('store_id')['weekly_sales'].sum().reset_index()
sales_by_type_size = merged_df.groupby(['type', 'size'])['weekly_sales'].sum().reset_index()
avg_weekly_sales_dept_df = sales_df.groupby('dept_id')['weekly_sales'].mean().reset_index()
sales_holiday_vs_nonholiday_df = sales_df.groupby('is_holiday')['weekly_sales'].sum().reset_index()
sales_by_external_factors = merged_df.groupby(['fuel_price', 'unemployment'])['weekly_sales'].sum().reset_index()
sales_by_temperature = merged_df.groupby('temperature')['weekly_sales'].sum().reset_index()

# Prepare data for plotting
total_sales_per_store_df['weekly_sales'] = pd.to_numeric(total_sales_per_store_df['weekly_sales'], errors='coerce')
sales_by_type_size['weekly_sales'] = pd.to_numeric(sales_by_type_size['weekly_sales'], errors='coerce')
sales_by_type_size['size'] = sales_by_type_size['size'].astype(str)
sales_holiday_vs_nonholiday_df['weekly_sales'] = pd.to_numeric(sales_holiday_vs_nonholiday_df['weekly_sales'], errors='coerce')
sales_holiday_vs_nonholiday_df['is_holiday'] = sales_holiday_vs_nonholiday_df['is_holiday'].map({True: 'Holiday', False: 'Non-Holiday'})

# Set the style for the plots
sns.set(style='whitegrid')

# Visualize total sales per store
total_sales_per_store_df.plot(kind='bar', x='store_id', y='weekly_sales', title='Total Sales per Store')
plt.xlabel('Store ID')
plt.ylabel('Total Sales')
plt.legend().remove()
plt.show()

# Visualize total sales by store size and type
sales_by_type_size.pivot(index='size', columns='type', values='weekly_sales').plot(kind='bar')
plt.title("Total Sales by Store Size and Type")
plt.xlabel("Store Size")
plt.ylabel("Total Sales")
plt.legend(title='Store Type')
plt.tight_layout()
plt.show()

# Plot average weekly sales for each department
avg_weekly_sales_dept_df.plot(kind='bar', x='dept_id', y='weekly_sales', title='Average Weekly Sales per Department')
plt.xlabel('Department ID')
plt.ylabel('Average Weekly Sales')
plt.ylim(bottom=0)
plt.legend().remove()
plt.show()

# Plot sales during holiday weeks vs. non-holiday weeks
sales_holiday_vs_nonholiday_df.plot(kind='bar', x='is_holiday', y='weekly_sales', title='Sales: Holiday Weeks vs Non-Holiday Weeks')
plt.xlabel('Is Holiday')
plt.ylabel('Weekly Sales')
plt.xticks(rotation=45)
plt.legend().remove()
plt.show()

# Plot Fuel Prices vs Sales
plt.scatter(sales_by_external_factors['fuel_price'], sales_by_external_factors['weekly_sales'])
plt.title("Sales vs. Fuel Price")
plt.xlabel("Fuel Price")
plt.ylabel("Total Sales")
plt.ylim(bottom=0)
plt.show()

# Plot Unemployment vs Sales
plt.scatter(sales_by_external_factors['unemployment'], sales_by_external_factors['weekly_sales'])
plt.title("Sales vs. Unemployment")
plt.xlabel("Unemployment Rate")
plt.ylabel("Total Sales")
plt.ylim(bottom=0)
plt.show()

# Plot Temperature vs Sales
plt.scatter(sales_by_temperature['temperature'], sales_by_temperature['weekly_sales'])
plt.title("Sales vs. Temperature")
plt.xlabel("Temperature")
plt.ylabel("Total Sales")
plt.ylim(bottom=0)
plt.show()

# Close the connection
cluster.shutdown()