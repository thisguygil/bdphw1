from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import pandas as pd

# Path to the quieries file and datasets
queries_file_path = 'queries.cql'
stores_dataset_path = 'datasets/stores dataset.csv'
sales_dataset_path = 'datasets/sales dataset.csv'
features_dataset_path = 'datasets/features dataset.csv'

# Function to read a query from a file
def read_query(file_path, query_name):
    with open(file_path, 'r') as file:
        queries = file.read().split(';')
        for query in queries:
            if query.strip().startswith('-- ' + query_name):
                return query.split('-- ' + query_name)[-1].strip()
    return None

# Function to prepare data for the features table
def prepare_features_data(row):
    # Convert 'NA' and empty strings to None for all numeric fields
    numeric_fields = ['Temperature', 'Fuel_Price', 'MarkDown1', 'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5', 'CPI', 'Unemployment']
    for field in numeric_fields:
        if pd.isna(row[field]) or row[field] == '' or row[field] == 'NA':
            row[field] = None
        else:
            row[field] = float(row[field])
    # Format the date
    formatted_date = pd.to_datetime(row['Date'], dayfirst=True).strftime('%Y-%m-%d')
    return (row['Store'], formatted_date, row['Temperature'], row['Fuel_Price'], row['MarkDown1'], row['MarkDown2'], row['MarkDown3'], row['MarkDown4'], row['MarkDown5'], row['CPI'], row['Unemployment'], row['IsHoliday'])

# Function to asynchronously execute batch insertions
def async_batch_insert(dataframe, insert_query, batch_size=500):
    for i in range(0, len(dataframe), batch_size):
        batch = dataframe.iloc[i:i + batch_size]
        arguments = [tuple(x) for x in batch.to_numpy()]
        execute_concurrent_with_args(session, insert_query, arguments)

# Connect to Cassandra and set up the session and keyspace
cluster = Cluster(['localhost'])
session = cluster.connect()
keyspace = "retail_data"
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}")
session.set_keyspace(keyspace)

# Get queries from file
create_stores_query = read_query(queries_file_path, 'Create stores table')
create_sales_query = read_query(queries_file_path, 'Create sales table')
create_features_query = read_query(queries_file_path, 'Create features table')

# Create tables
session.execute(create_stores_query)
session.execute(create_sales_query)
session.execute(create_features_query)

# Read CSV files
stores_df = pd.read_csv(stores_dataset_path)
sales_df = pd.read_csv(sales_dataset_path)
features_df = pd.read_csv(features_dataset_path)

# Prepare insert queries
insert_stores_query = read_query(queries_file_path, 'Insert into stores')
insert_sales_query = read_query(queries_file_path, 'Insert into sales')
insert_features_query = read_query(queries_file_path, 'Insert into features')
prepared_insert_stores = session.prepare(insert_stores_query)
prepared_insert_sales = session.prepare(insert_sales_query)
prepared_insert_features = session.prepare(insert_features_query)

# Convert dates to the correct format
sales_df['Date'] = pd.to_datetime(sales_df['Date'], dayfirst=True).dt.strftime('%Y-%m-%d')

# Ingest stores data
stores_df.apply(lambda row: session.execute(prepared_insert_stores, (row['Store'], row['Type'], row['Size'])), axis=1)
print("Stores data ingested successfully!")

# Ingest sales data asynchronously in batches (since the dataset has over 400,000 rows)
async_batch_insert(sales_df, prepared_insert_sales, batch_size=500)
print("Sales data ingested successfully!")

# Ingest features data (could be done in batches, but it's not necessary since the dataset has only about 8000 rows)
features_df.apply(lambda row: session.execute(prepared_insert_features, prepare_features_data(row)), axis=1)
print("Features data ingested successfully!")

# Close the connection
cluster.shutdown()