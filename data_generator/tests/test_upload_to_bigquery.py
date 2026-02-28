from data_generator.generate_events import generate_billing_events
from data_generator.generate_updates import generate_updates
from data_generator.upload_to_bigquery import initial_load, incremental_load
from google.cloud import bigquery

print(bigquery.Client().project)

df1 = generate_billing_events(num_customers=50, months_back=6)
initial_load(df1)

df2 = generate_updates(df1)
incremental_load(df2)
