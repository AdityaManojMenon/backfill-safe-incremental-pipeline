from data_generator.generate_events import generate_billing_events
from data_generator.generate_updates import generate_updates
from upload_to_bigquery import upload_pipeline

df = generate_billing_events()
updated = generate_updates(df)

upload_pipeline(updated)