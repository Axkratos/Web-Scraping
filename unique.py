import pandas as pd
data = pd.read_csv('merotutor_data.csv')
unique_emails_count = data['Email'].nunique()
print(f"Unique emails count: {unique_emails_count}")
