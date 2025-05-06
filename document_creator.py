import pandas as pd
import json
df = pd.read_csv('Reviews.csv')

print(df.info())

records=df.to_dict(orient='records')

with open('Reviews.json', 'w') as outfile:
    json.dump(records, outfile, indent=4)
