



#Get bigquery json schema and parse do data-proc job list-tuple schema
import json
  
# Opening JSON file
f = open('avg_delay_flight_nums.json')
data = json.load(f)

# print(type(data))
print()
schema_list = []
for i in data:
    schema_list.append((i["name"],i["type"],i["mode"]))
    
print(schema_list)
