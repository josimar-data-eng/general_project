
import os
import json
import pandas as pd
from datetime import datetime

# print('Get current working directory : ', os.getcwd())

# Json with 1 row
# path_json_file = "data-proc-job/avg_delays_by_flight_nums.json"
# json_file = open(path_json_file)
# dict_file = json.load(json_file)

# student_df = pd.DataFrame(dict_file,index=[0])
# print(student_df)




# Json with multile rows
# json_list = []
# with open("avg.json") as file:
#     for each_row in file:
#         dict_file = json.loads(each_row)
#         json_list.append(dict_file)

# avg_df = pd.DataFrame(json_list)
# avg_df["load_datetime"] = datetime.now()

# print(avg_df)


def func(num: int):
    print(num * 2)
func(float(3.14))


