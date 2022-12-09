import os
import zipfile

#Zip



# print('Get current working directory : ', os.getcwd())

filenames = [ "main.py", "requirements.txt",".gitignore", "bigquery_schema/avg_delay_flight_nums.json", "table_module.py", "load_module.py"]
targetdir = "/Users/josimardossantosjunior/Code/general_project/cloud-function/cloud-function.zip"
with zipfile.ZipFile(targetdir, mode="w") as archive:
    for filename in filenames:
        archive.write(filename)

#Unzip
# file = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud-function-test/function-source.zip"
# targetdir = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud-function-test/"
# with zipfile.ZipFile(file,"r") as zip_ref:
#     zip_ref.extractall(targetdir)


