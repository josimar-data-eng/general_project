import zipfile

#Zip
filenames = [ "main.py", "requirements.txt", ".gitignore","bigquery_schema","table_module.py","load_module.py"]
targetdir = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud-function/cloud-function.zip"
with zipfile.ZipFile(targetdir, mode="w") as archive:
    for filename in filenames:
        archive.write(filename)

#Unzip
# file = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud-function-test/function-source.zip"
# targetdir = "/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud-function-test/"
# with zipfile.ZipFile(file,"r") as zip_ref:
#     zip_ref.extractall(targetdir)


