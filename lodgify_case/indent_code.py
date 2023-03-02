import os
import subprocess

paths = [".", "raw", "staging", "sandbox"]
# print(os.listdir('.')) -->> Get files from current directory

for path in paths:
    for file in os.listdir(path):
        if (file[-3:]) == ".py":
            if path == ".":
                bashCommand = f"black {file}"
            else:
                bashCommand = f"black {path}/{file}"
            process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
