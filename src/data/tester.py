import os

# Full path to the file on the external hard drive
file_path = "F:/sds_extract/FSEID-00006734/kontakter.asc"

# Check if the file exists
if os.path.exists(file_path):
    # File exists, proceed with loading it
    with open(file_path, 'r') as file:
        # Read the file content or perform other operations
        content = file.read()
        print(content)
else:
    print("File not found:", file_path)
