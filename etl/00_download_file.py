import io
import zipfile
import requests
import os

# # Set the URL and the target directory
# URL = 'https://download.inep.gov.br/microdados/microdados_enem_2024.zip'

EXTRACT_TO_DIR = (
    "/Workspace/Users/"
    "mikeias.oliveira@al.infnet.edu.br/"
    "databricks-repo/etl/tmp"
)

# # Download the file
# response = requests.get(URL, verify=False, stream=True)

# with open("./microdados_enem_2024", "wb") as f:
#     for chunk in response.iter_content(chunk_size=1024 * 1024):
#         if chunk:
#             f.write(chunk)


print(os.getcwd())
# Read the bytes into memory and extract everything
with open("./microdados_enem_2024", "rb") as f:
    with zipfile.ZipFile(f) as zip_file:
        zip_file.extractall(EXTRACT_TO_DIR)