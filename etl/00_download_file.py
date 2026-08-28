import io
import zipfile
import requests

# Set the URL and the target directory
URL = 'https://download.inep.gov.br/microdados/microdados_enem_2025.zip'

EXTRACT_TO_DIR = SOURCE_PATH = (
    "file:/Workspace/Users/"
    "mikeias.oliveira@al.infnet.edu.br/"
    "databricks-repo/etl/microdados_enem_2025"
)

# Download the file
with requests.get(URL, stream=True,verify=False) as response:
    with open(
        "../../microdados_enem_2025", "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

# Read the bytes into memory and extract everything
with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
    zip_file.extractall(EXTRACT_TO_DIR)