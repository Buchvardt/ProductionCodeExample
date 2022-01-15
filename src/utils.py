import pandas as pd
import os
import json
from io import StringIO
from typing import Dict
from src.blobstorage import BlobAccess

def load_unspsc_mapping_from_blob(municipality_number: str) -> str:
    """Load unspsc mapping for a specific municipality.


    Returns:
        Dict: returns a dictionary with unspsc as key and GL account as value.

    """

    # TODO get from a config.
    blob_container_name = "mapunspsc-blob-container"

    file_path = f"OP{municipality_number}_unspsc.tsv"

    # TODO set defaultAzureCredential based on config.
    blob_access = BlobAccess(defaultAzureCredential=False)


    file_data = blob_access.download_file_to_bytes(blob_container_name=blob_container_name,
                                              file_path=file_path)

    df = pd.read_csv(StringIO(file_data.decode()), sep="\t")

    # Reorder columns
    df = df[["GL_ACCOUNT", "LEVEL", "UNSPSC", "UNSPSC_TXT"]]

    records_list = df.to_dict(orient="records")

    data = {"CONTROL_FIELDS": {"CLIENT": municipality_number},
            "UNSPSC": records_list
            }

    data1 = json.dumps(data)

    
    data2 = "{\"CONTROL_FIELDS\": {\"CLIENT\":" + municipality_number +"}, \"UNSPSC\":" + records_list + "}"
    
    return json.dumps(data, indent=4, ensure_ascii=False)


if __name__ =="__main__":

    os.environ["ENV_IS_PROD"] = "False"
    
    unspsc_mapping = load_unspsc_mapping_from_blob("123")