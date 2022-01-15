import os
import re
import logging
from azure.storage.filedatalake import DataLakeServiceClient, FileProperties
from conf import config
from typing import List, Dict, Union

# TODO 
# Finish arg strings

class WrongEnvIsProd(Exception):
    """Exception raised for error when os.environ["ENV_IS_PROD"] not in ["True", "False"]

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, env_is_prod: bool):
        self.message = f"os.environ[\"ENV_IS_PROD\"]: {env_is_prod} is not in [\"True\", \"False\"] " + \
                       "Environment variable \"ENV_IS_PROD\" has to be either the string \"False\" or the string \"True\""
        super().__init__(self.message)


class BlobAccess():
    
    def __init__(self, defaultAzureCredential: bool= False):
        """Constructor for BlobAccess.

        The constructor for BlobAccess can be initialized with og without azure.identity.DefaultAzureCredential.
        By setting DefaultAzureCredential the BlobAccess can be configured for either development machine or for k8s in 
        Dima Blueprint.

        Also the Environment Variable ENV_IS_PROD has to be set to either the string "True" or the string "False".
        By using an environment variable this can be set when running the file as main, when unittesting or when configuring the CD pipeline. 
        The variable is used when initiating the StorageConfigurations class from conf.config.StorageConfigurations where there can be multiple 
        settings for the data lake storage.     

        Args:
            defaultAzureCredential (bool): When developing it is not given, that one can use DefaultAzureCredential. \
                                           Set to false in order to use multifactor authentication. \
                                           See https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python
                                
        Raises:
            WrongEnvIsProd: if os.environ["ENV_IS_PROD] does not exist
            WrongEnvIsProd: if os.environ["ENV_IS_PROD] not in ["True", "False"]
        """

        # Set environment based on environment variable "ENV_IS_PROD".
        # Has to be either exist and be either "False" or "True", else raise exception.
        if "ENV_IS_PROD" in os.environ.keys():

            # If environment variable is not either "True" or "False" raise exception.
            if os.environ["ENV_IS_PROD"] not in ["True", "False"]:

                raise WrongEnvIsProd(env_is_prod=os.environ["ENV_IS_PROD"])

            else:
                # Convert string from environment variable to bool.
                ENV_IS_PROD = os.environ["ENV_IS_PROD"] == "True" 

        else:

            raise WrongEnvIsProd(env_is_prod=os.environ["ENV_IS_PROD"])

        # Get Storage Configurations from config.

        if ENV_IS_PROD:

            storage_configurations = config.storage_configurations_dict["env_is_prod"]

        else:
            
            storage_configurations = config.storage_configurations_dict["env_is_dev"]


        # defaultAzureCredential is set in the __init__
        if not defaultAzureCredential:

            from azure.identity import InteractiveBrowserCredential, DeviceCodeCredential
            
            # Use <<Initials>>@kmd.dk with multifactor.
            # Example xyz@kmd.dk
            # (NOT xyz@dima.kmd.dk)
            credential = DeviceCodeCredential(tenant_id = storage_configurations["tenant_id"], # Dima Tenant
                                                   authority = "login.microsoftonline.com")  # Multifactor Auth via browser
    
        else:
            # When developing it is not given, that one can use DefaultAzureCredential.
            
            from azure.identity import DefaultAzureCredential
                # From https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python
                
                    # A default credential capable of handling most Azure SDK authentication scenarios.
                    # The identity it uses depends on the environment. When an access token is needed, 
                    # it requests one using these identities in turn, stopping when one provides a token:

                        # - A service principal configured by environment variables. See EnvironmentCredential for more details.
                        # - An Azure managed identity. See ManagedIdentityCredential for more details.
                        # - On Windows only: a user who has signed in with a Microsoft application, such as Visual Studio. 
                        #                    If multiple identities are in the cache, then the value of the environment variable 
                        #                    AZURE_USERNAME is used to select which identity to use. 
                        #                    See SharedTokenCacheCredential for more details.
                        # - The user currently signed in to Visual Studio Code.
                        # - The identity currently logged in to the Azure CLI.
                        # - The identity currently logged in to Azure PowerShell.

                    # This default behavior is configurable with keyword arguments.

            credential = DefaultAzureCredential()

        # The data_lake_service_client can then be used to work with the data lake.
        self.data_lake_service_client = DataLakeServiceClient(account_url= storage_configurations["data_lake_endpoint"],
                                                              credential=credential)


    def get_blob_container_names(self) -> List[str]:
        """Get all the blob container names in the configured data lake.

        Returns:
            List (str): Returns a list of strings, one for each blob container name in the data lake set in the conf.config.StorageConfigurations
        """

        file_systems = self.data_lake_service_client.list_file_systems()

        return [i.name for i in file_systems]
        

    def get_file_path_names(self, blob_container_name: str) -> List[str]:
        """Get all the file path names.

        Args:
            blob_container_name (str): a string corresponding to a name of a blob container in the data lake


        Returns:
            List (str): Returns a list of strings, one for each file path name in the blob container taken as argument.
        """

        file_system_client = self.data_lake_service_client.get_file_system_client(blob_container_name)

        return [i.name for i in file_system_client.get_paths()]


    def download_file_to_bytes(self, blob_container_name: str, file_path: str) -> Union[bytes, None]:
        """Download a file from blob container.

        Args:
            blob_container_name (str): Name of blob container.
            file_path (str): Name of file in blob container

        Returns:
            file_data: A byte encoded string of the data.
            None:      If file_path is a folder/directory

        Warning:
            Logging prints warning if file_path is a folder/directory

        Example:
        This example shows how to convert the returned bytes object to a StringIO object that
        for example can be used as input to create a dataframe. Given that the actual file can
        be used for this.

        >>> file = blob_access.download_file("mapunspsc-blob-container", "OP123_unspsc.tsv")
        df = pd.read_csv(StringIO(file_data.decode()), sep="\t")
        df
                UNSPSC                                         UNSPSC_TXT  LEVEL  GL_ACCOUNT
        0     10000000  Levende planter og animalsk materiale og tilbe...      1         NaN
        1     10100000                                        Levende dyr      2         NaN
        2     10101500                                             Husdyr      3         NaN
        3     10101600                                     Fugle og fugle      3         NaN
        4     10101700                                       Levende fisk      3         NaN
        ...        ...                                                ...    ...         ...
        6706  98402000                                             Tillæg      3         NaN
        6707  98500000                                  Fakturadifference      2         NaN
        6708  99000000                                        Ingen Match      1         NaN
        6709  99100000                                        Ingen tekst      2         NaN
        6710  99200000                                     Ikke placerbar      2         NaN

        [6711 rows x 4 columns]
        """

        file_client = self.data_lake_service_client.get_file_client(file_system=blob_container_name, file_path=file_path)

        # Check if type is folder.
        # This can happen when this function is used with the download_pattern function
        if "hdi_isfolder" in file_client.get_file_properties()["metadata"].keys():

            logging.warning(f"The file_path: <<{file_path}>> is a folder!")

            return

        download = file_client.download_file()

        file_data = download.readall()

        # To use the returned bytes object do for example:
        # pd.read_csv(StringIO(file_data.decode()), sep="\t")
        return file_data

    
    def download_pattern(self, blob_container_name: str, regex_pattern: str) -> Dict[str, bytes]:
        """Download all files matching RegEx pattern from blob container.

        Args:
            blob_container_name (str): Name of blob container.
            regex_pattern (str): RegEx pattern to match with filenames in blob container.

        Returns:
            files_dict: A dictionary, {key=file_path: value=file (as bytes object)}.

        Example

        >>> files_dict = blob_access.download_pattern("mapunspsc-blob-container", ".*unspsc*.")
        files_dict
        {'OP123_unspsc.tsv': b'"UNSPSC"\t"UNSPSC_TXT...rbar"\t2\t\r\n', 'unspsc.tsv': b'"UNSPSC"\t"UNSPSC_TXT...rbar"\t2\t\r\n'}
        """

        # Get all filenames in blob container.
        names_list = self.get_file_path_names(blob_container_name)

        # Compile regex_pattern.
        regex_pattern = re.compile(regex_pattern)
        
        # Match all filenames in blob container with compiled regex_pattern.
        file_paths = [i for i in names_list if regex_pattern.match(i)]

        # Create dictionary with key=file_path: value=file as bytes object.
        files_dict = {}
        
        for file_path in file_paths:
            
            files_dict[file_path] = self.download_file_to_bytes(blob_container_name, file_path)

        return files_dict


    def upload_file(self, blob_container_name: str, upload_file_name: str, data: bytes) -> None:
        """Upload file to blob container in specified directory.


        """
        file_system_client = self.data_lake_service_client.get_file_system_client(blob_container_name)

        file_client = file_system_client.create_file(upload_file_name)

        file_client.upload_data(data, overwrite=True)


    def delete_file(self, blob_container_name: str, delete_file_name: str) -> None:
        """Delete file from blob container


        """

        file_system_client = self.data_lake_service_client.get_file_system_client(blob_container_name)

        file_system_client.delete_file(delete_file_name)


    def delete_directory(self, blob_container_name: str, directory_name: str) -> None:
        """Delete directory from blob container


        """

        file_system_client = self.data_lake_service_client.get_file_system_client(blob_container_name)

        file_system_client.delete_directory(directory_name)


if __name__ == "__main__":

    # When running in container make sure to set environmentvariable.
    os.environ["ENV_IS_PROD"] = "False"  # Set to false for multifactor browser auth.

    blob_access = BlobAccess(defaultAzureCredential=False)
    
    blob_container_names = blob_access.get_blob_container_names()
    print(f"BLOB_NAMES\n{blob_container_names}")

    if len(blob_container_names) > 0:
        print(f"BLOB_PATH_NAMES\n{blob_access.get_file_path_names(blob_container_names[0])}")

    file = blob_access.download_file_to_bytes(blob_container_name="mapunspsc-blob-container", file_path="OP123_unspsc.tsv")

    files = blob_access.download_pattern(blob_container_name="mapunspsc-blob-container", regex_pattern=".*test*.")

    blob_access.upload_file(blob_container_name="mapunspsc-blob-container", upload_file_name="testdir/test.tsv", data=file)

    blob_access.delete_file(blob_container_name="mapunspsc-blob-container", delete_file_name="testdir/test.tsv")

    blob_access.delete_directory(blob_container_name="mapunspsc-blob-container", directory_name="testdir")