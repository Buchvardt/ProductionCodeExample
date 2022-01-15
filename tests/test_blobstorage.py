import unittest
import os
from src.blobstorage import BlobAccess

class TestBlobStorage(unittest.TestCase):

    def setUp(self):
        
        #TODO test environment variable "develop"/"production"
        # Set ENV_IS_PROD
        os.environ["ENV_IS_PROD"] = "False"
        
        # Get Access
        self.blob_access = BlobAccess(defaultAzureCredential=False)

        # A blob container we know exists
        self.blob_container_name = 'mapunspsc-blob-container'

        # A file we know exists
        self.blob_file_path = 'unspsc.tsv'

        # Test dir
        self.test_dir = "testdir"

        # Test file names
        self.test_file_names = [f"{self.test_dir}/OP789_unspsc.tsv", 
                                f"{self.test_dir}/wrong_name.tsv"]


    def test_get_blob_container_names(self):

        blob_container_names = self.blob_access.get_blob_container_names()

        check = self.blob_container_name in blob_container_names

        self.assertTrue(check)


    def test_get_file_path_names(self):

        file_names = self.blob_access.get_file_path_names(self.blob_container_name)

        check = self.blob_file_path in file_names

        self.assertTrue(check)


    def test_download_file_to_bytes(self):

        file = self.blob_access.download_file_to_bytes(blob_container_name=self.blob_container_name,
                                                       file_path=self.blob_file_path)

        self.assertIsNotNone(file)


    def test_upload_downloadpattern_delete(self):

        # check self.test_file_names NOT in blob container
        file_names = self.blob_access.get_file_path_names(self.blob_container_name)

        for file_name in self.test_file_names:
            
            self.assertTrue(file_name not in file_names)

        # Use base file for upload
        file = b'abc'

        # Upload self.test_file_names to blob container
        for file_name in self.test_file_names:

            self.blob_access.upload_file(blob_container_name=self.blob_container_name,
                                         upload_file_name=file_name, 
                                         data=file)


        # check self.test_file_names in blob container
        file_names = self.blob_access.get_file_path_names(self.blob_container_name)

        for file_name in self.test_file_names:
            
            self.assertTrue(file_name in file_names)

        files = self.blob_access.download_pattern(blob_container_name=self.blob_container_name,
                                             regex_pattern=".*unspsc*.")

        # Test that testdir/wrong_name.tsv not in files
        self.assertTrue(self.test_file_names[1] not in files.keys())

        
        # Test that testdir/OP789_unspsc.tsv is in files
        self.assertTrue(self.test_file_names[0] in files.keys())

        # Delete test files
        for file_name in self.test_file_names:

            self.blob_access.delete_file(blob_container_name=self.blob_container_name,
                                         delete_file_name=file_name)

        # Test that files have been deleted
        file_names = self.blob_access.get_file_path_names(self.blob_container_name)
        
        for file_name in self.test_file_names:

            self.assertTrue(file_name not in file_names)

        
        # Delete test dir
        self.blob_access.delete_directory(blob_container_name=self.blob_container_name,
                                          directory_name=self.test_dir)

        # Test that test dir has been deleted
        file_names = self.blob_access.get_file_path_names(self.blob_container_name)
 
        self.assertTrue(self.test_dir not in file_names)

