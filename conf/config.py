"""This module contains configurations for the application.

All relevant configuration are set here.
They are placed in a dictionary with keys named after purpose.
"""
# data_lake_endpoint
    # To find Data Lake Storage Endpoint in portal.azure.com
    #     Go to: subscription -> storage account -> endpoints
    #     See screenshot in path: docs\img\storage_oauth_url.png

# tenant_id
    # Dima Tenant ID

storage_configurations_dict = {"env_is_prod": {"tenant_id": "<<Azure Tenant ID>>" ,
                                                "data_lake_endpoint": "<<Data Lake Endpoint>>"},
                               "env_is_dev":  {"tenant_id": "<<Azure Tenant ID>>" ,
                                                "data_lake_endpoint": "<<Data Lake Endpoint>>"}}

