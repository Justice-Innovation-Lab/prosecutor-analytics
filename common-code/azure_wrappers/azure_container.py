import logging
import os
import sys
from functools import lru_cache
from io import BytesIO, StringIO
from logging import Logger
from pathlib import Path
from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import PyPDF2
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    DefaultAzureCredential,
    EnvironmentCredential,
    ManagedIdentityCredential,
)
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    ContainerClient,
    ContentSettings,
)

from azure_box import data_storage as ds
from azure_box import version_info

LOGGER = Logger(__file__)
DEFAULT_TENANT_ID = "YOUR-TENANT-ID-HERE"
# NOTE: where the code uses an environment variable e.g. tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID); we have
# set that environment variable in our docker files.

# reference: https://stackoverflow.com/questions/43878953/how-does-one-detect-if-one-is-running-within-a-docker-container-within-python
SECRET_KEY = os.environ.get("AM_I_IN_A_DOCKER_CONTAINER", False)


@lru_cache
def get_credential(tenant_id=None):
    logger = logging.getLogger("azure.identity")
    logger.setLevel(logging.ERROR)
    if not tenant_id:
        # may have funny behavior due to default tenant id being
        # defined at import time rather than runtime
        tenant_id = DEFAULT_TENANT_ID
    managed_identity_credential = ManagedIdentityCredential()
    azure_cli_credential = AzureCliCredential()
    env_credential = EnvironmentCredential()
    default_credential = DefaultAzureCredential(
        exclude_visual_studio_code_credential=True,
        exclude_interactive_browser_credential=False,
        interactive_browser_tenant_id=tenant_id,
        exclude_managed_identity_credential=True,
    )
    return ChainedTokenCredential(
        env_credential,
        managed_identity_credential,
        azure_cli_credential,
        default_credential,
    )


def get_browser_creds():
    return InteractiveBrowserCredential()


def get_container_client_from_url(container_url, credential=None):
    if credential is None:
        tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
        credential = get_credential(tenant_id)
    container_client = ContainerClient.from_container_url(
        container_url=container_url, credential=credential
    )
    return container_client


def get_container_client(account_url, container_name, credential=None):
    if credential is None:
        tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
        credential = get_credential(tenant_id)
    try:
        container_client = ContainerClient(
            account_url, container_name, credential=credential
        )
    except:
        print("The specified container does not exist.")
        sys.exit(1)
    return container_client


def get_blob_service_client(account_url, credential=None):
    if credential is None:
        tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
        credential = get_credential(tenant_id)
    blob_service_client = BlobServiceClient(account_url, credential)
    containers = blob_service_client.list_containers()
    print(
        "Successful connection using Azure AD. Available containers are: ",
        [c.name for c in containers],
    )
    return blob_service_client


def list_container_files(
    account_url, container_name, print_names=False, name_starts_with=None
):
    """
    List the files available in a given blob container.
    Updated to use container client directly rather than from SA client.
    """
    tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
    credential = get_credential(tenant_id)
    container_client = ContainerClient(
        account_url, container_name, credential=credential
    )
    blob_list = container_client.list_blobs(name_starts_with=name_starts_with)
    if print_names:
        print("The following files are available in", container_name)
        for blob in blob_list:
            print("\t" + blob.name)
    return [blob.name for blob in blob_list]

def get_az_data(
    account_url,
    container_name,
    file_name,
    return_readable=True,
    return_stream=False,
    version_id=None,
):
    """
    Download a csv or excel file from Azure blob storage
    Uses container client directly
    """
    # conn = BlobServiceClient(account_url, credential=default_credential)
    try:
        tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
        container_client = ContainerClient(
            account_url, container_name, credential=get_credential(tenant_id)
        )
    except ResourceNotFoundError as e:
        LOGGER.info(e)
        raise ResourceNotFoundError(
            "Could not connect to azure for the container: {container_name}"
        )

    if not return_readable and ".pdf" in file_name:
        data = ds.SecureCachedStream(container_client, file_name, version_id).readall()
        return data

    data_source = ds.DataSource(
        container_client,
        file_name,
        return_stream=return_stream,
        version_id=version_id,
    )
    data = data_source.data
    # drop Unnamed: 0 columns from dataframe before returning it
    if isinstance(data, pd.DataFrame):
        data.drop(data.filter(regex="Unnamed"), axis=1, inplace=True)
    return data


def update_blob_metadata(account_url, container_name, blob_name, metadata_dict):
    """
    Update blob metadata for an exsting blob. Note that
    this creates a new version of the blob and overwrites the
    existing metadata.
    """
    tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
    blob_client = BlobClient(
        account_url, container_name, blob_name, credential=get_credential(tenant_id)
    )
    blob_client.set_blob_metadata(metadata=metadata_dict)


def upload_to_az(
    data,
    account_url,
    container_name,
    file_name,
    auto_overwrite=False,
    debug=False,
    uploading_package=None,
    metadata=None,
):
    """
    Upload a dataframe as a csv to Azure blob storage
    """
    if not metadata:
        metadata = {}
    if uploading_package is not None:
        version_metadata = version_info.get_version_info(__import__(uploading_package))
        metadata.update(version_metadata)
    tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
    container_client = get_container_client(
        account_url, container_name, credential=get_credential(tenant_id)
    )
    # check if file already exists and ask if file should be overwritten
    if debug:
        print("filename is ", file_name)
    try:
        blob_list = [blob.name for blob in container_client.list_blobs()]
    except ResourceNotFoundError as e:
        print("The container does not exist: {}".format(container_name))
        return None
    if (file_name in blob_list) and (not auto_overwrite):
        overwrite_allow = input(
            "The file already exists, would you like to overwrite? y/n\n"
        )
    else:
        overwrite_allow = True
    assert overwrite_allow, "Pick a new file name and retry"
    if "parquet" in file_name:
        upload_parquet(data, account_url, container_name, file_name, metadata=metadata)
        print("File successfully uploaded to", container_name, "as", file_name)
    elif "csv" in file_name:
        # Upload file as a csv to blob
        container_client.upload_blob(
            name=file_name,
            data=data.to_csv(),
            overwrite=True,
            timeout=14400,
            metadata=metadata,
        )
        print("File successfully uploaded to", container_name, "as", file_name)
    elif "xlsx" in file_name or ".txt" in file_name:
        container_client.upload_blob(
            name=file_name, data=data, overwrite=True, timeout=14400, metadata=metadata
        )
        print("File successfully uploaded to", container_name, "as", file_name)
    elif ".pdf" in file_name:
        if debug:
            print("uploading a pdf")
        container_client.upload_blob(
            name=file_name,
            data=data,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/pdf"),
            timeout=14400,
            metadata=metadata,
        )
        print("File successfully uploaded to", container_name, "as", file_name)

    else:
        print(
            "Failed to upload. Please include extension in file path. \
              The function currently supports csv and parquet."
        )




def upload_parquet(df, account_url, container_name, file_name, metadata=None):
    """
    Convert pandas dataframe to parquet and upload to Azure.
    """
    table = pa.Table.from_pandas(df)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
    container_client = get_container_client(
        account_url, container_name, credential=get_credential(tenant_id)
    )

    container_client.upload_blob(
        name=file_name,
        data=buf.getvalue().to_pybytes(),
        overwrite=True,
        timeout=14400,
        metadata=metadata,
    )


def upload_files_from_folder(
    account_url, container_name, source_folder_path, dest_folder_name=None
):
    all_file_names = os.listdir(source_folder_path)
    for file_name in all_file_names:
        upload_file_from_path(
            account_url,
            container_name,
            source_folder_path,
            file_name,
            dest_folder_name=dest_folder_name,
        )


def upload_file_from_path(
    account_url,
    container_name,
    source_folder_path,
    file_name,
    dest_folder_name=None,
    metadata=None,
):
    """
    Uploads a file to a container from a given filepath.
    Currently only supports PDFs.
    """
    # code inspired by: https://www.quickprogrammingtips.com/azure/how-to-upload-files-to-azure-storage-blobs-using-python.html
    # Content_types reference: http://www.iana.org/assignments/media-types/media-types.xhtml
    if metadata is None:
        metadata = {}
    if ".pdf" in file_name:
        content_type = "application/pdf"
    elif ".jpg" in file_name:
        content_type = "image/jpeg"
    elif ".png" in file_name:
        content_type = "image/png"
    elif ".csv" in file_name:
        content_type = "application/CSV"
    elif ".xlsx" in file_name:
        content_type = "application/XLSX"
    elif ".tsv" in file_name:
        content_type = "text/tab-separated-values"
    elif ".txt" in file_name:
        content_type = "text/plain"
    elif ".dta" in file_name:
        content_type = "application/x-stata-dta"
    # maybe move above to dict if needed
    else:
        print(
            "Function only supports csv, pdf, jpeg, and png at this time and could not upload ",
            file_name,
        )
        return None
    dest_name = file_name
    if dest_folder_name:
        dest_name = dest_folder_name + "/" + file_name
    tenant_id = os.environ.get("TENANT_ID", DEFAULT_TENANT_ID)
    container_client = ContainerClient(
        account_url, container_name, credential=get_credential(tenant_id)
    )
    upload_file_path = os.path.join(source_folder_path, file_name)
    print(f"uploading file - {file_name}")
    with open(upload_file_path, "rb") as data:
        container_client.upload_blob(
            name=dest_name,
            data=data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
            timeout=14400,
            metadata=metadata,
        )


