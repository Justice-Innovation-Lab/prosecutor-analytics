try:
    import cv2

    HAVE_CV = True
except ImportError:
    HAVE_CV = False

import pandas as pd
import pytest
from azure.core.exceptions import ResourceNotFoundError

from azure_box.azure_wrappers import *

TEST_ACCOUNT_URL = "YOUR_TEST_ACCOUNT_URL"
TEST_CONTAINER = "YOUR_TEST_CONTAINER"

# Skip these tests unless --all is used in the pytest invocation:
pytestmark = pytest.mark.with_credentials

def test_upload_blob_with_metadata():
    upload_az_data(
        "this is test data",
        TEST_ACCOUNT_URL,
        TEST_CONTAINER,
        "test_metadata_upload.txt",
        metadata={"test_key": "test_val"},
        uploading_package="azure_box",
        auto_overwrite=True,
    )

def test_get_data():
    data = get_az_data(TEST_ACCOUNT_URL, TEST_CONTAINER, "test_metadata_upload.txt")

# @pytest.mark.parametrize(
#     "folder_path, dest_folder_name",
#     [
#         (DATA_PATH / "dummy_pdfs", "test_pdf_folder"),
#         (DATA_PATH / "dummy_images", None),
#         (DATA_PATH / "dummy_csvs", "test_csv_folder"),
#         (DATA_PATH / "dummy_other_types", "test_other_types_folder"),
#     ],
# )
# def test_upload_files_from_folder(folder_path, dest_folder_name):
#     upload_files_from_folder(
#         TEST_ACCOUNT_URL, TEST_CONTAINER, folder_path, dest_folder_name=dest_folder_name
#     )




@pytest.mark.parametrize(
    "account_url, container_name, file_name, version_id",
    [
        (TEST_ACCOUNT_URL, "fakecontainer", "test_parq.parquet", None),
    ],
)
def test_download_data_resource_error(
    account_url, container_name, file_name, version_id
):
    with pytest.raises(ResourceNotFoundError):
        data = get_az_data(
            account_url, container_name, file_name, version_id=version_id
        )


@pytest.mark.parametrize(
    "account_url, container_name",
    [(TEST_ACCOUNT_URL, TEST_CONTAINER), (PRIVATE_ACCOUNT_URL, PRIVATE_CONTAINER)],
)
def test_list_container_files(account_url, container_name):
    list_container_files(account_url, container_name)





# def test_download_stream():
#     get_az_data(
#         TEST_ACCOUNT_URL, TEST_CONTAINER, "uploaded_sample.pdf", return_stream=True
#     )


# def test_upload_pdf():
#     data = get_az_data(
#         TEST_ACCOUNT_URL, TEST_CONTAINER, "pdf-test.pdf", return_readable=False
#     )
#     print("type of downloaded pdf is: ", type(data))
#     upload_az_data(
#         data,
#         TEST_ACCOUNT_URL,
#         TEST_CONTAINER,
#         "uploaded_sample.pdf",
#         auto_overwrite=True,
#     )

