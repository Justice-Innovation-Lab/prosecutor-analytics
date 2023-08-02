try:
    import cv2

    HAVE_CV = True
except ImportError:
    HAVE_CV = False

import pandas as pd
import numpy as np
import pytest
from azure.core.exceptions import ResourceNotFoundError

from ..azure_container import *

DEFAULT_ACCOUNT_URL = "YOUR_ACCOUNT_URL"
TEST_ACCOUNT_URL = os.environ.get('ACCOUNT_URL', DEFAULT_ACCOUNT_URL)
DEFAULT_TEST_CONTAINER = "YOUR_TEST_CONTAINER"
TEST_CONTAINER = os.environ.get('CONTAINER_NAME', DEFAULT_TEST_CONTAINER)

def test_upload_blob_with_metadata():
    upload_to_az(
        pd.DataFrame(
    {
        "A": ["foo", "bar", "foo", "bar", "foo", "bar", "foo", "foo"],
        "B": ["one", "one", "two", "three", "two", "two", "one", "three"],
        "C": np.random.randn(8),
        "D": np.random.randn(8),
    }
        ),
        TEST_ACCOUNT_URL,
        TEST_CONTAINER,
        "test_parq.parquet",
        metadata={"test_key": "test_val"},
        uploading_package="azure_wrappers",
        auto_overwrite=True,
    )

def test_get_data():
    data = get_az_data(TEST_ACCOUNT_URL, TEST_CONTAINER, "test_metadata_upload.txt")

def test_upload_files_from_folder():
    upload_files_from_folder(
        TEST_ACCOUNT_URL, TEST_CONTAINER, 'data/', dest_folder_name="test_uploads"
    )

@pytest.mark.parametrize(
"account_url, container_name, file_name",
[
    pytest.param(
        TEST_ACCOUNT_URL,
        TEST_CONTAINER,
        "test_other_types_folder/auto.dta",
        marks=pytest.mark.xfail(reason="utf-8 codec cannot decode auto.dta"),
    ),
    (TEST_ACCOUNT_URL, TEST_CONTAINER, "test_uploads/dummy_data.csv"),
    (TEST_ACCOUNT_URL, TEST_CONTAINER, "test_uploads/sample_image.jpg"),
    (TEST_ACCOUNT_URL, TEST_CONTAINER, "test_uploads/sample_img.png"),
    (TEST_ACCOUNT_URL, TEST_CONTAINER, "test_uploads/sample.pdf"),
    (TEST_ACCOUNT_URL, TEST_CONTAINER, "test_uploads/test_text.txt"),
    (TEST_ACCOUNT_URL, TEST_CONTAINER, "test_uploads/test.tsv"),
    (TEST_ACCOUNT_URL, TEST_CONTAINER, "test_parq.parquet"),
],
)
def test_download_data(account_url, container_name, file_name):
    try:
        data = get_az_data(
            account_url,
            container_name,
            file_name,
        )
        if isinstance(data, pd.DataFrame):
            assert not any([col for col in data.columns if col.startswith("Unnamed:")])
            print(file_name)
            print(data.head())
    except ResourceNotFoundError:
        pytest.mark.xfail(reason=f"{file_name} not found")

def test_download_data_resource_error():
    with pytest.raises(ResourceNotFoundError):
        data = get_az_data(
            TEST_ACCOUNT_URL, "fakecontainer", "test_parq.parquet"
        )

def test_list_container_files():
    list_container_files(TEST_ACCOUNT_URL, TEST_CONTAINER)





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

