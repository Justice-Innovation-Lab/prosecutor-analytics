import io
from logging import Logger

try:
    import geopandas as gpd

    HAVE_GPD = True
except ImportError:
    HAVE_GPD = False

import numpy as np
import pandas as pd
import PyPDF2

try:
    import cv2

    HAVE_CV = True
except ImportError:
    HAVE_CV = False

LOGGER = Logger(__file__)


def parse_data_source(
    file_name,
    stream,
    return_stream=None,
):
    if return_stream:
        data = stream
    elif ".csv" in file_name:
        data = pd.read_csv(io.StringIO(stream.readall().decode("utf-8")))
    elif ".tsv" in file_name:
        data = pd.read_csv(
            io.StringIO(stream.readall().decode("utf-8")), sep="\t"
        )
    elif ".tab" in file_name:
        data = pd.read_csv(
            io.StringIO(stream.readall().decode("utf-8")), sep="\t"
        )
    elif ".dta" in file_name:
        data = pd.read_stata(io.StringIO(stream.readall().decode("utf-8")))
    elif ".xls" in file_name:
        data = pd.read_excel(stream.stream, sheet_name=None, engine="openpyxl")
        LOGGER.info(
            "Excel files are downloaded as dictionaries where each "
            "sheet is a key:value pair."
        )
    elif ".parquet" in file_name:
        data = pd.read_parquet(io.BytesIO(stream.readall()), engine="pyarrow")
    elif ".txt" in file_name:
        data = stream.readall().decode("utf-8")
    elif ".pdf" in file_name:
        # code from: https://stackoverflow.com/questions/59309654/open-an-azure-storagestreamdownloader-without-saving-it-as-a-file
        data = PyPDF2.PdfReader(io.BytesIO(stream.readall()))
        LOGGER.info(f"{file_name} has {len(data.pages)} pages")
    elif ".jpg" in file_name or ".png" in file_name:
        if not HAVE_CV:
            raise EnvironmentError(
                "Image IO currently requires that opencv is installed."
            )

        # use numpy to construct an array from the bytes
        x = np.frombuffer(stream.readall(), dtype="uint8")
        # decode the array into an image
        data = cv2.imdecode(x, cv2.IMREAD_UNCHANGED)
    elif ".zip" in file_name:
        if not HAVE_GPD:
            raise EnvironmentError(
                "Geofile IO currently requires that geopandas is installed."
            )
        try:
            stream.seek(0)
            data = gpd.read_file(stream)
        except TypeError:
            raise TypeError(
                "Can only process .zip files that are geofiles (using geopandas)."
            )
    else:
        raise ValueError(
            f"Cannot process {file_name}. Consider adding support for this filetype"
            " or choosing an alternative format."
        )
    if data is None:
        raise ValueError(f"No data was found in {file_name}.")

    LOGGER.info(f"Successfully parsed {file_name}")
    if isinstance(data, pd.DataFrame):
        LOGGER.info(f"{file_name} parsed as a dataframe has {len(data)} entries.")
    return data
