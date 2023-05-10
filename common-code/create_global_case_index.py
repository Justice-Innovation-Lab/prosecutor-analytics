import pandas as pd

def get_global_case_index(
    df,
    day_window,
    arrest_date_col_name,
    disp_date_col_name,
    defendant_id_col_name,
):
    """
    :Info: Groups together cases with disposition dates within a specified range of days.\
    It will "chain" together cases until subsequent cases occur more than 'day_window' 
    number of days apart. Finally, it groups together cases that occur within and including 
    day_window number of days (e.g., if day_window is 5, cases with disposition dates 5 
    days apart are grouped together).
    :param df: DataFrame
    :param day_window: int
    :param arrest_date_col_name: str
    :param disp_date_col_name: str
    :param defendant_id_col_name: str
    :returns: dataframe
    """
    case_index_df = df.copy()
    case_index_df[arrest_date_col_name] = pd.to_datetime(
        case_index_df[arrest_date_col_name]
    )
    case_index_df[disp_date_col_name] = pd.to_datetime(
        case_index_df[disp_date_col_name]
    )
    case_index_df["max_disposition_date"] = case_index_df.groupby(
        [defendant_id_col_name, arrest_date_col_name]
    )[disp_date_col_name].transform("max")
    case_index_df = case_index_df.sort_values(
        by=[defendant_id_col_name, "max_disposition_date"]
    )
    case_index_df["time_diff"] = (
        case_index_df.groupby(defendant_id_col_name)["max_disposition_date"]
        .diff()
        .fillna(pd.Timedelta("0D"))
    )
    change_to_zero = case_index_df["time_diff"] <= day_window
    case_index_df["time_diff_masked"] = (
        case_index_df["time_diff"].mask(change_to_zero, pd.Timedelta("0D")).dt.days
    )
    case_index_df["cumulative_time_diff"] = case_index_df.groupby(
        defendant_id_col_name
    )["time_diff_masked"].transform("cumsum")
    case_index_df["time_group"] = case_index_df.groupby(defendant_id_col_name)[
        "cumulative_time_diff"
    ].transform(lambda x: pd.factorize(x)[0] + 1)
    case_index_df["global_case_index"] = (
        case_index_df[defendant_id_col_name].astype(str)
        + "-"
        + case_index_df["time_group"].astype(str)
    )

    return case_index_df.drop(
        columns=[
            "time_diff",
            "time_diff_masked",
            "cumulative_time_diff",
            "time_group",
        ]
    )
