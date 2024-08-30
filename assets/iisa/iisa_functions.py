import gzip
import json
import logging
import socket
from typing import Optional
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
from numpy.linalg import pinv
from pandera.typing import DataFrame
from requests.exceptions import (
    HTTPError,
    ConnectionError as ReqConnectionError,
)
from scipy.stats import t
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .iata import IataInfoSchema, get_iata_geolocation_info

# Combine exceptions from different modules into a tuple
ExceptionsToRetry = (ConnectionError, ReqConnectionError, HTTPError, socket.timeout)

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def adjust_rows(initial_query_results_pandas, target_rows):
    """
    Dynamically adjust the number of rows per group to approximate a target total number of rows.

    This function iteratively adjusts the upper limit of rows for each group (defined by 'deployment_hash'
    and 'indexer') in the DataFrame to ensure that the sum of restricted rows is close to the specified
    target number of rows. It decreases or increases the upper limit based on the difference between the
    current sum and the target, and stops when the sum is within a specified tolerance or a maximum number
    of iterations is reached.

    Parameters:
    initial_query_results_pandas (DataFrame): DataFrame containing the initial query results with a 'num_rows' column.
    target_rows (int): The target total number of rows for the DataFrame.

    Returns:
    int: The adjusted upper limit for the number of rows per group.
    """
    if target_rows < 0:
        raise ValueError("Target rows must be a non-negative integer")

    x = 1_000  # Starting estimate for the number of rows to record for each ['deployment_hash', 'indexer'] combination.
    initial_query_results_pandas["num_rows_restricted"] = initial_query_results_pandas[
        "num_rows"
    ].clip(upper=x)
    tolerance = target_rows * 0.01  # 1% tolerance range
    max_iterations = 1_000  # Maximum number of iterations to avoid infinite loops
    iteration = 0

    while not (
        target_rows - tolerance
        <= initial_query_results_pandas["num_rows_restricted"].sum()
        <= target_rows + tolerance
    ):
        current_sum = initial_query_results_pandas["num_rows_restricted"].sum()
        if current_sum > target_rows:
            x = int(x * 0.99)  # Decrease x by 1%
        elif current_sum < target_rows:
            x = int(x * 1.01)

        initial_query_results_pandas["num_rows_restricted"] = (
            initial_query_results_pandas["num_rows"].clip(upper=x)
        )
        iteration += 1

        # Break the loop if the difference between the current sum and the target is within the
        # tolerance range or if the maximum number of iterations is reached.
        if abs(current_sum - target_rows) <= tolerance or iteration >= max_iterations:
            break

    return initial_query_results_pandas["num_rows_restricted"].max()


def apply_location_details(unique_urls_indexers_pandas):
    """
    Apply the extract_location_and_details function to each URL and expand the results into separate columns.

    This function applies the extract_location_and_details function to each URL in the input DataFrame,
    adding columns for location, organization, geographical coordinates, and IP address.

    Parameters:
    unique_urls_indexers_pandas (DataFrame): DataFrame containing the unique URLs and indexers.

    Returns:
    pandas.DataFrame: The input DataFrame with additional columns:
                      - 'location': String describing the location (country, region, city)
                      - 'org': Organization associated with the IP
                      - 'loc': Geographical coordinates
                      - 'ip': IP address of the URL
    """
    # So long as the DataFrame is not empty, apply the extract_location_and_details function
    if not unique_urls_indexers_pandas.empty:
        unique_urls_indexers_pandas[["location", "org", "loc", "ip"]] = (
            unique_urls_indexers_pandas["url"].apply(extract_location_and_details)
        )

        # Return the transformed df
        return unique_urls_indexers_pandas

    # Otherwise, simply create headers for ["location", "org", "loc", "ip"]
    else:
        for column in ["location", "org", "loc", "ip"]:
            unique_urls_indexers_pandas[column] = pd.Series(dtype="str")

        # Return the transformed df
        return unique_urls_indexers_pandas


def extract_location_and_details(url):
    """
    Extract location and other details from a given URL by resolving it to an IP address.

    This function first resolves the URL to an IP address, then fetches geographical and
    organizational details for that IP address.

    Parameters:
    url (str): The URL to be resolved and analyzed.

    Returns:
    pandas.Series: A Series containing the following information:
                   - 'location': String describing the location (country, region, city)
                   - 'org': Organization associated with the IP
                   - 'loc': Geographical coordinates
                   - 'ip': IP address of the URL

    Note:
    This function relies on external API calls to resolve the IP and fetch location data.
    It may return default "Unknown" values if the URL cannot be resolved or if the IP details cannot be fetched.
    """
    ip = url_to_ip(url)
    return pd.Series(get_location_and_details_from_ip(ip))


def url_to_ip(url):
    """
    This function attempts to extract the hostname from the given URL and resolve it to an IP address.
    It handles various edge cases such as invalid URLs or network issues.

    Parameters:
    url (str): The URL to be resolved to an IP address.

    Returns:
    str or None: The IP address as a string if resolution is successful, None otherwise.
    """
    # First handle missing or nan URL's
    if pd.isna(url) or not isinstance(url, str):
        return None

    try:
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname

        # If the hostname is not present, return nothing.
        if hostname is None:
            return None

        return socket.gethostbyname(hostname)

    # If there's a gaierror (getaddrinfo error) return nothing.
    # e.g. Non-Existent Domain, DNS Issue, Network Problem, Invalid Hostname Format...
    except socket.gaierror:
        return None


@retry(
    retry=retry_if_exception_type(ExceptionsToRetry),
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, max=60),
    reraise=True,  # (Default) After set number of attempts the decorator will re-raise the issue further up.
)
def get_location_and_details_from_ip(ip):
    """
    Fetch location and organizational details for a given IP address using an external API (ipinfo.io).

    This function makes an HTTP request to the ipinfo.io API to retrieve geographical and
    organizational information associated with the provided IP address. It includes a retry
    mechanism to handle potential network issues or API failures.

    Parameters:
    ip (str): The IP address to query.

    Returns:
    dict: A dictionary containing the following keys:
        - 'location': String combining country, region, and city (e.g., "US, California, San Francisco")
        - 'org': Organization associated with the IP
        - 'loc': Geographical coordinates
        - 'ip': The queried IP address
    """
    if ip is None:
        return {
            "location": "Unknown",
            "org": "Unknown",
            "loc": "Unknown",
            "ip": "Unknown",
        }
    try:
        response = requests.get(
            f"https://ipinfo.io/{ip}/json?token=67647c2e5ccd95", timeout=10
        )
        response.raise_for_status()  # Raise a HTTPError in case of bad response.

        # Try to decode the content manually
        try:
            data = response.json()

        except requests.exceptions.JSONDecodeError:
            # If JSON decoding fails, try to decompress manually
            decompressed_content = gzip.decompress(response.content)
            data = json.loads(decompressed_content)

        return {
            "location": f'{data.get("country", "Unknown")}, {data.get("region", "Unknown")}, {data.get("city", "Unknown")}',
            "org": data.get("org", "Unknown"),
            "loc": data.get("loc", "Unknown"),
            "ip": data.get("ip", "Unknown")
            if data.get("ip") is None
            else data.get("ip"),
        }

    # If there's been a connection error then we can raise the issue to the retry decerator and retry the connection
    except ExceptionsToRetry as e:
        logger.debug(f"Failed to retrieve IP details: {e}")
        raise  # Raise to trigger retry decorator

    except Exception as e:
        logger.error(f"Unexpected error when retrieving IP details: {e}")
        return {
            "location": "Unknown",
            "org": "Unknown",
            "loc": "Unknown",
            "ip": "Unknown",
        }


def merge_dataframes(combined_query_pandas, unique_urls_indexers_pandas):
    """
    Merge two DataFrames containing combined query results and unique URL-indexer information.

    This function performs a left merge operation, combining data from the combined query results
    with the unique URL and indexer information. The merge is based on the 'indexer', 'day_partition',
    and 'url' columns.

    Parameters:
    combined_query_pandas (pandas.DataFrame): DataFrame containing the combined query results.
    unique_urls_indexers_pandas (pandas.DataFrame): DataFrame containing unique URLs and indexers information.

    Returns:
    pandas.DataFrame: A new DataFrame resulting from the left merge of the input DataFrames.
    """
    return pd.merge(
        left=combined_query_pandas,
        right=unique_urls_indexers_pandas,
        how="left",
        # Meaning that all rows from the left df will be in the merged df. Columns are merged together as expected.
        on=["indexer", "day_partition", "url"],
    )


def extract_iata_codes(df):
    """
    Extract IATA (International Air Transport Association) codes from query IDs and count their occurrences.

    This function assumes that the last three characters of each query ID represent an IATA code.
    It extracts these codes and counts how many times each unique code appears in the dataset.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing a 'query_id' column.

    Returns:
    pandas.DataFrame: A new DataFrame with columns:
        - 'IATA_code': The extracted 3-letter IATA code
        - 'count': The number of occurrences of each IATA code
    """
    if df.empty or "query_id" not in df.columns:
        return pd.DataFrame(columns=["IATA_code", "count"])

    df = df.dropna(subset=["query_id"])
    df["query_id"] = df["query_id"].astype(str)

    iata_df = (
        df.groupby(df["query_id"].str[-3:])
        .agg(count=("query_id", "size"))
        .reset_index()
        .rename(columns={"query_id": "IATA_code"})
    )
    return iata_df


def extract_iata_code(df):  # Not the same function as extract_iata_codes!
    """
    Extract the IATA code from the 'query_id' column of a DataFrame.

    This function creates a new 'IATA_code' column in the input DataFrame by extracting
    the last three characters from the 'query_id' column, assuming these represent the IATA code.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing a 'query_id' column.

    Returns:
    pandas.DataFrame: The input DataFrame with an additional 'IATA_code' column.
    """
    # If the DataFrame does not contain the 'query_id' column, set the query_id and IATA_code columns to None
    # and return the DataFrame
    if "query_id" not in df.columns:
        logger.error("DataFrame must include a 'query_id' column.")

        df["query_id"] = None
        df["IATA_code"] = None
        return df

    df["IATA_code"] = df["query_id"].str[-3:]
    return df


def merge_in_iata_geolocation_info(
    combined_query_pandas, iata_info: Optional[DataFrame[IataInfoSchema]] = None
):
    """
    Merge IATA information into combined query data.

    This function merges two DataFrames: one containing IATA code information and another
    containing combined query results. The merge is performed on the 'IATA_code' column.

    Parameters:
    combined_query_pandas (pandas.DataFrame): DataFrame containing combined query results.
    iata_df (DataFrame[IataInfoSchema], optional): DataFrame containing IATA code information.

    Returns:
    pandas.DataFrame: A new DataFrame resulting from the right merge of the input DataFrames.
    """
    if iata_info is None:
        iata_info = get_iata_geolocation_info()

    return pd.merge(
        combined_query_pandas,
        iata_info,
        on="IATA_code",
        how="left",
    )


def process_combined_query_pandas(df):
    """
    Process and transform the combined query DataFrame to prepare it for further analysis.

    This function performs several operations on the input DataFrame:
    1. Adds an 'indexer_count' column
    2. Renames certain columns
    3. Creates an 'origin_loc' column from latitude and longitude
    4. Drops unnecessary columns
    5. Removes rows with NaN or invalid location data

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing combined query results.

    Returns:
    pandas.DataFrame: Processed DataFrame with new and modified columns.

    Note:
    - The function expects columns: 'indexer', 'loc', 'country', 'latitude', 'longitude'
    - New columns created: 'indexer_count', 'destination_loc', 'origin_country', 'origin_loc'
    - Rows with NaN values or 'nan,nan' in 'origin_loc' or 'destination_loc' are dropped
    - If the input DataFrame is empty, returns an empty DataFrame with expected columns
    """
    if df.empty:
        # Return an empty DataFrame with the expected columns
        return pd.DataFrame(
            columns=[
                "indexer",
                "indexer_count",
                "destination_loc",
                "origin_country",
                "origin_loc",
            ]
        )

    # Add an indexer count
    df["indexer_count"] = df.groupby("indexer")["indexer"].transform("count")

    # Rename columns
    df.rename(
        columns={"loc": "destination_loc", "country": "origin_country"}, inplace=True
    )

    # Create 'origin_loc' column
    df["origin_loc"] = (
        df[["latitude", "longitude"]].astype(str).agg(",".join, axis=1)
    )  # vectorized for speed

    # Drop 'latitude' and 'longitude' columns
    df.drop(columns=["latitude", "longitude"], inplace=True)

    # Drop all NaNs and string NaNs
    df.dropna(subset=["origin_loc", "destination_loc"], inplace=True)
    df = df[~df["origin_loc"].str.contains("nan,nan", na=False)]
    df = df[~df["destination_loc"].str.contains("nan,nan", na=False)]

    return df


def split_locations(df):
    """
    Split 'origin_loc' and 'destination_loc' columns into separate latitude and longitude columns.

    This function takes a DataFrame with 'origin_loc' and 'destination_loc' columns containing
    comma-separated coordinate pairs, and splits them into four new columns: 'origin_lat',
    'origin_lon', 'dest_lat', and 'dest_lon'.The function handles non-numeric entries by converting
    them to NaN.

    Parameters:
    df (DataFrame): The DataFrame containing 'origin_loc' and 'destination_loc' columns.

    Returns:
    pandas.DataFrame: The input DataFrame with four new columns added:
        - 'origin_lat': Latitude of the origin location
        - 'origin_lon': Longitude of the origin location
        - 'dest_lat': Latitude of the destination location
        - 'dest_lon': Longitude of the destination location

    Note:
        - NaN values in input columns are implicitly handled and result in NaN values in output columns.
        - Non-numeric entries or invalid coordinate pairs will also result in NaN values.
    """
    # Handle potential empty input df.
    if df.empty:
        return df.assign(origin_lat=[], origin_lon=[], dest_lat=[], dest_lon=[])

    for prefix, col in [("origin", "origin_loc"), ("dest", "destination_loc")]:
        # Split the strings
        split = df[col].str.split(",", expand=True)

        # Convert to float, invalid values become NaN
        df[f"{prefix}_lat"] = pd.to_numeric(split[0], errors="coerce")
        df[f"{prefix}_lon"] = pd.to_numeric(split[1], errors="coerce")

    return df


def calculate_distances(df):
    """
    Calculate the spherical distances between origin and destination coordinates.

    This function applies the Haversine formula to compute the distance between each pair
    of origin and destination coordinates in the input DataFrame.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing columns:
        - 'origin_lon': Longitude of the origin location
        - 'origin_lat': Latitude of the origin location
        - 'dest_lon': Longitude of the destination location
        - 'dest_lat': Latitude of the destination location

    Returns:
    pandas.DataFrame: The input DataFrame with an additional 'distance_miles' column
                      containing the calculated distances in miles.
    """
    df["distance_miles"] = haversine_vectorized(
        df["origin_lon"], df["origin_lat"], df["dest_lon"], df["dest_lat"]
    )
    return df


def haversine_vectorized(lon1, lat1, lon2, lat2):
    """
    Calculate the spherical distances between two sets of coordinates using the Haversine formula.

    This function computes distances between multiple pairs of points on Earth's surface,
    treating the Earth as a sphere. It uses a vectorized implementation for efficiency.

    Parameters:
    lon1 (array-like): Longitudes of the first set of points
    lat1 (array-like): Latitudes of the first set of points
    lon2 (array-like): Longitudes of the second set of points
    lat2 (array-like): Latitudes of the second set of points

    Returns:
    numpy.ndarray: An array of distances in miles between each pair of points
    """
    lon1, lat1, lon2, lat2 = np.radians([lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 3956  # Radius of earth in miles
    return c * r


def drop_intermediate_columns(df):
    """
    Remove specified intermediate columns from the DataFrame to conserve memory.

    This function drops the columns used for intermediate calculations, specifically
    the latitude and longitude columns for both origin and destination.

    Parameters:
    df (pandas.DataFrame): Input DataFrame potentially containing intermediate columns.

    Returns:
    pandas.DataFrame: The input DataFrame with specified intermediate columns removed.

    Note:
    - Columns to be dropped: 'origin_lat', 'origin_lon', 'dest_lat', 'dest_lon'
    - If any of these columns are not present in the DataFrame, they are simply ignored.
    """
    columns_to_drop = ["origin_lat", "origin_lon", "dest_lat", "dest_lon"]
    existing_columns = [col for col in columns_to_drop if col in df.columns]

    if existing_columns:
        df.drop(columns=existing_columns, inplace=True)

    return df


def filter_status(df):
    """
    Filter the DataFrame to include only rows where the status is '200 OK'.

    This function creates a new DataFrame containing only the rows from the input
    DataFrame where the 'status' column has the value '200 OK'.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing a 'status' column.

    Returns:
    pandas.DataFrame: A new DataFrame with only the rows where status is '200 OK'.
    """
    return df[df["status"] == "200 OK"].copy()


def apply_round_distance(df):
    """
    Apply the round_distance function to the 'distance_miles' column of the DataFrame.

    This function rounds the values in the 'distance_miles' column to the nearest x miles
    using the round_distance function. Where x is set inside round_distance.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing a 'distance_miles' column.

    Returns:
    pandas.DataFrame: The input DataFrame with the 'distance_miles' column rounded.
    """
    if "distance_miles" in df.columns:
        df["distance_miles"] = df["distance_miles"].apply(
            lambda x: round_distance(x) if pd.notnull(x) else x
        )
    return df


def round_distance(value):
    """
    Round a distance value to the nearest multiple of 250 miles.

    This function takes a numeric distance value and rounds it to the nearest
    multiple of 250. It's used for simplifying distance measurements.

    Parameters:
    value (float): The distance value to be rounded, in miles.

    Returns:
    float: The input value rounded to the nearest multiple of 250.
    """
    return round(value / 250) * 250


def filter_columns(df, all_columns):
    """
    Filter the DataFrame to include only specified columns.

    This function creates a new DataFrame that includes only the columns
    specified in the all_columns list.

    Parameters:
    df (pandas.DataFrame): The input DataFrame to be filtered.
    all_columns (list): A list of column names to retain in the output DataFrame.

    Returns:
    pandas.DataFrame: A new DataFrame containing only the specified columns.

    """
    return df[all_columns]


def iterative_filter(
    df,
    min_deployment_indexers,
    min_deployments_per_indexer,
    min_queries_per_indexer,
    min_queries_per_deployment,
):
    """
    Iteratively filter a DataFrame based on multiple criteria related to deployments, indexers, and queries.

    This function applies a series of filters to the input DataFrame, removing rows that don't meet
    the specified criteria. It continues to apply these filters iteratively until no further changes occur.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing columns: 'deployment_hash', 'indexer', 'query_id'.
    min_deployment_indexers (int): Minimum number of indexers required for each deployment.
    min_deployments_per_indexer (int): Minimum number of deployments required for each indexer.
    min_queries_per_indexer (int): Minimum number of queries required for each indexer.
    min_queries_per_deployment (int): Minimum number of queries required for each deployment.

    Returns:
    pandas.DataFrame: Filtered DataFrame meeting all specified criteria.

    Note:
    - The filtering process is iterative and continues until the DataFrame size stabilizes.
    - If the filtering results in an empty DataFrame, an empty DataFrame is returned.
    """
    while True:
        initial_len = len(df)

        # Ensure deployments have at least `min_deployment_indexers` indexers
        indexer_per_deployment = df.groupby("deployment_hash")["indexer"].nunique()
        df = df[
            df["deployment_hash"].map(indexer_per_deployment) >= min_deployment_indexers
        ]

        # Ensure indexers serve at least `min_deployments_per_indexer` deployments
        deployment_per_indexer = df.groupby("indexer")["deployment_hash"].nunique()
        df = df[
            df["indexer"].map(deployment_per_indexer) >= min_deployments_per_indexer
        ]

        # Ensure indexers serve at least `min_queries_per_indexer` unique queries
        queries_per_indexer = df.groupby("indexer")["query_id"].nunique()
        df = df[df["indexer"].map(queries_per_indexer) >= min_queries_per_indexer]

        # Ensure deployments have at least `min_queries_per_deployment` queries
        query_counts_per_deployment = df.groupby("deployment_hash").size()
        df = df[
            df["deployment_hash"].map(query_counts_per_deployment)
            >= min_queries_per_deployment
        ]

        # Check if no change in DataFrame size, else run the loop again.
        if len(df) == initial_len:
            break

    return df


def strategic_sample(df, target_rows_per_subgraph):
    """
    Sample query_id's in a way that creates balanced representation across indexers on each subgraph.
    The function adds a new column ('sampled_query_id') with some values set to None.

    Parameters:
    df (DataFrame): The DataFrame to sample.
    target_rows_per_subgraph (int): The number of rows (queries) to target for each deployment_hash.

    Returns:
    tuple: A tuple containing two elements:
        - pandas.DataFrame: The input DataFrame with an additional 'sampled_query_id' column.
          This column contains the sampled query IDs where applicable, and None for non-sampled rows.
        - int: The square root of the number of sampled query_ids, intended to inform the number of buckets for
               subsequent hashing operations.

    Note:
    - The function does not reduce the size of the input DataFrame. It only marks sampled rows.
    - The actual number of sampled rows can (will) be greater than target_rows_per_subgraph, as sampling is done
      separately for each (deployment_hash, indexer) combination.
    - Each deployment_hash is sampled for (target_rows_per_subgraph // number_of_indexers) rows.
    - The function aims for balance: it tries to sample an equal number of rows for each
      indexer within a deployment_hash, subject to the calculated or provided cap for each deployment_hash.
    """
    if df.empty:
        df["sampled_query_id"] = pd.Series(dtype="float64")
        return df, 0

    # Calculate number of unique indexers per subgraph.
    # Then calculate how many queries to sample for each indexer, subgraph combination.
    # In the lambda function, x represents the number of unique indexers for a particular deployment_hash.
    indexers_per_subgraph = df.groupby("deployment_hash")["indexer"].nunique()
    cap_per_indexer = indexers_per_subgraph.map(
        lambda x: target_rows_per_subgraph // x if x else 0
    ).to_dict()

    # Create a DataFrame that contains the info above
    query_counts = (
        df.groupby(["deployment_hash", "indexer"])["query_id"]
        .agg(lambda x: list(x.unique()))
        .reset_index(name="unique_query_ids")
    )
    query_counts["cap"] = query_counts["deployment_hash"].map(cap_per_indexer)

    # Then sample the query_id's associated with each indexer, subgraph combination
    def sample_queries(query_ids, cap):
        query_ids = (
            list(np.concatenate(query_ids))
            if isinstance(query_ids[0], list)
            else query_ids
        )
        return np.random.choice(query_ids, size=min(len(query_ids), cap), replace=False)

    # Apply sampling function
    query_counts["sampled_query_id_list"] = query_counts.apply(
        lambda x: sample_queries(x["unique_query_ids"], x["cap"]), axis=1
    )

    # Filter the df with the sampled id's
    # x represents each individual query ID from the df["query_id"] Series
    sampled_ids = set(np.concatenate(query_counts["sampled_query_id_list"].values))
    df["sampled_query_id"] = df["query_id"].apply(
        lambda x: x if x in sampled_ids else None
    )

    # Take the square root of the number of sampled id's to inform the number of buckets to hash mod the query into.
    integer_root = int(np.sqrt(len(sampled_ids)))

    return df, integer_root


def hash_sampled_queries(df, integer_root):
    """
    Hash the sampled query IDs to create a new column with hashed values.

    This function takes a DataFrame with a 'sampled_query_id' column and creates a new column
    'sampled_query_id_hashed_mod_integer_root' containing the hash of each sampled query ID
    modulo the provided integer root.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing a 'sampled_query_id' column.
    integer_root (int): The modulus to apply to the hash values.

    Returns:
    pandas.DataFrame: A copy of the input DataFrame with an additional column
                      'sampled_query_id_hashed_mod_integer_root' containing the hashed values.
    """
    # Create a copy of the input DataFrame
    result_df = df.copy()

    result_df.loc[
        result_df["sampled_query_id"].notna(),
        "sampled_query_id_hashed_mod_integer_root",
    ] = result_df["sampled_query_id"].apply(lambda x: hash(x) % integer_root)

    return result_df


def perform_linear_regression(df, predictor, categorical, numeric):
    """
    Perform linear regression analysis on the given data.

    This function orchestrates the entire linear regression process, including data preprocessing,
    model fitting, prediction, and result analysis. It also calculates robust normalized coefficients
    for indexer rankings.

    Parameters:
    df (pandas.DataFrame): The data to perform regression on.
    predictor (list): List of column names to be used as the dependent variable(s).
    categorical (list): List of column names containing categorical features.
    numeric (list): List of column names containing numeric features.

    Returns:
    tuple: A tuple containing two elements:
        - pandas.DataFrame: The original DataFrame with additional columns for regression results.
        - pandas.DataFrame: A DataFrame containing indexer rankings based on robust normalized coefficients.
    """
    # Preprocess the data
    X, y, preprocessor = preprocess_data_for_regression(
        df, predictor, categorical, numeric
    )

    # Perform linear regression
    pipeline, y_pred = perform_regression(X, y, preprocessor)

    # Analyze the results
    results_df = analyze_regression_results(pipeline, X, y, y_pred)

    # Calculate robust normalized coefficients
    indexer_rankings = calculate_robust_normalized_coefficients(results_df)

    return df, indexer_rankings


def preprocess_data_for_regression(df, predictor, categorical, numeric):
    """
    Preprocess data for linear regression by encoding categorical variables and scaling numeric variables.

    This function prepares the input data for linear regression by separating features and target variables,
    and applying appropriate preprocessing techniques to categorical and numeric features.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing all variables.
    predictor (list): List of column names to be used as the dependent variable(s).
    categorical (list): List of column names containing categorical features.
    numeric (list): List of column names containing numeric features.

    Returns:
    tuple: A tuple containing three elements:
        - pandas.DataFrame: Preprocessed feature DataFrame (X).
        - pandas.DataFrame: Target variable DataFrame (y).
        - ColumnTransformer: The preprocessor object used for transforming the data.
    """
    model_columns = categorical + numeric

    # Define features (X) and target (y)
    X = df[model_columns]
    y = df[predictor]

    # Use a Column transformer to apply OneHotEncoder to categorical data and StandardScaler to numeric data.
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "one_hot",
                OneHotEncoder(handle_unknown="ignore", drop="first"),
                categorical,
            ),
            ("scaler", StandardScaler(), numeric),
        ],
        remainder="passthrough",
    )

    return X, y, preprocessor


def perform_regression(X, y, preprocessor):
    """
    Perform linear regression using preprocessed data.

    This function creates a regression pipeline that includes the preprocessor and a linear regression model,
    fits the pipeline to the data, and generates predictions.

    Parameters:
    X (pandas.DataFrame): The feature DataFrame.
    y (pandas.DataFrame): The target variable DataFrame.
    preprocessor (ColumnTransformer): The preprocessor object for transforming the features.

    Returns:
    tuple: A tuple containing two elements:
        - sklearn.pipeline.Pipeline: The fitted regression pipeline.
        - numpy.ndarray: The predicted values (y_pred).
    """
    # Create regression pipeline
    pipeline = Pipeline(
        [("preprocessor", preprocessor), ("regressor", LinearRegression())]
    )

    # Fit pipeline & Use pipeline to predict Y
    pipeline.fit(X, y)
    y_pred = pipeline.predict(X)

    return pipeline, y_pred


def analyze_regression_results(pipeline, X, y, y_pred):
    """
    Analyze the results of the linear regression model.

    This function computes various statistical measures to evaluate the performance of the regression model,
    including coefficients, standard errors, and p-values for each feature.

    Parameters:
    pipeline (sklearn.pipeline.Pipeline): The fitted regression pipeline.
    X (pandas.DataFrame): The feature DataFrame.
    y (pandas.DataFrame): The actual target variable DataFrame.
    y_pred (numpy.ndarray): The predicted values from the model.

    Returns:
    pandas.DataFrame: A DataFrame containing the following columns for each feature:
        - 'Variable': Name of the feature
        - 'Coefficient': Estimated coefficient
        - 'Standard Error': Standard error of the coefficient
        - 'p-value': p-value for the coefficient
    """
    # Calculate the mean_squared_error
    mse = mean_squared_error(y, y_pred)

    # Extract feature names and coefficients from the regression pipeline
    feature_names = pipeline.named_steps["preprocessor"].get_feature_names_out()
    coefficients = pipeline.named_steps["regressor"].coef_

    # Ensure coefficients are a flat array
    if coefficients.ndim > 1:
        coefficients = coefficients.flatten()

    # Calculate standard error of each coefficient
    X_transformed = pipeline.named_steps["preprocessor"].transform(X)
    XtX_inv = pinv(
        np.dot(X_transformed.T, X_transformed) + np.eye(X_transformed.shape[1]) * 1.0
    )
    var_covar_matrix = mse * XtX_inv
    std_errors = np.sqrt(np.diag(var_covar_matrix))

    # Calculate significance of regression coefficients
    degfreedom = len(y) - len(coefficients)
    t_scores = coefficients / std_errors
    p_values = [2 * (1 - t.cdf(abs(t_score), degfreedom)) for t_score in t_scores]

    # Create results_df
    results_df = pd.DataFrame(
        {
            "Variable": feature_names,
            "Coefficient": coefficients,
            "Standard Error": std_errors,
            "p-value": p_values,
        }
    )

    return results_df


def calculate_robust_normalized_coefficients(results_df):
    """
    Calculate robust normalized coefficients for indexer rankings based on regression results.

    This function processes the regression results to create a ranking of indexers based on their
    coefficients, adjusting for statistical uncertainty and normalizing the results.

    Parameters:
    results_df (pandas.DataFrame): DataFrame containing regression results, including coefficients
                                   and standard errors for each variable.

    Returns:
    pandas.DataFrame: A DataFrame with columns:
        - 'indexer': Identifier for each indexer
        - 'Coefficient': Original regression coefficient
        - 'Standard Error': Standard error of the coefficient
        - 'p-value': p-value of the coefficient
        - 'Coefficient + 1.5 SE': Coefficient adjusted by adding 1.5 times the standard error
        - 'Robust Normalized Coefficient + 1.5 SE': Normalized version of the adjusted coefficient
    """
    # Extract indexer coefficients
    indexer_rankings = results_df[
        (results_df["Variable"].str.startswith("one_hot__indexer_"))
        & (~results_df["Variable"].str.startswith("one_hot__indexer_network_"))
    ].sort_values(by="Coefficient")

    # Reset the index and remove the old index column for a clean, sequential index
    indexer_rankings.reset_index(inplace=True)
    indexer_rankings.drop(columns=["index"], inplace=True)

    # Drop one_hot__indexer_ from coefficent names
    indexer_rankings["Variable"] = indexer_rankings["Variable"].str.replace(
        "one_hot__indexer_", ""
    )

    # Rename columns appropriately
    indexer_rankings.rename(columns={"Variable": "indexer"}, inplace=True)

    # Drop nan's
    indexer_rankings.dropna(
        subset=["Coefficient", "Standard Error", "p-value"], inplace=True
    )

    # Calculate the coefficient + 1.5 standard errors.
    indexer_rankings["Coefficient + 1.5 SE"] = (
        indexer_rankings["Coefficient"] + 1.5 * indexer_rankings["Standard Error"]
    )

    # Calculate the median and IQR
    median_val = indexer_rankings["Coefficient + 1.5 SE"].median()
    q1 = indexer_rankings["Coefficient + 1.5 SE"].quantile(0.25)
    q3 = indexer_rankings["Coefficient + 1.5 SE"].quantile(0.75)
    iqr_val = q3 - q1

    # Normalize the Coefficient + 1.5 SE using median and IQR
    indexer_rankings["Robust Normalized Coefficient + 1.5 SE"] = (
        indexer_rankings["Coefficient + 1.5 SE"] - median_val
    ) / iqr_val

    return indexer_rankings


def calculate_indexer_success_rate(df):
    """
    Calculate the success rate for each indexer based on query status.

    This function computes the proportion of successful queries (status '200 OK' or 'Unavailable(MissingBlock)')
    for each indexer in the dataset.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing 'indexer' and 'status' columns.

    Returns:
    pandas.DataFrame: A DataFrame with columns:
        - 'indexer': Unique identifier for each indexer
        - 'average_status': The proportion of successful queries for each indexer (range 0 to 1)
    """
    df_filtered = df[["indexer", "status"]].copy()
    df_filtered["status_numeric"] = df_filtered["status"].apply(
        lambda x: 1 if x in ["200 OK", "Unavailable(MissingBlock)"] else 0
    )
    indexer_success_rate = (
        df_filtered.groupby("indexer")
        .agg(average_status=("status_numeric", "mean"))
        .reset_index()
    )

    # Sorting by indexer name as a tie-breaker when success rates are equal.
    return indexer_success_rate.sort_values(
        by=["average_status", "indexer"], ascending=[True, True]
    )


def calculate_indexer_uptime(df, threshold_seconds=120):
    """
    Calculate the indexer uptime based on query response statuses and timestamps.

    This function computes two types of uptime metrics for each indexer:
    1. Full uptime: Considers the entire time range between queries.
    2. Restricted uptime: Limits the considered time between queries to a 'threshold' e.g. 120 seconds.

    The uptime calculation process involves:
    1. Determining the midpoint between consecutive timestamps for each indexer.
    2. Considering an indexer as 'up' if the status is '200 OK' or 'Unavailable(MissingBlock)'.
    3. Calculating the duration between midpoints infront and after a specific query response when the indexer is 'up'.
    4. Summing these durations to get the total uptime (seconds) for each indexer.
    5. Comparing the uptime to the total observed time to calculate the percentage uptime.

    The restricted uptime calculation differs in the following ways:
    - Both the restricted uptime and the total observed time are capped at the threshold for each interval.
    - This results in a separate, tailored calculation where both the numerator (restricted uptime)
      and denominator (observed time) are adjusted based on the threshold.
    - The restricted uptime percentage may differ significantly from the full uptime
      percentage, especially when there are large gaps between queries.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing 'indexer', 'timestamp', and 'status' columns.
    threshold_seconds (int, optional): Maximum time gap to consider for restricted uptime calculation.
                                       Defaults to 120 seconds.

    Returns:
    pandas.DataFrame: A DataFrame with columns:
        - 'indexer': Unique identifier for each indexer
        - 'observed_duration_restricted': Total observed time within the threshold
        - 'uptime_duration_restricted': Calculated uptime within the threshold
        - '% up_x': Percentage uptime based on restricted calculation
        - 'observed_duration_full': Total observed time without restrictions
        - 'uptime_duration_full': Calculated uptime without restrictions
        - '% up_y': Percentage uptime based on full calculation
    """
    df_copy = df.copy()
    df_copy["timestamp"] = pd.to_datetime(df_copy["timestamp"])
    df_copy.sort_values(by=["indexer", "timestamp"], inplace=True)

    # Calculate next and previous timestamps for each query
    df_copy["next_timestamp"] = df_copy.groupby("indexer")["timestamp"].shift(-1)
    df_copy["previous_timestamp"] = df_copy.groupby("indexer")["timestamp"].shift(1)

    # Calculate the seconds to the next/previous timestamps.
    df_copy["gap_to_next_query"] = (
        df_copy["next_timestamp"] - df_copy["timestamp"]
    ).dt.total_seconds()
    df_copy["gap_to_previous_query"] = (
        df_copy["timestamp"] - df_copy["previous_timestamp"]
    ).dt.total_seconds()

    # Set next_midpoint as the current timestamp plus half the gap to the next query
    # If a query represents the final query in the data for the indexer then next_midpoint is just equal to timestamp
    df_copy["next_midpoint"] = df_copy["timestamp"] + pd.to_timedelta(
        df_copy["gap_to_next_query"] / 2, unit="s"
    )
    df_copy["next_midpoint"] = df_copy["next_midpoint"].fillna(df_copy["timestamp"])

    # Set previous_midpoint as the current timestamp minus half the gap to the prior query
    # If a query represents the first query in the data for the indexer then previous_midpoint is just equal to timestamp
    df_copy["previous_midpoint"] = df_copy["timestamp"] - pd.to_timedelta(
        df_copy["gap_to_previous_query"] / 2, unit="s"
    )
    df_copy["previous_midpoint"] = df_copy["previous_midpoint"].fillna(
        df_copy["timestamp"]
    )

    # Use query response status to inform weather an indexer is online/offline.
    df_copy["is_up"] = (df_copy["status"] == "200 OK") | (
        df_copy["status"] == "Unavailable(MissingBlock)"
    )

    # Calculate uptime durations using next/prior midpoints, when the indexer was up
    df_copy["uptime_duration_full"] = (
        (df_copy["next_midpoint"] - df_copy["previous_midpoint"])
        .dt.total_seconds()
        .where(df_copy["is_up"], 0)
    )
    df_copy["uptime_duration_restricted"] = np.minimum(
        (df_copy["next_midpoint"] - df_copy["previous_midpoint"])
        .dt.total_seconds()
        .where(df_copy["is_up"], 0),
        threshold_seconds,
    )

    # Calculate observed durations using next/prior midpoints
    df_copy["observed_duration_full"] = (
        df_copy["next_midpoint"] - df_copy["previous_midpoint"]
    ).dt.total_seconds()
    df_copy["observed_duration_restricted"] = np.minimum(
        (df_copy["next_midpoint"] - df_copy["previous_midpoint"]).dt.total_seconds(),
        threshold_seconds,
    )

    # Save each indexers uptime.
    uptime_duration_full = df_copy.groupby("indexer")["uptime_duration_full"].sum()
    uptime_duration_restricted = df_copy.groupby("indexer")[
        "uptime_duration_restricted"
    ].sum()

    # Save each indexers total observed time.
    observed_duration_full = df_copy.groupby("indexer")["observed_duration_full"].sum()
    observed_duration_restricted = df_copy.groupby("indexer")[
        "observed_duration_restricted"
    ].sum()

    # Merge and Calculate "% up" for the "full" version
    merged_uptime_full = pd.merge(
        observed_duration_full, uptime_duration_full, on="indexer", how="left"
    ).reset_index()
    merged_uptime_full["% up"] = round(
        merged_uptime_full["uptime_duration_full"]
        / merged_uptime_full["observed_duration_full"]
        * 100,
        3,
    )
    merged_uptime_full = merged_uptime_full.sort_values(by="% up", ascending=False)

    # Merge and Calculate "% up" for the "restricted" version
    merged_uptime_restricted = pd.merge(
        observed_duration_restricted,
        uptime_duration_restricted,
        on="indexer",
        how="left",
    ).reset_index()
    merged_uptime_restricted["% up"] = round(
        merged_uptime_restricted["uptime_duration_restricted"]
        / merged_uptime_restricted["observed_duration_restricted"]
        * 100,
        3,
    )
    merged_uptime_restricted = merged_uptime_restricted.sort_values(
        by="% up", ascending=False
    )

    # Final merge
    # merged_uptime_both['% up_x'] represents merged_uptime_restricted["% up"]
    # merged_uptime_both['% up_y'] represents merged_uptime_full["% up"]
    merged_uptime_both = pd.merge(
        merged_uptime_restricted, merged_uptime_full, on="indexer", how="left"
    )
    return merged_uptime_both


def calculate_stake_to_fees(stake_query_pandas):
    """
    Calculate the stake-to-fees ratio and its deviation from the median for each indexer.

    This function processes the results of the stake-to-fees query, computing the
    interquartile range (IQR) normalized deviation of each indexer's stake-to-fees ratio
    from the median.

    Parameters:
    stake_query_pandas (pandas.DataFrame): DataFrame containing 'indexer' and 'stake_to_fees' columns.

    Returns:
    pandas.DataFrame: A DataFrame with columns:
        - 'indexer': Indexer identifier
        - 'stake_to_fees': Original stake-to-fees ratio
        - 'stake_to_fees_iqr_deviation': IQR-normalized deviation from the median ratio
    """

    stake_to_fees = stake_query_pandas[["stake_to_fees"]].copy()

    median_stake_to_fees = stake_to_fees["stake_to_fees"].median()
    q1 = stake_to_fees["stake_to_fees"].quantile(0.25)
    q3 = stake_to_fees["stake_to_fees"].quantile(0.75)
    iqr = q3 - q1

    stake_to_fees["stake_to_fees_iqr_deviation"] = (
        stake_to_fees["stake_to_fees"] - median_stake_to_fees
    ) / iqr

    # Ensure the index is named 'indexer' before resetting
    stake_to_fees.index.name = "indexer"

    # Reset the index to make 'indexer' a column
    stake_to_fees = stake_to_fees.reset_index()

    return stake_to_fees


def aggregate_indexer_info(df):
    """
    Aggregate organizational and location information for each indexer.

    This function groups the input DataFrame by indexer and aggregates the 'org' and 'destination_loc'
    information, selecting the most frequent value for each. It also rounds the location coordinates
    to the nearest 20 degrees for privacy and generalization purposes.

    Parameters:
    df (pandas.DataFrame): Input DataFrame containing 'indexer', 'org', and 'destination_loc' columns.

    Returns:
    pandas.DataFrame: An aggregated DataFrame with columns:
        - 'indexer': Unique identifier for each indexer
        - 'org': Most frequent organization associated with the indexer
        - 'destination_loc': Most frequent location associated with the indexer, rounded to nearest 20 degrees
    """
    # Group the DataFrame by 'indexer' and calculate the most frequent 'org' and 'destination_loc'
    # for each indexer. The `.mode()[0]` is used to select the first mode in case of multiple modes.
    agg_df = (
        df.groupby("indexer")
        .agg(
            {
                "org": lambda x: x.mode()[0] if not x.mode().empty else np.nan,
                "destination_loc": lambda x: x.mode()[0]
                if not x.mode().empty
                else np.nan,
            }
        )
        .reset_index()
    )

    def process_location(loc):
        if pd.notna(loc):
            lat, long = map(float, loc.split(","))
            return f"{round(lat / 20) * 20},{round(long / 20) * 20}"
        return loc

    agg_df["destination_loc"] = agg_df["destination_loc"].apply(process_location)

    return agg_df


def merge_and_prepare_dataframes(
    indexer_uptime, indexer_rankings, agg_df, indexer_success_rate, stake_to_fees
):
    """
    Merge multiple DataFrames related to indexer performance and prepare a consolidated DataFrame.

    This function combines information from various sources including uptime, rankings,
    organizational data, success rates, and stake-to-fees ratios. It also adds placeholder
    columns for additional metrics.

    Parameters:
    indexer_uptime (pandas.DataFrame): DataFrame with indexer uptime information.
    indexer_rankings (pandas.DataFrame): DataFrame with indexer rankings.
    agg_df (pandas.DataFrame): DataFrame with aggregated indexer organizational information.
    indexer_success_rate (pandas.DataFrame): DataFrame with indexer success rates.
    stake_to_fees (pandas.DataFrame): DataFrame with stake to fees ratios.

    Returns:
    pandas.DataFrame: A merged DataFrame containing all relevant indexer information.
    """
    # Merge df's together
    merged = pd.merge(indexer_uptime, indexer_rankings, on="indexer", how="left")

    # Drop unnecessary columns
    columns_to_drop = ["observed_duration_full", "uptime_duration_full", "% up_y"]
    columns_to_drop = [col for col in columns_to_drop if col in merged.columns]
    merged = merged.drop(columns=columns_to_drop)

    # Drop rows with no useful data if the columns exist
    columns_to_check = ["Coefficient", "Standard Error", "p-value"]
    existing_columns = [col for col in columns_to_check if col in merged.columns]
    if existing_columns:
        merged = merged.dropna(subset=existing_columns)

    # Merge df's together
    merged = pd.merge(merged, agg_df, on="indexer", how="left")

    # Merge df's together
    merged = pd.merge(merged, indexer_success_rate, on="indexer", how="left")

    # Merge df's together
    merged = pd.merge(merged, stake_to_fees, on="indexer", how="left")

    # Add new columns
    merged["existing_dips_agreements"] = 0
    merged["avg_sync_duration"] = np.nan
    merged["indexing_agreement_acceptance_latency"] = np.nan

    return merged


def normalize_metrics(merged):
    """
    Normalize various metrics in the merged DataFrame to create comparable scores across different dimensions.

    This function takes the merged DataFrame containing various indexer metrics and normalizes them,
    to create standardized scores. It handles different types of metrics, applying appropriate
    normalization techniques for each.

    Parameters:
    merged (pandas.DataFrame): The input DataFrame containing various indexer metrics.

    Returns:
    pandas.DataFrame: The input DataFrame with additional columns for normalized metrics:
        - 'norm_lin_reg_coefficient': Normalized linear regression coefficient
        - 'norm_uptime_score': Normalized uptime score
        - 'norm_existing_dips_agreements': Normalized score for existing DIP agreements
        - 'norm_stake_to_fees_iqr_deviation': Normalized stake-to-fees ratio deviation
        - 'norm_success_rate': Normalized success rate
        - 'norm_avg_sync_duration': Normalized average sync duration
        - 'norm_indexing_agreement_acceptance_latency': Normalized acceptance latency

    Note:
    - Each metric is normalized to a scale of 0 to 1, where 1 represents better performance.
    - Some metrics are inverted (1 - normalized value) if lower values are better (e.g., latency).
    - The function handles missing data by assigning a neutral score of 0.5 to NaN values.
    - Different normalization techniques are used based on the nature of each metric:
        - Generic min-max normalization for most metrics
        - Special normalization for uptime and success rate to emphasize high performance
        - Logistic function for acceptance latency
    """
    if merged.empty:
        new_columns = [
            "norm_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_existing_dips_agreements",
            "norm_stake_to_fees_iqr_deviation",
            "norm_success_rate",
            "norm_avg_sync_duration",
            "norm_indexing_agreement_acceptance_latency",
        ]
        for col in new_columns:
            merged[col] = pd.Series(dtype=float)
        return merged

    # Normalise linear regression score
    if "Coefficient + 1.5 SE" in merged.columns:
        merged["norm_lin_reg_coefficient"] = 1 - normalize_generic(
            merged["Coefficient + 1.5 SE"]
        )  # lower is better
    else:
        merged["norm_lin_reg_coefficient"] = np.nan

    # Normalise uptime score
    if "% up_x" in merged.columns:
        merged["norm_uptime_score"] = normalize_uptime_and_success_rate(
            merged["% up_x"]
        )  # higher is better
    else:
        merged["norm_uptime_score"] = np.nan

    # Normalise the number of indexing agreements each indexer has
    if "existing_dips_agreements" in merged.columns:
        merged["norm_existing_dips_agreements"] = 1 - normalize_generic(
            merged["existing_dips_agreements"]
        )  # lower is better
    else:
        merged["norm_existing_dips_agreements"] = np.nan

    # Normalise stake to fees ratio
    if "stake_to_fees_iqr_deviation" in merged.columns:
        merged["norm_stake_to_fees_iqr_deviation"] = normalize_generic(
            merged["stake_to_fees_iqr_deviation"]
        )  # higher is better
    else:
        merged["norm_stake_to_fees_iqr_deviation"] = np.nan

    # Normalise success rate score
    if "average_status" in merged.columns:
        merged["norm_success_rate"] = normalize_uptime_and_success_rate(
            merged["average_status"]
        )  # higher is better
    else:
        merged["norm_success_rate"] = np.nan

    # Normalize avg_sync_duration
    if "avg_sync_duration" in merged.columns:
        merged["norm_avg_sync_duration"] = 1 - normalize_generic(
            merged["avg_sync_duration"]
        )  # lower is better
    else:
        merged["norm_avg_sync_duration"] = np.nan

    # Normalize indexing_agreement_acceptance_latency
    if "indexing_agreement_acceptance_latency" in merged.columns:
        merged["norm_indexing_agreement_acceptance_latency"] = (
            normalize_indexing_agreement_acceptance_latency(
                merged["indexing_agreement_acceptance_latency"]
            )
        )  # lower is better
    else:
        merged["norm_indexing_agreement_acceptance_latency"] = np.nan

    # Fill NaN values with 0.5
    norm_columns = [col for col in merged.columns if col.startswith("norm_")]
    merged[norm_columns] = merged[norm_columns].fillna(0.5)

    return merged


def normalize_generic(series):
    """
    Perform a generic min-max normalization on a pandas Series.

    This function normalizes the input series to a range between 0 and 1 using min-max scaling.
    It handles edge cases such as constant series or series with NaN values.

    Parameters:
    series (pandas.Series): The input series to be normalized.

    Returns:
    pandas.Series: A new series with normalized values between 0 and 1.

    Note:
    - If the input series is empty or contains only one unique value, it returns a series of 0.5.
    """
    if series.empty or series.max() == series.min():
        return pd.Series(0.5, index=series.index)

    min_val = series.min()
    max_val = series.max()
    # Handle cases where all values are the same
    if min_val == max_val:
        return pd.Series(0.5, index=series.index)

    # Normalize to between 0 and 1 range
    normalized = (series - min_val) / (max_val - min_val)

    # Handle any potential NaN or inf values
    normalized = normalized.fillna(0.5)

    return normalized


def normalize_uptime_and_success_rate(series, is_raw_data=True):
    """
    Normalize either uptime or success rate data using a piecewise linear scaling method.

    This function applies a custom normalization to uptime / success rate data, emphasizing
    high performance. Uptime between 0% and 97% of the best indexers uptime results in a
    score of 0, while uptime between 97% and 100% of the best indexers uptime results in a
    linear score scaling from 0 to 1. So for example 98.5% of the best indexers uptime would
    result in a normalised score of 0.5. The same calculation applies to success rate.

    Parameters:
    series (pandas.Series): The input series containing uptime or success rate data.
    is_raw_data (bool, optional): Flag indicating if the data is raw (True) or already normalized (False).
                                  Defaults to True.

    Returns:
    pandas.Series: A new series with normalized values between 0 and 1.
    """
    if series.empty or series.isnull().all() or series.max() == series.min():
        return pd.Series(0.5, index=series.index)

    if is_raw_data:
        # Remove NaN values for calculations.
        valid_series = series.dropna()

        # Find the best uptime/success rate score in the series first.
        best = valid_series.max()

        # Threshold whereby indexers that have less uptime/success rate than this get no score.
        threshold = best - 0.03

        # Linear score between the threshold and the best.
        normalized = valid_series.apply(
            lambda x: max(0, min(1, (x - threshold) / 0.03))
        )

        # Reindex and fill NaN with 0.5.
        normalized = normalized.reindex(series.index).fillna(0.5)

        return normalized

    else:
        # If this is already normalized data, return it as is, filling NaN with 0.5
        return series.fillna(0.5)


def normalize_indexing_agreement_acceptance_latency(latency_series, L=1, k=0.5, x0=12):
    """
    Normalize indexing agreement acceptance latency using a logistic function.

    This function applies a logistic normalization to the acceptance latency data,
    creating a smooth logistic transition between high and low latency values.

    Parameters:
    latency_series (pandas.Series): The input series containing latency data in hours.
    L (float, optional): The logistic function's maximum value. Defaults to 1.
    k (float, optional): The steepness of the curve. Defaults to 0.5.
    x0 (float, optional): The x-value of the sigmoid's midpoint. Defaults to 12 hours.

    Returns:
    pandas.Series: A new series with normalized values between 0 and 1.

    Note:
    - Indexing agreement acceptancy latency should be measured in hours, not minutes or seconds.
    - Lower latency results in higher normalized values.
    - Negative latency values are clipped to 0 before normalization.
    - Very large latency values are clipped to a maximum of 1000 hours to prevent overflow.
    - If the input series is empty or constant, it returns a series of 0.5.
    - NaN values in the input are replaced with 0.5 in the output.
    """
    if latency_series.empty or latency_series.max() == latency_series.min():
        return pd.Series(0.5, index=latency_series.index)

    # Replace negative values with 0 (as negative latency doesn't make sense)
    latency_series = latency_series.clip(lower=0)

    # Clip very large values to avoid overflow
    max_latency = 1000  # Adjust this value based on your expected maximum latency
    clipped_latency = np.clip(latency_series, 0, max_latency)

    # Logistic function to normalize acceptance latency
    normalized = L / (1 + np.exp(k * (clipped_latency - x0)))

    # Handle any potential NaN or inf values
    normalized = normalized.fillna(0.5)

    return normalized


def calculate_weighted_score(row, weights):
    """
    Calculate a weighted score for an indexer based on multiple normalized metrics.

    This function computes a single score by combining multiple performance metrics,
    each weighted according to predefined weights.

    Parameters:
    row (pandas.Series): A series containing normalized metric values for an indexer.
                         Expected to have columns prefixed with 'norm_'.
    weights (dict): A dictionary mapping metric names to their respective weights.
                    Keys should match the suffix of the 'norm_' columns in the row.

    Returns:
    float: The calculated weighted score, or np.nan if no valid metrics are found.
    """
    weighted_sum = 0
    weight_total = 0
    for metric, weight in weights.items():
        if f"norm_{metric}" in row and not pd.isna(row[f"norm_{metric}"]):
            weighted_sum += row[f"norm_{metric}"] * weight
            weight_total += weight
    if weight_total == 0:
        return np.nan

    return weighted_sum / weight_total
