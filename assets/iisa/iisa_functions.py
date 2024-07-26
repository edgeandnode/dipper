import socket
from datetime import datetime, timedelta
from urllib.parse import urlparse

import bigframes.pandas as bpd
import numpy as np
import pandas as pd
import requests
from scipy.stats import t
from numpy.linalg import pinv
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from tabulate import tabulate


def derive_timestamps(num_days):
    """
    Derive start and end timestamps for the data collection period.
    """
    today = datetime.today()

    end_date = today
    start_date = today - timedelta(days=num_days)
    start_ts = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_ts = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    return start_date, end_date, start_ts, end_ts


def get_initial_query(start_date, num_days):
    """
    Construct the initial query to fetch basic filter data.
    """
    return f"""
    WITH BasicFilter AS (
        SELECT
            deployment AS deployment_hash,
            indexer,
            COUNT(*) AS num_rows
        FROM internal_metrics.metrics_indexer_attempts
        WHERE day_partition BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
        GROUP BY deployment_hash, indexer
    ),
    TotalQueries AS (
        SELECT
            deployment_hash,
            indexer,
            num_rows
        FROM BasicFilter
    )
    SELECT
        deployment_hash,
        indexer,
        num_rows
    FROM TotalQueries;
    """


def fetch_initial_query_results(initial_query, project):
    """
    Fetch the initial query results.
    """
    initial_query_results_pandas = bpd.read_gbq(
        initial_query, project_id=project
    ).to_pandas()
    return initial_query_results_pandas.sort_values(by="num_rows", ascending=False)


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
    x = 1000  # Starting estimate for the number of rows to record for each ['deployment_hash', 'indexer'] combination.
    initial_query_results_pandas["num_rows_restricted"] = initial_query_results_pandas[
        "num_rows"
    ].clip(upper=x)
    tolerance = target_rows * 0.01  # 1% tolerance range
    max_iterations = 1000  # Maximum number of iterations to avoid infinite loops
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


def get_combined_query(start_date, num_days, rows_to_use):
    """
    Construct the combined query to fetch detailed data.
    """
    return f"""
    WITH production_metrics_gateway_subgraph_queries AS (
        WITH initial_data AS (
            SELECT
                day_timestamp AS day_partition,
                subgraph_deployment_ipfs_hash AS deployment_hash,
                subgraph_chain_indexed AS subgraph_network,
                subgraph_deployment_chain AS indexer_network
            FROM production_metrics.prod_metrics_gateway_subgraph_queries
            WHERE subgraph_deployment_ipfs_hash IS NOT NULL
            AND subgraph_chain_indexed IS NOT NULL
            AND subgraph_deployment_chain IS NOT NULL
        ),
        non_dupe_data AS (
            SELECT DISTINCT * FROM initial_data
        ),
        mode_subgraph_networks AS (
            SELECT
                deployment_hash,
                subgraph_network,
                COUNT(subgraph_network) AS freq
            FROM non_dupe_data
            GROUP BY deployment_hash, subgraph_network
        ),
        aggregated_data AS (
            SELECT
                n.deployment_hash,
                ARRAY_AGG(n.indexer_network) AS indexer_network_list,
                ARRAY_AGG(DISTINCT n.subgraph_network) AS subgraph_network_list,
                COUNT(DISTINCT n.indexer_network) AS number_of_unique_indexer_networks,
                COUNT(n.indexer_network) AS number_of_indexer_networks,
                ARRAY_AGG(s.subgraph_network ORDER BY s.freq DESC LIMIT 1)[OFFSET(0)] AS mode_subgraph_network
            FROM non_dupe_data n
            LEFT JOIN mode_subgraph_networks s
            ON n.deployment_hash = s.deployment_hash
            GROUP BY n.deployment_hash
        )
        SELECT
            deployment_hash,
            CASE
                WHEN ARRAY_LENGTH(indexer_network_list) = 1 THEN indexer_network_list[OFFSET(0)]
                ELSE 'arbitrum'
            END AS indexer_network,
            CASE
                WHEN ARRAY_LENGTH(subgraph_network_list) = 1 THEN subgraph_network_list[OFFSET(0)]
                ELSE mode_subgraph_network
            END AS subgraph_network
        FROM aggregated_data
        WHERE deployment_hash IS NOT NULL
        AND deployment_hash <> ''
        ORDER BY number_of_unique_indexer_networks DESC
    ),
    
    combined_indexer_dimensions AS (
        WITH indexer_dimensions AS (
            SELECT
                day AS day_partition,
                indexer_wallet AS indexer,
                indexer_url AS url,
                'mainnet-gateway' AS indexer_network
            FROM internal_metrics.indexer_dimensions_daily
            WHERE day BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
        ),
        indexer_dimensions_arbitrum AS (
            SELECT
                day AS day_partition,
                indexer_wallet AS indexer,
                indexer_url AS url,
                'mainnet-thegraph-arbitrum' AS indexer_network
            FROM internal_metrics.indexer_dimensions_arbitrum_daily
            WHERE day BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
        ),
        combined_data AS (
            SELECT * FROM indexer_dimensions
            UNION ALL
            SELECT * FROM indexer_dimensions_arbitrum
        )
        SELECT
            day_partition,
            indexer,
            url,
            CASE
                WHEN indexer_network = 'mainnet-thegraph-arbitrum' THEN 'arbitrum'
                WHEN indexer_network = 'mainnet-gateway' THEN 'mainnet'
            END AS indexer_network
        FROM combined_data
        WHERE indexer IS NOT NULL AND url IS NOT NULL
        GROUP BY day_partition, indexer, url, indexer_network
        ORDER BY day_partition
    ),
    
    metrics_indexer_attempts AS (
        WITH BasicFilter AS (
            SELECT
                query_id,
                deployment AS deployment_hash,
                query_fee AS fee,
                query_ts AS timestamp,
                CAST(blocks_behind AS INT64) AS blocks_behind,
                SAFE_CAST(response_time_ms AS INT64) AS response_time_ms,
                indexer,
                status,
                day_partition,
                RAND() as rnd
            FROM internal_metrics.metrics_indexer_attempts
            WHERE day_partition BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
            AND deployment IN (SELECT deployment_hash FROM production_metrics_gateway_subgraph_queries)
        ),
        FilteredRows AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY deployment_hash, indexer ORDER BY rnd) as row_num
            FROM BasicFilter
        )
        SELECT
            query_id,
            deployment_hash,
            fee,
            timestamp,
            blocks_behind,
            response_time_ms,
            indexer,
            status,
            day_partition
        FROM FilteredRows
        WHERE row_num <= {rows_to_use}
    )
    
    SELECT
        m.query_id,
        m.deployment_hash,
        m.fee,
        m.timestamp,
        m.blocks_behind,
        m.response_time_ms,
        m.indexer,
        m.status,
        m.day_partition,
        pm.subgraph_network,
        c.url
    FROM metrics_indexer_attempts m
    LEFT JOIN production_metrics_gateway_subgraph_queries pm
    ON m.deployment_hash = pm.deployment_hash
    LEFT JOIN combined_indexer_dimensions c
    ON m.indexer = c.indexer AND m.day_partition = c.day_partition AND pm.indexer_network = c.indexer_network
    WHERE pm.indexer_network = 'arbitrum'
    ORDER BY m.timestamp;
    """


def fetch_combined_query_results(combined_query, project):
    """
    Fetch the combined query results.
    """
    return bpd.read_gbq(combined_query, project_id=project).to_pandas()


def get_url_query(start_date, num_days):
    """
    Construct the query to fetch IP data.
    """
    return f"""
    SELECT
        day AS day_partition,
        indexer_wallet AS indexer,
        indexer_url AS url,
        'arbitrum' AS indexer_network
    FROM internal_metrics.indexer_dimensions_arbitrum_daily
    WHERE day BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
    AND indexer_wallet IS NOT NULL AND indexer_url IS NOT NULL
    GROUP BY day, indexer_wallet, indexer_url
    ORDER BY day_partition
    """


def fetch_url_data(url_query, project):
    """
    Fetch the url query results.
    """
    return bpd.read_gbq(url_query, project_id=project).to_pandas()


def apply_location_details(unique_urls_indexers_pandas):
    """
    Apply the function to each URL and expand the results into separate columns.

    Parameters:
    unique_urls_indexers_pandas (DataFrame): DataFrame containing the unique URLs and indexers.

    Returns:
    DataFrame: DataFrame with additional columns for location details.
    """
    unique_urls_indexers_pandas[["location", "org", "loc", "ip"]] = (
        unique_urls_indexers_pandas["url"].apply(extract_location_and_details)
    )
    return unique_urls_indexers_pandas


def extract_location_and_details(url):
    """
    This function extracts location and details from a URL by resolving it to an IP address.

    Parameters:
    url (str): The URL to be resolved.

    Returns:
    pd.Series: A pandas Series containing location details.
    """
    ip = url_to_ip(url)
    return pd.Series(get_location_and_details_from_ip(ip))


def url_to_ip(url):
    """
    This function will figure our the IP address of the host pc for the URL that the indexer reports.
    """
    if pd.isna(url) or not isinstance(url, str):
        return None
    try:
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def get_location_and_details_from_ip(ip):
    """
    This function gets location and other details from an IP address.

    Parameters:
    ip (str): The IP address to be resolved to location details.

    Returns:
    dict: A dictionary containing location/other details.
    """
    if ip is None:
        return {
            "location": "Unknown",
            "org": "Unknown",
            "loc": "Unknown",
            "ip": "Unknown",
        }
    try:
        response = requests.get(f"https://ipinfo.io/{ip}/json?token=67647c2e5ccd95")
        data = response.json()
        return {
            "location": f'{data.get("country", "Unknown")}, {data.get("region", "Unknown")}, {data.get("city", "Unknown")}',
            "org": data.get("org", "Unknown"),
            "loc": data.get("loc", "Unknown"),
            "ip": data.get("ip", "Unknown"),
        }
    except requests.RequestException:
        return {
            "location": "Unknown",
            "org": "Unknown",
            "loc": "Unknown",
            "ip": "Unknown",
        }


def merge_dataframes(combined_query_pandas, unique_urls_indexers_pandas):
    """
    Merge the information contained inside unique_urls_indexers_pandas with combined_query_pandas.

    Parameters:
    combined_query_pandas (DataFrame): The DataFrame containing combined query results.
    unique_urls_indexers_pandas (DataFrame): The DataFrame containing unique URLs and indexers.

    Returns:
    DataFrame: The merged DataFrame.
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
    Create a DataFrame containing the last 3 characters (the IATA code) from the query_id's found inside the DataFrame
    and count the number of times the specific IATA showed up.

    Parameters:
    df (DataFrame): The DataFrame containing the query_id column.

    Returns:
    DataFrame: A DataFrame with IATA codes and their counts.
    """
    iata_df = (
        df.groupby(df["query_id"].str[-3:])
        .agg(count=("query_id", "nunique"))
        .reset_index()
        .rename(columns={"query_id": "IATA_code"})
    )
    return iata_df


def apply_iata_details(iata_df):
    """
    Apply location and details extraction to each IATA code in the DataFrame.
    Remember the IATA_df is already grouped by IATA.

    Parameters:
    iata_df (DataFrame): The DataFrame containing IATA codes.

    Returns:
    DataFrame: The DataFrame with additional columns for latitude, longitude, and country.
    """
    result = iata_df["IATA_code"].apply(get_location_and_details_from_iata)
    iata_df[["latitude", "longitude", "country"]] = result
    return iata_df


def get_location_and_details_from_iata(iata):
    """
    Get location and other details from an IATA Code.

    Parameters:
    iata (str): The IATA code to get details for.

    Returns:
    pd.Series: A pandas Series containing latitude, longitude, and country.
    """
    if iata is None:
        return pd.Series({"latitude": None, "longitude": None, "country": None})
    try:
        response = requests.get(
            f"https://api.api-ninjas.com/v1/airports?iata={iata}",
            headers={"X-Api-Key": "tKjUrCjntxiwVrcAdxyH0w==Wcmi2BuwNCpb2l3K"},
        )
        data = response.json()
        if data and isinstance(data, list) and len(data) > 0:
            return pd.Series(
                {
                    "latitude": float(data[0].get("latitude", None)),
                    "longitude": float(data[0].get("longitude", None)),
                    "country": data[0].get("country", None),
                }
            )
        else:
            return pd.Series({"latitude": None, "longitude": None, "country": None})
    except requests.RequestException:
        return pd.Series({"latitude": None, "longitude": None, "country": None})


def extract_iata_code(df):
    """
    Extract the last 3 characters from the 'query_id' column as a new column 'IATA_code'.

    Parameters:
    df (DataFrame): The DataFrame containing the 'query_id' column.

    Returns:
    DataFrame: The DataFrame with the new 'IATA_code' column.
    """
    df["IATA_code"] = df["query_id"].str[-3:]
    return df


def merge_iata_info(combined_query_pandas, iata_df):
    """
    Merge the information contained inside the newly created IATA_df with the existing combined_query_pandas.

    Parameters:
    combined_query_pandas (DataFrame): The DataFrame containing combined query results.
    iata_df (DataFrame): The DataFrame containing IATA codes and their counts.

    Returns:
    DataFrame: The merged DataFrame.
    """
    merged_df = pd.merge(
        left=iata_df,
        right=combined_query_pandas,
        how="right",  # All rows from the right DataFrame (combined_query_pandas) will be in the merged DataFrame.
        on="IATA_code",
    )
    return merged_df


def process_combined_query_pandas(df):
    """
    Rename columns, drop old columns, and add an indexer count.

    Parameters:
    df (DataFrame): The DataFrame to process.

    Returns:
    DataFrame: The processed DataFrame.
    """
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
    Split origin_loc and destination_loc into latitude and longitude.

    Parameters:
    df (DataFrame): The DataFrame containing 'origin_loc' and 'destination_loc' columns.

    Returns:
    DataFrame: The DataFrame with new 'origin_lat', 'origin_lon', 'dest_lat', and 'dest_lon' columns.
    """
    df[["origin_lat", "origin_lon"]] = (
        df["origin_loc"].str.split(",", expand=True).astype(float)
    )
    df[["dest_lat", "dest_lon"]] = (
        df["destination_loc"].str.split(",", expand=True).astype(float)
    )
    return df


def calculate_distances(df):
    """
    Apply the vectorized Haversine function to calculate distances.

    Parameters:
    df (DataFrame): The DataFrame containing 'origin_lon', 'origin_lat', 'dest_lon', and 'dest_lat' columns.

    Returns:
    DataFrame: The DataFrame with a new 'distance_miles' column.
    """
    df["distance_miles"] = haversine_vectorized(
        df["origin_lon"], df["origin_lat"], df["dest_lon"], df["dest_lat"]
    )
    return df


def haversine_vectorized(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points on the earth (specified in decimal degrees).

    Parameters:
    lon1, lat1, lon2, lat2 (array-like): Arrays of longitude and latitude values.

    Returns:
    array-like: Distances between points in miles.
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
    Drop intermediate columns to save memory.

    Parameters:
    df (DataFrame): The DataFrame to process.

    Returns:
    DataFrame: The DataFrame with intermediate columns dropped.
    """
    df.drop(columns=["origin_lat", "origin_lon", "dest_lat", "dest_lon"], inplace=True)
    return df


def filter_status(df):
    """
    Filter the DataFrame to only include rows where status is '200 OK'.

    Parameters:
    df (DataFrame): The DataFrame to filter.

    Returns:
    DataFrame: The filtered DataFrame.
    """
    return df[df["status"] == "200 OK"].copy()


def apply_round_distance(df):
    """
    Apply the round_distance function to the 'distance_miles' column.

    Parameters:
    df (DataFrame): The DataFrame with the 'distance_miles' column.

    Returns:
    DataFrame: The DataFrame with rounded distances.
    """
    df.loc[:, "distance_miles"] = df["distance_miles"].apply(round_distance)
    return df


def round_distance(value):
    """
    Round the distance to the nearest 250 miles.

    Parameters:
    value (float): The distance in miles.

    Returns:
    float: The rounded distance.
    """
    return round(value / 250) * 250


def filter_columns(df, all_columns):
    """
    Filter the DataFrame to include only the specified columns.

    Parameters:
    df (DataFrame): The DataFrame to filter.
    all_columns (list): The list of columns to keep.

    Returns:
    DataFrame: The filtered DataFrame.
    """
    return df[all_columns]


def iterative_filter(df, a, b, c, d):
    """
    Iteratively filter the DataFrame based on specified thresholds for indexers, deployments, and queries.
    Apply filtering criteria in rounds, recalculating metrics and adjusting filters in each iteration, until the
    DataFrame size is no longer shrinking after a round.

    Parameters:
    `df`: DataFrame to filter.
    `a`: Each deployment must be served by at least a indexers.
    `b`: Each indexer must serve at least b deployments.
    `c`: Each indexer must serve at least c queries.
    `d`: Each subgraph deployment must be queried at least d times.

    Returns:
    DataFrame: The filtered DataFrame.
    """
    while True:
        initial_len = len(df)

        # Ensure deployments have at least `a` indexers
        indexer_per_deployment = df.groupby("deployment_hash")["indexer"].nunique()
        df = df[df["deployment_hash"].map(indexer_per_deployment) >= a]

        # Ensure indexers serve at least `b` deployments
        deployment_per_indexer = df.groupby("indexer")["deployment_hash"].nunique()
        df = df[df["indexer"].map(deployment_per_indexer) >= b]

        # Ensure indexers serve at least `c` unique queries
        queries_per_indexer = df.groupby("indexer")["query_id"].nunique()
        df = df[df["indexer"].map(queries_per_indexer) >= c]

        # Ensure deployments have at least `d` queries
        query_counts_per_deployment = df.groupby("deployment_hash").size()
        df = df[df["deployment_hash"].map(query_counts_per_deployment) >= d]

        # Check if no change in DataFrame size, else run the loop again.
        if len(df) == initial_len:
            break

    return df


def strategic_sample(df, rows_to_use, cap_per_indexer=None):
    """
    Sample query_id's in a way that creates balanced representation across indexers on each subgraph.

    Parameters:
    df (DataFrame): The DataFrame to sample.
    rows_to_use (int): The number of rows to target.
    cap_per_indexer (dict, optional): Cap per indexer. Defaults to None.

    Returns:
    DataFrame: The sampled DataFrame.
    int: The square root of the number of sampled IDs.
    """

    # Calculate number of unique indexers per subgraph.
    # Then calculate how many queries to sample for each indexer, subgraph combination.
    if cap_per_indexer is None:
        indexers_per_subgraph = df.groupby("deployment_hash")["indexer"].nunique()
        cap_per_indexer = indexers_per_subgraph.map(
            lambda x: rows_to_use // x if x else 0
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
    query_counts["sampled_query_ids"] = query_counts.apply(
        lambda x: sample_queries(x["unique_query_ids"], x["cap"]), axis=1
    )

    # Filter the df with the sampled id's
    sampled_ids = set(np.concatenate(query_counts["sampled_query_ids"].values))
    df["sampled_query_id"] = df["query_id"].apply(
        lambda x: x if x in sampled_ids else None
    )

    # Take the square root of the number of sampled id's to inform the number of buckets to hash mod the query into.
    integer_root = int(np.sqrt(len(sampled_ids)))

    return df, integer_root


def hash_sampled_queries(df, integer_root):
    """
    Hash the sampled query_id's to the hash mod of the integer root.

    Parameters:
    df (DataFrame): The DataFrame with sampled queries.
    integer_root (int): The integer root used for hashing.

    Returns:
    DataFrame: The DataFrame with hashed query IDs.
    """
    df.loc[
        df["sampled_query_id"].notna(), "sampled_query_id_hashed_mod_integer_root"
    ] = df["sampled_query_id"].apply(lambda x: hash(x) % integer_root)
    return df


def perform_linear_regression(df):
    """
    Perform linear regression on the given data.

    Parameters:
    df (DataFrame): The data to perform regression on, must contain 'response_time_ms' and 'Score' columns.

    Returns:
    DataFrame: The original data with an added 'predicted_score' column containing regression predictions.
    """
    # Preprocess the data
    X, y, preprocessor = preprocess_data_for_regression(df)

    # Perform linear regression
    pipeline, y_pred = perform_regression(X, y, preprocessor)

    # Analyze the results
    results_df = analyze_regression_results(pipeline, X, y, y_pred)

    # Calculate robust normalized coefficients
    indexer_rankings = calculate_robust_normalized_coefficients(results_df)

    return df, indexer_rankings


def preprocess_data_for_regression(df, predictor, categorical, numeric):
    """
    Preprocess data for linear regression by applying one-hot encoding to categorical features
    and scaling numeric features.

    Parameters:
    df (DataFrame): The DataFrame containing the data.
    predictor (list): List of predictor column names.
    categorical (list): List of categorical column names.
    numeric (list): List of numeric column names.

    Returns:
    X (DataFrame): The preprocessed feature DataFrame.
    y (DataFrame): The target variable DataFrame.
    preprocessor (ColumnTransformer): The preprocessor object.
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
    Perform linear regression on the given data.

    Parameters:
    X (DataFrame): The feature DataFrame.
    y (DataFrame): The target variable DataFrame.
    preprocessor (ColumnTransformer): The preprocessor object.

    Returns:
    Pipeline: The regression model pipeline.
    ndarray: The predicted values.
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
    Analyze the results of the linear regression.

    Parameters:
    pipeline (Pipeline): The regression model pipeline.
    X (DataFrame): The feature DataFrame.
    y (DataFrame): The target variable DataFrame.
    y_pred (ndarray): The predicted values.

    Returns:
    results_df (DataFrame): DataFrame containing regression coefficients and statistics.
    """
    mse = mean_squared_error(y, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y, y_pred)
    r2 = r2_score(y, y_pred)
    adjusted_r2 = 1 - ((1 - r2) * (len(y) - 1) / (len(y) - X.shape[1] - 1))

    # Print regression model stats
    print(f"RMSE: {rmse}")
    print(f"MAE: {mae}")
    print(f"R²: {r2}")
    print(f"Adjusted R²: {adjusted_r2}")

    # Extract feature names, coefficients, and standard errors from the regression pipeline
    feature_names = pipeline.named_steps["preprocessor"].get_feature_names_out()
    coefficients = pipeline.named_steps["regressor"].coef_
    intercept = pipeline.named_steps["regressor"].intercept_

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

    # Show results
    print(f"The regression intercept is: {intercept}")
    print(tabulate(results_df, headers="keys", tablefmt="psql", showindex=False))

    return results_df


def calculate_robust_normalized_coefficients(results_df):
    """
    Calculate robust normalized coefficients for the indexers.

    Parameters:
    results_df (DataFrame): DataFrame containing regression coefficients and statistics.

    Returns:
    indexer_rankings (DataFrame): DataFrame containing indexer rankings based on the robust normalized coefficients.
    """
    indexer_rankings = results_df[
        results_df["Variable"].str.startswith("one_hot__indexer_")
    ].sort_values(by="Coefficient")
    indexer_rankings.reset_index(inplace=True)
    indexer_rankings.drop(columns=["index"], inplace=True)
    indexer_rankings["Variable"] = indexer_rankings["Variable"].str.replace(
        "one_hot__indexer_network_", ""
    )
    indexer_rankings["Variable"] = indexer_rankings["Variable"].str.replace(
        "one_hot__indexer_", ""
    )
    indexer_rankings = indexer_rankings[indexer_rankings["Variable"] != "mainnet"]
    indexer_rankings.rename(columns={"Variable": "indexer"}, inplace=True)
    indexer_rankings.dropna(
        subset=["Coefficient", "Standard Error", "p-value"], inplace=True
    )

    indexer_rankings["Coefficient + 1.5 SE"] = (
        indexer_rankings["Coefficient"] + 1.5 * indexer_rankings["Standard Error"]
    )

    # Calculate the median and IQR
    median_val = indexer_rankings["Coefficient + 1.5 SE"].median()
    q1 = indexer_rankings["Coefficient + 1.5 SE"].quantile(0.25)
    q3 = indexer_rankings["Coefficient + 1.5 SE"].quantile(0.75)
    iqr_val = q3 - q1

    # Normalize the values using median and IQR
    indexer_rankings["Robust Normalized Coefficient + 1.5 SE"] = (
        indexer_rankings["Coefficient + 1.5 SE"] - median_val
    ) / iqr_val

    return indexer_rankings


def calculate_indexer_success_rate(df):
    """
    Calculate the indexer query success rate.
    '200 OK' or 'Unavailable(MissingBlock)' =  success
    Anything else = fail.

    Parameters:
    df (DataFrame): The data frame containing indexer and status columns.

    Returns:
    DataFrame: Data frame with indexer and their success rates.
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
    return indexer_success_rate.sort_values(by="average_status", ascending=True)


def calculate_indexer_uptime(df, threshold_seconds=120):
    """
    Calculate the indexer uptime.

    Parameters:
    df (DataFrame): The data frame containing indexer and timestamp columns.
    threshold_seconds (int): Threshold for restricted uptime calculation.

    Returns:
    DataFrame: Data frame with indexer uptime information.
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
    merged_uptime_both = pd.merge(
        merged_uptime_restricted, merged_uptime_full, on="indexer", how="left"
    )
    return merged_uptime_both


def get_initial_stake_to_fees_query():
    """
    Construct the initial query to fetch the stake to fees data.
    """
    return """
    SELECT  indexer_wallet AS indexer,
            GREATEST(available_stake, 0) /
                CASE
                    WHEN ROUND((query_fees_collected - query_fee_rebates - delegator_query_fees), 0) = 0
                    THEN 1
                    ELSE ROUND((query_fees_collected - query_fee_rebates - delegator_query_fees), 0)
                END AS stake_to_fees
    FROM internal_metrics.indexer_dimensions_arbitrum
    """


def calculate_stake_to_fees(initial_stake_query):
    """
    Calculate the stake to fees ratio.

    Returns:
    DataFrame: Data frame with stake to fees ratio.
    """
    stake_query_pandas = bpd.read_gbq(initial_stake_query).to_pandas()
    stake_to_fees = stake_query_pandas[["indexer", "stake_to_fees"]].copy()
    median_stake_to_fees = stake_to_fees["stake_to_fees"].median()
    q1 = stake_to_fees["stake_to_fees"].quantile(0.25)
    q3 = stake_to_fees["stake_to_fees"].quantile(0.75)
    iqr = q3 - q1
    stake_to_fees["stake_to_fees_iqr_deviation"] = (
        stake_to_fees["stake_to_fees"] - median_stake_to_fees
    ) / iqr
    return stake_to_fees


def aggregate_indexer_info(df):
    """
    Aggregate indexer organizational and location information.

    Parameters:
    df (DataFrame): The data frame containing indexer information.

    Returns:
    DataFrame: Aggregated data frame with indexer organizational and location information.
    """
    agg_df = (
        df.groupby("indexer")
        .agg({"org": lambda x: x.mode()[0], "destination_loc": lambda x: x.mode()[0]})
        .reset_index()
    )

    # Function to round lat/long to nearest 20 deg.
    def process_location(loc):
        lat, long = map(float, loc.split(","))
        return f"{round(lat / 20) * 20},{round(long / 20) * 20}"

    # Round the Lat/Long with prior function
    agg_df["destination_loc"] = agg_df["destination_loc"].apply(
        lambda x: process_location(x) if pd.notna(x) else x
    )

    return agg_df


def merge_and_prepare_dataframes(
    indexer_uptime, indexer_rankings, agg_df, indexer_success_rate, stake_to_fees
):
    """
    Merge and prepare dataframes.

    Parameters:
    indexer_uptime (DataFrame): Data frame with indexer uptime information.
    indexer_rankings (DataFrame): Data frame with indexer rankings.
    agg_df (DataFrame): Data frame with indexer organizational information.
    indexer_success_rate (DataFrame): Data frame with indexer success rates.
    stake_to_fees (DataFrame): Data frame with stake to fees ratios.

    Returns:
    DataFrame: Merged data frame.
    """
    # Merge df's together
    merged = pd.merge(indexer_uptime, indexer_rankings, on="indexer", how="left")

    # Drop unnecessary columns
    merged = merged.drop(
        columns=["observed_duration_full", "uptime_duration_full", "% up_y", "% up_x"]
    )
    merged = merged.dropna(subset=["Coefficient", "Standard Error", "p-value"])

    # Merge df's together
    merged = pd.merge(merged, agg_df, on="indexer", how="left")

    # Merge df's together
    merged = pd.merge(merged, indexer_success_rate, on="indexer", how="left")

    # Merge df's together
    merged = pd.merge(merged, stake_to_fees, on="indexer", how="left")

    # Add new columns
    merged["existing_dips_agreements"] = 0
    merged["avg_sync_duration"] = np.nan
    return merged


def normalize_metrics(merged):
    # Normalise linear regression score:
    merged["norm_lin_reg_coefficient"] = 1 - normalize_generic(
        merged["Coefficient + 1.5 SE"]
    )  # lower is better

    # Normalise uptime score:
    merged["norm_uptime_score"] = normalize_uptime_and_success_rate(
        merged["up_x"]
    )  # higher is better

    # Normalise the number of indexing agreements each indexer has:
    merged["norm_existing_dips_agreements"] = 1 - normalize_generic(
        merged["existing_dips_agreements"]
    )  # lower is better

    # Normalise stake to fees ratio:
    merged["norm_stake_to_fees_iqr_deviation"] = normalize_generic(
        merged["stake_to_fees_iqr_deviation"]
    )  # higher is better

    # Normalise success rate score:
    merged["norm_success_rate"] = normalize_uptime_and_success_rate(
        merged["average_status"]
    )  # higher is better

    # Needs attention:
    merged["norm_avg_sync_duration"] = 1 - normalize_generic(
        merged["avg_sync_duration"]
    )  # lower is better

    merged["norm_indexing_agreement_acceptance_latency"] = (
        normalize_indexing_agreement_acceptance_latency(
            merged["indexing_agreement_acceptance_latency"]
        )
    )

    return merged


# Function to normalize other metrics
def normalize_generic(series):
    return (series - series.min()) / (series.max() - series.min())


# Function to normalize uptime/succes rate
def normalize_uptime_and_success_rate(series):
    return series.apply(lambda x: max(0, (x - 0.97) / 0.03))


# Function to Normalize acceptance latency
def normalize_indexing_agreement_acceptance_latency(latency, L=1, k=0.5, x0=12):
    return L / (1 + np.exp(k * (latency - x0)))


def calculate_weighted_score(row, weights):
    weighted_sum = 0
    weight_total = 0
    for metric, weight in weights.items():
        if not pd.isna(row[f"norm_{metric}"]):
            weighted_sum += row[f"norm_{metric}"] * weight
            weight_total += weight
    if weight_total == 0:
        return np.nan

    return weighted_sum / weight_total
