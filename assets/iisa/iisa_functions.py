import logging

import numpy as np
import pandas as pd

# Constants
NON_ZERO_UPTIME_SUCCESS_RATE_SCORE_THRESHOLD = 0.97

# Module-level logger
logger = logging.getLogger(__name__)


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
        - 'norm_lat_lin_reg_coefficient': Normalized latency linear regression coefficient
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
            "norm_lat_lin_reg_coefficient",
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

    # Normalise latency linear regression score
    if "Latency Coefficient + Error Confidence Interval" in merged.columns:
        merged["norm_lat_lin_reg_coefficient"] = 1 - _normalize_generic(
            merged["Latency Coefficient + Error Confidence Interval"]
        )  # lower is better
    else:
        merged["norm_lat_lin_reg_coefficient"] = np.nan

    # Normalise uptime score
    if "% up_x" in merged.columns:
        merged["norm_uptime_score"] = _normalize_uptime_and_success_rate(
            merged["% up_x"]
        )  # higher is better
    else:
        merged["norm_uptime_score"] = np.nan

    # Normalise the number of indexing agreements each indexer has
    if "existing_dips_agreements" in merged.columns:
        merged["norm_existing_dips_agreements"] = 1 - _normalize_generic(
            merged["existing_dips_agreements"]
        )  # lower is better
    else:
        merged["norm_existing_dips_agreements"] = np.nan

    # Normalise stake to fees ratio
    if "stake_to_fees_iqr_deviation" in merged.columns:
        merged["norm_stake_to_fees_iqr_deviation"] = _normalize_generic(
            merged["stake_to_fees_iqr_deviation"]
        )  # higher is better
    else:
        merged["norm_stake_to_fees_iqr_deviation"] = np.nan

    # Normalise success rate score
    if "average_status" in merged.columns:
        merged["norm_success_rate"] = _normalize_uptime_and_success_rate(
            merged["average_status"]
        )  # higher is better
    else:
        merged["norm_success_rate"] = np.nan

    # Normalize avg_sync_duration
    if "avg_sync_duration" in merged.columns:
        merged["norm_avg_sync_duration"] = 1 - _normalize_generic(
            merged["avg_sync_duration"]
        )  # lower is better
    else:
        merged["norm_avg_sync_duration"] = np.nan

    # Normalize indexing_agreement_acceptance_latency
    if "indexing_agreement_acceptance_latency" in merged.columns:
        merged["norm_indexing_agreement_acceptance_latency"] = (
            _normalize_indexing_agreement_acceptance_latency(
                merged["indexing_agreement_acceptance_latency"]
            )
        )  # lower is better
    else:
        merged["norm_indexing_agreement_acceptance_latency"] = np.nan

    # Fill NaN values with 0 for all norm_ columns except norm_indexing_agreement_acceptance_latency
    norm_columns = [
        col
        for col in merged.columns
        if col.startswith("norm_")
        and col != "norm_indexing_agreement_acceptance_latency"
    ]
    merged[norm_columns] = merged[norm_columns].fillna(0)

    return merged


def _normalize_generic(series):
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
    min_val = series.min()
    max_val = series.max()

    # Normalize to between 0 and 1 range
    normalized = (series - min_val) / (max_val - min_val)

    # Handle any potential NaN or inf values
    normalized = normalized.fillna(0)

    return normalized


def _normalize_uptime_and_success_rate(series):
    """
    Normalize either uptime or success rate data using a piecewise linear scaling method.

    This function applies a custom normalization to uptime / success rate data, emphasizing
    high performance. Uptime between 0% and 97% of the best indexers uptime results in a
    score of 0, while uptime between 97% and 100% of the best indexers uptime results in a
    linear score scaling from 0 to 1. So for example 98.5% of the best indexers uptime would
    result in a normalised score of 0.5. The same calculation applies to success rate.

    Parameters:
    series (pandas.Series): The input series containing uptime or success rate data.

    Returns:
    pandas.Series: A new series with normalized values between 0 and 1.
    """
    # Find the best uptime/success rate score in the series first.
    best = series.max()

    # Threshold whereby indexers that have less uptime/success rate than this get no score.
    threshold = best * NON_ZERO_UPTIME_SUCCESS_RATE_SCORE_THRESHOLD

    # Linear score between the threshold and the best.
    normalized = series.apply(
        lambda x: max(
            0,
            min(1, (x - threshold) / (best - threshold)),
        )
    )

    # Reindex and fill NaN's with 0.
    normalized = normalized.reindex(series.index).fillna(0)

    return normalized


def _normalize_indexing_agreement_acceptance_latency(
    latency_series,
    l=1.002,  # noqa: E741
    k=1,
    x0=6,
):
    """
    Normalize indexing agreement acceptance latency using a piecewise function:
    logistic for x ≤ x0, linear for x > x0.

    Parameters:
    latency_series (pandas.Series): The input series containing latency data in hours.
    L (float, optional): The logistic function's maximum value. Defaults to 1.002.
    k (float, optional): The steepness of the curve. Defaults to 1.
    x0 (float, optional): The x-value of the sigmoid's midpoint. Defaults to 6 hours.

    Returns:
    pandas.Series: A new series with normalized values between 0 and 1.

    Note:
    - Indexing agreement acceptancy latency should be measured in hours to 2 d.p, not minutes or seconds.
    - Lower latency results in higher normalized values.
    - Negative latency values are clipped to 0 before normalization.
    - Large latency values are clipped to a maximum of 8 hours, after this the score is 0 anyway.
    """

    def logistic(x):
        """
        This function creates the smooth transition from high scores
        for low latencies to low scores for high latencies.

        x: time in hours
        """
        return l / (1 + np.exp(k * (x - x0)))

    # x0 is the midpoint of the logistic function, we need to find the gradient of the slope through that point
    def slope_at_x0():
        """
        Calculate the slope of the logistic function at x0.
        """
        h = 1e-6
        return (logistic(x0 + h) - logistic(x0 - h)) / (2 * h)

    m = slope_at_x0()

    def piecewise_function(x):
        """
        Apply a piecewise function: logistic for x ≤ x0, linear for x > x0.
        """
        return np.where(x <= x0, logistic(x), logistic(x0) + m * (x - x0))

    # Replace negative values with 0 (as negative latency doesn't make sense)
    latency_series = latency_series.clip(lower=0)

    # Configure max input latency and clip the series so all values are <= the max value.
    max_latency = 8
    clipped_latency = np.clip(latency_series, 0, max_latency)

    # Apply the piecewise function to normalize acceptance latency
    normalized = pd.Series(piecewise_function(clipped_latency)).round(3)

    # Handle NaN's
    normalized = normalized.fillna(0)

    return normalized


def calculate_weighted_score(row, weights):
    """
    Calculate a weighted score for an indexer based on multiple normalized metrics.

    This function computes a single score by combining multiple performance metrics,
    each weighted according to predefined weights. NaN values and missing metrics
    are treated as 0, but all weights contribute to the total score.

    Parameters:
    row (pandas.Series): A series containing normalized metric values for an indexer.
                         Expected to have columns prefixed with 'norm_'.
    weights (dict): A dictionary mapping metric names to their respective weights.
                    Keys should match the suffix of the 'norm_' columns in the row.

    Returns:
    float: The calculated weighted score.

    Raises:
    ValueError: If the total weight is 0.
    """
    weighted_sum = 0
    weight_total = 0
    missing_columns = []

    for metric, weight in weights.items():
        column_name = f"norm_{metric}"

        # Append any missing columns to the list
        if column_name not in row.index:
            missing_columns.append(column_name)
            continue

        value = row.get(column_name, np.nan)  # Uses np.nan if column is missing

        # So long as the column has a value that isn't nan, then:
        if not pd.isna(value):
            weighted_sum += value * weight
            weight_total += weight

    if missing_columns:
        logger.warning(f"Missing columns in input data: {', '.join(missing_columns)}")

    if weight_total == 0:
        logger.error(
            "Total sum of weights is 0. Sum of weights should be non-zero, ideally 1."
        )
        raise ValueError("Total weight cannot be 0.")

    return weighted_sum / weight_total
