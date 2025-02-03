import pandas as pd
import json
import pytz
from datetime import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from dotenv import load_dotenv
import os

load_dotenv()

KUSTO_CLUSTER = os.getenv("KUSTO_CLUSTER") or ""
KUSTO_DATABASE = os.getenv("KUSTO_DATABASE") or ""
KUSTO_CLIENT_ID = os.getenv("KUSTO_CLIENT_ID") or ""
KUSTO_CLIENT_SECRET = os.getenv("KUSTO_CLIENT_SECRET") or ""
KUSTO_TENANT_ID = os.getenv("KUSTO_TENANT_ID") or ""

# Set up the kusto client
kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    KUSTO_CLUSTER, KUSTO_CLIENT_ID, KUSTO_CLIENT_SECRET, KUSTO_TENANT_ID
)
client = KustoClient(kcsb)

ALLOWED_TAG_NAMES = [
    "saved-energy"
]  # Use ["*"] for all tags, or ["Temperature", "Humidity"] for specific tags


def get_query_results(
    start_datetime: str, end_datetime: str = "", timezone: str = "UTC"
) -> pd.DataFrame:
    """
    Execute the KQL query with the specified start and end datetime.

    Args:
        start_datetime (str): The start datetime in ISO 8601 format.
        end_datetime (str): The end datetime in ISO 8601 format, if this is not
        provided then use now().

    Returns:
        pd.DataFrame: Query results as a Pandas DataFrame.
    """
    tz = pytz.timezone(timezone)

    start_datetime = datetime.fromisoformat(start_datetime).astimezone(tz).isoformat()

    if not end_datetime:
        end_datetime = datetime.now(tz).isoformat()
    else:
        end_datetime = datetime.fromisoformat(end_datetime).astimezone(tz).isoformat()

    query = f"""
    ["quill-city-mall-poc"]
    | where site == "quill-city-mall"
    | where dateTimeGenerated >= datetime({start_datetime}) and dateTimeGenerated <= datetime({end_datetime})
    | order by dateTimeGenerated asc
    """
    response = client.execute(KUSTO_DATABASE, query)
    df = dataframe_from_result_table(response.primary_results[0])
    df["dateTimeGenerated"] = pd.to_datetime(df["dateTimeGenerated"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    df["data"] = df["data"].apply(lambda x: json.dumps(x))
    return df


# Read the data into a DataFrame
start_datetime = "2025-01-26T00:00:00.000000+0800"
end_datetime = "2025-02-01T00:00:00.000000+0800"
data = get_query_results(start_datetime, end_datetime, "Asia/Kuala_Lumpur")
# print(data)
# data = pd.read_csv("export (3).csv")
# print(data)


# Function to convert UTC to local time
def convert_to_local_time(utc_time_str, timezone_str):
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%f")
    utc_time = pytz.utc.localize(utc_time)
    local_time = utc_time.astimezone(pytz.timezone(timezone_str))
    return local_time


# Function to flatten the data
def flatten_data(row, timezone_str):
    try:
        utc_date_time = row["dateTimeGenerated"][:-2] + "Z"
        local_date_time = convert_to_local_time(utc_date_time.rstrip("Z"), timezone_str)
        local_date = local_date_time.strftime("%Y-%m-%d")
        local_time = local_date_time.strftime("%H:%M:%S.%f")
        site = row["site"]
        data_array = json.loads(row["data"])

        flattened_rows = []
        for item in data_array:
            tag_name = item.get("tagName", "")
            # Include all tags if ALLOWED_TAG_NAMES contains "*", otherwise filter
            if "*" in ALLOWED_TAG_NAMES or any(
                pattern.lower() in tag_name.lower() for pattern in ALLOWED_TAG_NAMES
            ):
                flattened_row = {
                    "date": local_date,
                    "time": local_time,
                    "site": site,
                    "modbusAddress": item.get("modbusAddress"),
                    "tagName": tag_name,
                    "unit": item.get("unit"),
                    "value": item.get("value"),
                }
                flattened_rows.append(flattened_row)
        return flattened_rows
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing row: {e}")
        return []


# Flatten all rows
timezone_str = "Asia/Kuala_Lumpur"
flattened_data = [
    flattened_row
    for _, row in data.iterrows()
    for flattened_row in flatten_data(row, timezone_str)
]

# Create a new DataFrame with the flattened data
flattened_df = pd.DataFrame(flattened_data)

# Convert to xlsx
# Extract dates from start_datetime and end_datetime
start_date = datetime.fromisoformat(start_datetime).strftime("%Y%m%d")
end_date = datetime.fromisoformat(end_datetime).strftime("%Y%m%d")
flattened_df.to_excel(f"flattened_data_{start_date}_{end_date}.xlsx", index=False)

# Save to CSV (optional)
# output_csv = "flattened_data.csv"
# flattened_df.to_csv(output_csv, index=False)

# Display the flattened data
print(flattened_df)
