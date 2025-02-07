import pandas as pd
import json
import pytz
from datetime import datetime, timedelta
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Constants
KUSTO_CLUSTER = os.getenv("KUSTO_CLUSTER") or ""
KUSTO_DATABASE = os.getenv("KUSTO_DATABASE") or ""
KUSTO_CLIENT_ID = os.getenv("KUSTO_CLIENT_ID") or ""
KUSTO_CLIENT_SECRET = os.getenv("KUSTO_CLIENT_SECRET") or ""
KUSTO_TENANT_ID = os.getenv("KUSTO_TENANT_ID") or ""
ALLOWED_TAG_NAMES = ["*"]  # For all tags
TIMEZONE = "Asia/Kuala_Lumpur"

# Kusto Client Setup
kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    KUSTO_CLUSTER, KUSTO_CLIENT_ID, KUSTO_CLIENT_SECRET, KUSTO_TENANT_ID
)
client = KustoClient(kcsb)

def get_query_results(start_datetime: str, end_datetime: str = "", timezone: str = "UTC") -> pd.DataFrame:
    tz = pytz.timezone(timezone)
    start_dt = datetime.fromisoformat(start_datetime).astimezone(tz)
    end_dt = datetime.fromisoformat(end_datetime).astimezone(tz) if end_datetime else datetime.now(tz)

    all_dfs = []  # Use list instead of repeatedly appending to DataFrame

    current_start = start_dt
    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=1), end_dt)
        query = f"""
        ["quill-city-mall-poc"]
        | where site == "quill-city-mall"
        | where dateTimeGenerated >= datetime({current_start.isoformat()}) 
        and dateTimeGenerated <= datetime({current_end.isoformat()})
        | order by dateTimeGenerated asc
        """
        try:
            response = client.execute(KUSTO_DATABASE, query)
            df = dataframe_from_result_table(response.primary_results[0])
            if not df.empty:
                df["dateTimeGenerated"] = pd.to_datetime(df["dateTimeGenerated"]).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                df["data"] = df["data"].apply(json.dumps)
                all_dfs.append(df)
        except Exception as e:
            print(f"Error fetching data for {current_start}: {e}")
        current_start = current_end

    # Concatenate all DataFrames at once
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

def convert_to_local_time(utc_time_str, timezone_str):
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_time = pytz.utc.localize(utc_time)
    return utc_time.astimezone(pytz.timezone(timezone_str))

def flatten_data(row, timezone_str):
    try:
        utc_date_time = row["dateTimeGenerated"]
        local_date_time = convert_to_local_time(utc_date_time, timezone_str)
        local_date = local_date_time.strftime("%Y-%m-%d")
        local_time = local_date_time.strftime("%H:%M:%S.%f")
        data_array = json.loads(row["data"])

        return [
            {
                "date": local_date,
                "time": local_time,
                "site": row["site"],
                "modbusAddress": item.get("modbusAddress"),
                "tagName": item.get("tagName", ""),
                "unit": item.get("unit"),
                "value": item.get("value"),
            }
            for item in data_array if "*" in ALLOWED_TAG_NAMES
        ]
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing row: {e}")
        return []

# Set starting and ending dates
start_datetime = "2025-01-14T00:00:00.000000+0800"
end_datetime = "2025-02-08T00:00:00.000000+0800"

data = get_query_results(start_datetime, end_datetime, TIMEZONE)

# Flatten the data
timezone_str = TIMEZONE
flattened_data = [
    flat_row
    for _, row in data.iterrows()
    for flat_row in flatten_data(row, timezone_str)
]

flattened_df = pd.DataFrame(flattened_data)

# Generate Excel filename
start_date = datetime.fromisoformat(start_datetime).strftime("%Y%m%d")
end_date = datetime.fromisoformat(end_datetime).strftime("%Y%m%d")
excel_file = f"flattened_data_{start_date}_{end_date}.xlsx"

# Write to Excel, grouped by date
with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
    grouped_data = flattened_df.groupby("date")
    for date, df in grouped_data:
        sheet_name = str(date)
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(flattened_df)
