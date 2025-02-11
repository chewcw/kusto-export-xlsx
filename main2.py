import json
import os
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pytz
import requests
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Load environment variables
load_dotenv()

# Constants
KUSTO_CLUSTER = os.getenv("KUSTO_CLUSTER") or ""
KUSTO_DATABASE = os.getenv("KUSTO_DATABASE") or ""
KUSTO_CLIENT_ID = os.getenv("KUSTO_CLIENT_ID") or ""
KUSTO_CLIENT_SECRET = os.getenv("KUSTO_CLIENT_SECRET") or ""
KUSTO_TENANT_ID = os.getenv("KUSTO_TENANT_ID") or ""
USER_EMAIL = os.getenv("USER_EMAIL") or ""
USER_PASSWORD = os.getenv("USER_PASSWORD") or ""
OFFICE365_ONEDRIVE_FOLDER = os.getenv("OFFICE365_ONEDRIVE_FOLDER") or ""
ALLOWED_TAG_NAMES = ["*"]  # For all tags
TIMEZONE = "Asia/Kuala_Lumpur"

# Kusto Client Setup
kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    KUSTO_CLUSTER, KUSTO_CLIENT_ID, KUSTO_CLIENT_SECRET, KUSTO_TENANT_ID
)
client = KustoClient(kcsb)


def get_query_results(
    start_datetime: str, end_datetime: str = "", timezone: str = "UTC"
) -> pd.DataFrame:
    tz = pytz.timezone(timezone)
    start_dt = datetime.fromisoformat(start_datetime).astimezone(tz)
    end_dt = (
        datetime.fromisoformat(end_datetime).astimezone(tz)
        if end_datetime
        else datetime.now(tz)
    )

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
                df["dateTimeGenerated"] = pd.to_datetime(
                    df["dateTimeGenerated"]
                ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
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
            for item in data_array
            if "*" in ALLOWED_TAG_NAMES
            or any(
                tag.lower() in item.get("tagName", "").lower()
                for tag in ALLOWED_TAG_NAMES
            )
        ]
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing row: {e}")
        return []


# Upload the Excel file to OneDrive
def upload_to_onedrive(excel_file: str):
    # Get credentials from environment variables to upload to OneDrive
    client_id = os.getenv("OFFICE365_CLIENT_ID")
    client_secret = os.getenv("OFFICE365_CLIENT_SECRET")
    tenant_id = os.getenv("OFFICE365_TENANT_ID")
    redirect_uri = os.getenv("OFFICE365_REDIRECT_URI")

    if not client_id or not client_secret or not tenant_id or not redirect_uri:
        raise ValueError(
            "OFFICE365_CLIENT_ID, OFFICE365_CLIENT_SECRET, OFFICE365_TENANT_ID, and OFFICE365_REDIRECT_URI must be set in environment variables"
        )

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )

    # Get the authorization URL
    auth_url = app.get_authorization_request_url(scopes, redirect_uri=redirect_uri)
    # print(f"Please go to this URL and authorize the application: {auth_url}")

    # Use Selenium to automate the browser
    driver = webdriver.Chrome()

    driver.get(auth_url)

    # Wait until the username input element is present
    username_input = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.NAME, "loginfmt"))
    )
    # Enter username
    username_input.send_keys(USER_EMAIL)
    username_input.send_keys(Keys.RETURN)

    # Wait for the next page to load and the password input element to be present
    password_input = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.NAME, "passwd"))
    )
    # Enter password
    password_input.send_keys(USER_PASSWORD)

    # Wait for the submit button to be clickable and click it
    submit_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "idSIButton9"))
    )
    submit_button.click()

    # two-factor authentication
    WebDriverWait(driver, 60).until(EC.url_changes(driver.current_url))

    always_signin_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "idSIButton9"))
    )
    always_signin_button.click()

    time.sleep(2)

    # Get the authorization code from the redirected URL
    current_url = driver.current_url

    # Close the browser
    driver.quit()

    # Extract the authorization code from the URL
    parsed_url = urlparse(current_url)
    auth_code = parse_qs(parsed_url.query).get("code", [None])[0]

    if not auth_code:
        raise ValueError("Failed to obtain authorization code from the URL")

    # Acquire access token by authorization code
    result = app.acquire_token_by_authorization_code(
        auth_code, scopes=scopes, redirect_uri=redirect_uri
    )
    if "access_token" in result:
        access_token = result["access_token"]
        upload_url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{OFFICE365_ONEDRIVE_FOLDER}/{excel_file}:/content"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }

        try:
            with open(excel_file, "rb") as file_content:
                response = requests.put(upload_url, headers=headers, data=file_content)
            if response.status_code in (200, 201):
                print("File successfully uploaded to OneDrive.")
            else:
                print(f"Upload failed: {response.status_code} - {response.json()}")
        except Exception as e:
            print(f"Exception during file upload: {e}")
    else:
        print("Failed to obtain access token:", result.get("error_description"))


# -----------------------------------------------------------------------------

# Get today at midnight (00:00:00)
today_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
# Get yesterday at midnight
yesterday_midnight = today_midnight - timedelta(days=1)
# Format with timezone (+0800)
start_datetime = yesterday_midnight.strftime("%Y-%m-%dT%H:%M:%S.000000+0800")
end_datetime = today_midnight.strftime("%Y-%m-%dT%H:%M:%S.000000+0800")

# Manually set starting and ending dates
# start_datetime = "2025-02-09T00:00:00.000000+0800"
# end_datetime = "2025-02-11T00:00:00.000000+0800"

# -----------------------------------------------------------------------------

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

# Upload the Excel file to OneDrive
upload_to_onedrive(excel_file)

print(flattened_df)
