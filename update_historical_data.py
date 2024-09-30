import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import schedule
import time

# URLs
height_url = "https://www.cdc.gov/growthcharts/html_charts/lenageinf.htm#males"
weight_url = "https://www.cdc.gov/growthcharts/html_charts/wtageinf.htm"
temp_url = (
    "https://www.advil.ca/resources/childrens-fever-pain/children-s-temperature-chart/"
)
api_base_url = (
    "https://smartcribmainapp-djdsaff9a0cqcvh2.eastus-01.azurewebsites.net/api"
)


# Function to fetch data from a URL
def fetch_data(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print(f"Successfully fetched data from {url}")
        return response.text
    else:
        print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        return None


# Function to get height data from URL and create a dataframe
def height_dataframe(height_url):
    dataframes = []
    web_content = fetch_data(height_url)
    soup = BeautifulSoup(web_content, "html.parser")
    tables = soup.find_all("table", class_="table")

    for tab in tables:
        table_data = []
        headers = []
        # Extract table headers
        for header in tab.find_all("th", scope="col"):
            headers.append(header.text)

        # Extract table rows
        for row in tab.find_all("tr"):
            row_data = []
            th = row.find("th", scope="row")
            if th:
                row_data.append(th.text)
            for td in row.find_all("td"):
                row_data.append(td.text)
            if row_data:
                table_data.append(row_data)

        dataframe = pd.DataFrame(table_data, columns=headers)[
            ["Age (in months)", "50th Percentile Length (in centimeters)"]
        ]
        dataframe.columns = ["Age(months)", "Height(cm)"]
        dataframes.append(dataframe)

    print("Height Dataframe:")
    for df in dataframes:
        print(df)

    return dataframes


# Function to get weight data from URL and create a dataframe
def weight_dataframe(weight_url):
    dataframes = []
    web_content = fetch_data(weight_url)
    soup = BeautifulSoup(web_content, "html.parser")
    tables = soup.find_all("table", class_="table")

    for tab in tables:
        table_data = []
        headers = []
        # Extract table headers
        for header in tab.find_all("th", scope="col"):
            headers.append(header.text)

        # Extract table rows
        for row in tab.find_all("tr"):
            row_data = []
            th = row.find("th", scope="row")
            if th:
                row_data.append(th.text)
            for td in row.find_all("td"):
                row_data.append(td.text)
            if row_data:
                table_data.append(row_data)

        dataframe = pd.DataFrame(table_data, columns=headers)[
            ["Age (in months)", "50th Percentile Weight (in kilograms)"]
        ]
        dataframe.columns = ["Age(months)", "Weight(Kg)"]
        dataframes.append(dataframe)

    print("Weight Dataframe:")
    for df in dataframes:
        print(df)

    return dataframes


# Function to get temperature data from URL and create a dataframe
def temp_dataframe(temp_url):
    dataframes = []
    web_content = fetch_data(temp_url)
    soup = BeautifulSoup(web_content, "html.parser")
    tables = soup.find_all("table")

    for tab in tables:
        table_data = []
        headers = []
        # Extract table headers
        for header in tab.find_all("th"):
            headers.append(header.text)

        # Extract table rows
        for row in tab.find_all("tr"):
            row_data = []
            for td in row.find_all("td"):
                row_data.append(td.text)

            if row_data:
                table_data.append(row_data)

        dataframe = pd.DataFrame(table_data, columns=headers)
        dataframes.append(dataframe)

    print("Temperature Dataframe:")
    for df in dataframes:
        print(df)

    return dataframes


# Helper function to update or create records
def update_or_create_record(endpoint, records, key_field):
    # Fetch existing records (with /withoutJWT appended to the endpoint)
    response = requests.get(f"{api_base_url}/{endpoint}/withoutJWT/")
    if response.status_code != 200:
        print(
            f"Failed to fetch existing {endpoint} records. Status code: {response.status_code}"
        )
        return

    existing_records = response.json()
    existing_records_dict = {
        str(record[key_field]): record for record in existing_records
    }

    for record in records:
        avg_market_age = str(record[key_field])
        url = f"{api_base_url}/{endpoint}/withoutJWT/"
        if avg_market_age in existing_records_dict:

            response = requests.put(url, json=record)
            if response.status_code in [200, 201]:
                print(
                    f"{endpoint.capitalize()} data for avgMarketAge {avg_market_age} successfully updated"
                )
            else:
                print(
                    f"Failed to update {endpoint} data for avgMarketAge {avg_market_age}. Status code: {response.status_code}"
                )
                print("Response:", response.text)
        else:
            # Create new record
            response = requests.post(url, json=record)
            if response.status_code == 201:
                print(
                    f"{endpoint.capitalize()} data for avgMarketAge {avg_market_age} successfully created"
                )
            else:
                print(
                    f"Failed to create {endpoint} data for avgMarketAge {avg_market_age}. Status code: {response.status_code}"
                )
                print("Response:", response.text)


# Update height, weight, and temperature records
def update_data():
    # Get height dictionary
    baby_height_df = pd.merge(
        height_dataframe(height_url)[0],
        height_dataframe(height_url)[1],
        on="Age(months)",
    )
    baby_height_df.columns = ["avgMarketAge", "avgHeightMale", "avgHeightFemale"]
    baby_height_data = baby_height_df.to_dict(orient="records")

    print("Baby Height Data:")
    print(baby_height_data)

    # Get weight dictionary
    baby_weight_df = pd.merge(
        weight_dataframe(weight_url)[0],
        weight_dataframe(weight_url)[1],
        on="Age(months)",
    )
    baby_weight_df.columns = [
        "avgMarketAge",
        "maleTop50PercentileWeight",
        "femaleTop50PercentileWeight",
    ]
    baby_weight_data = baby_weight_df.to_dict(orient="records")

    print("Baby Weight Data:")
    print(baby_weight_data)

    # Get temperature dictionary
    def calculate_mean(temp_range):
        if pd.isna(temp_range):
            return None
        # Extract the °C range
        celsius_range = temp_range.split(" ")[0]
        temps = celsius_range.replace("°C", "").split("–")
        temps = [float(temp) for temp in temps]
        return sum(temps) / len(temps)

    baby_temp_df = pd.concat(temp_dataframe(temp_url))

    baby_temp_df["Normal Ear Temperature"] = baby_temp_df[
        "Normal Ear Temperature"
    ].apply(calculate_mean)
    baby_temp_df["Normal Oral Temperature"] = baby_temp_df[
        "Normal Oral Temperature"
    ].apply(calculate_mean)

    baby_temp_df["Normal Ear Temperature"] = baby_temp_df[
        "Normal Ear Temperature"
    ].fillna(0)
    baby_temp_df["Normal Oral Temperature"] = baby_temp_df[
        "Normal Oral Temperature"
    ].fillna(0)

    mean_temperature = (
        baby_temp_df["Normal Ear Temperature"].sum()
        + baby_temp_df["Normal Oral Temperature"].sum()
    ) / 2

    baby_temp_data = {"avgMarketAge": "12", "avgMarketTemperature": mean_temperature}

    print("Baby Temperature Data:")
    print(baby_temp_data)

    update_or_create_record("length/historical", baby_height_data, "avgMarketAge")
    update_or_create_record("weight/historical", baby_weight_data, "avgMarketAge")
    update_or_create_record("temp/historical", [baby_temp_data], "avgMarketAge")


# Initial data update
update_data()

# Schedule the job to update data every 21 days
schedule.every(21).days.do(update_data)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)
