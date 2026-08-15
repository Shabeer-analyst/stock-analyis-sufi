import io
import json
import os
import requests
import boto3
import pandas as pd
import consonants as con
from datetime import datetime


# =========================================================
# AWS CLIENTS
# =========================================================

s3_client = boto3.client("s3")
ssm = boto3.client("ssm")
sns = boto3.client("sns")


# =========================================================
# SSM PARAMETERS & S3 CONFIG
# =========================================================

github_api_url = ssm.get_parameter(
    Name=con.urlapi,
    WithDecryption=True
)["Parameter"]["Value"]

print("GitHub API URL:", github_api_url)

S3_BUCKET_NAME = os.environ.get(
    "S3_BUCKET_NAME",
    "stockdev-0416"
)


# =========================================================
# SNS SUCCESS NOTIFICATION
# =========================================================

def send_sns_success(csv_file_name, row_count, top_5_rows):
    success_sns_arn = ssm.get_parameter(
        Name=con.SUCCESSNOTIFICATIONARN,
        WithDecryption=True
    )["Parameter"]["Value"]

    env = ssm.get_parameter(
        Name=con.ENVIRONMENT,
        WithDecryption=True
    )["Parameter"]["Value"]

    component_name = con.COMPONENT_NAME
    success_msg = con.SUCCESS_MSG

    sns_message = (
        f"Hello,\n\n"
        f"{component_name} : {success_msg}\n\n"
        f"CSV File: {csv_file_name}\n"
        f"Total Number of Rows: {row_count}\n"
        f"S3 Location: s3://{S3_BUCKET_NAME}/{csv_file_name}\n\n"
        f"Top 5 Rows:\n"
        f"{top_5_rows}\n\n"
        f"Thanks"
    )

    print("SNS Success Message:")
    print(sns_message)

    response = sns.publish(
        TargetArn=success_sns_arn,
        Message=json.dumps({"default": sns_message}),
        Subject=env + " : " + component_name,
        MessageStructure="json"
    )

    print(f"SNS success notification sent. Message ID: {response['MessageId']}")
    return response


# =========================================================
# SNS ERROR NOTIFICATION
# =========================================================

def send_error_sns(msg):
    error_sns_arn = ssm.get_parameter(
        Name=con.ERRORNOTIFICATIONARN,
        WithDecryption=True
    )["Parameter"]["Value"]

    env = ssm.get_parameter(
        Name=con.ENVIRONMENT,
        WithDecryption=True
    )["Parameter"]["Value"]

    error_message = con.ERROR_MSG + msg
    component_name = con.COMPONENT_NAME

    sns_message = f"{component_name} : {error_message}"

    print("SNS Error Message:")
    print(sns_message)

    response = sns.publish(
        TargetArn=error_sns_arn,
        Message=json.dumps({"default": sns_message}),
        Subject=env + " : " + component_name,
        MessageStructure="json"
    )

    return response


# =========================================================
# LAMBDA HANDLER
# =========================================================

def lambda_handler(event, context):
    try:
        # Avoid colons in S3 file keys
        timestamp = datetime.now().strftime("%d-%m-%Y/stock_data_%H-%M-%S")
        S3_FILE_KEY = timestamp + ".csv"

        print("S3 File Key:", S3_FILE_KEY)

        # -----------------------------------------------------
        # GitHub API Request
        # -----------------------------------------------------
        headers = {
            "User-Agent": "AWS-Lambda-Stock-Analysis"
        }

        response = requests.get(
            github_api_url,
            headers=headers
        )
        response.raise_for_status()
        files = response.json()

        # -----------------------------------------------------
        # Find CSV Files
        # -----------------------------------------------------
        csv_files = [
            file["download_url"]
            for file in files
            if file["name"].endswith(".csv")
        ]

        if not csv_files:
            return {
                "statusCode": 400,
                "body": "No CSV files found in the repository."
            }

        # -----------------------------------------------------
        # Read Metadata File
        # -----------------------------------------------------
        metadata_url = csv_files.pop()
        d = pd.read_csv(metadata_url)

        if "Symbol" in d.columns:
            d["Symbol"] = d["Symbol"].astype(str).str.strip()

        # -----------------------------------------------------
        # Read Stock CSV Files
        # -----------------------------------------------------
        dataframes = []

        for url in csv_files:
            # FIX: Clean symbol cleanly without inserting an asterisk '*'
            file_name = (
                url.split("/")[-1]
                .replace(".csv", "")
                .strip()
            )

            df = pd.read_csv(url)
            df["Symbol"] = file_name
            dataframes.append(df)

        # -----------------------------------------------------
        # Combine & Merge
        # -----------------------------------------------------
        combined_df = pd.concat(dataframes, ignore_index=True)

        o_df = pd.merge(
            combined_df,
            d,
            on="Symbol",
            how="left"
        )

        # -----------------------------------------------------
        # Date Conversion & Filtering
        # -----------------------------------------------------
        o_df["timestamp"] = pd.to_datetime(o_df["timestamp"])

        filtered_df = o_df[
            (o_df["timestamp"] >= "2021-01-01") &
            (o_df["timestamp"] <= "2021-05-26")
        ]

        # -----------------------------------------------------
        # Aggregation
        # -----------------------------------------------------
        result_time = (
            filtered_df
            .groupby("Sector")
            .agg({
                "open": "mean",
                "close": "mean",
                "high": "max",
                "low": "min",
                "volume": "mean"
            })
            .reset_index()
        )

        # -----------------------------------------------------
        # Filter Sectors (Case-insensitive match)
        # -----------------------------------------------------
        list_sector = [
            "TECHNOLOGY",
            "FINANCE"
        ]

        result_time = result_time[
            result_time["Sector"].astype(str).str.upper().isin(list_sector)
        ].reset_index(drop=True)

        # -----------------------------------------------------
        # Rename Columns
        # -----------------------------------------------------
        result_time.columns = [
            "Sector",
            "sector_open_mean",
            "sector_close_mean",
            "sector_high",
            "sector_low",
            "sector_volume_mean"
        ]

        # -----------------------------------------------------
        # Output Generation
        # -----------------------------------------------------
        csv_buffer = io.StringIO()
        result_time.to_csv(
            csv_buffer,
            index=False,
            header=True
        )
        csv_content = csv_buffer.getvalue()

        row_count = len(result_time)
        print(f"Number of rows in the final CSV file: {row_count}")

        top_5_rows = result_time.head(5).to_string(index=False)

        # -----------------------------------------------------
        # Upload CSV to S3
        # -----------------------------------------------------
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_FILE_KEY,
            Body=csv_content,
            ContentType="text/csv"
        )

        print(f"File uploaded successfully to s3://{S3_BUCKET_NAME}/{S3_FILE_KEY}")

        # -----------------------------------------------------
        # Send Notification
        # -----------------------------------------------------
        send_sns_success(
            csv_file_name=S3_FILE_KEY,
            row_count=row_count,
            top_5_rows=top_5_rows
        )

        return {
            "statusCode": 200,
            "body": (
                f"Data processed successfully. Rows: {row_count}. "
                f"File uploaded to s3://{S3_BUCKET_NAME}/{S3_FILE_KEY} "
                f"and SNS notification sent."
            )
        }

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        try:
            send_error_sns(str(e))
        except Exception as sns_error:
            print(f"Failed to send error SNS notification: {str(sns_error)}")

        return {
            "statusCode": 500,
            "body": f"Error processing data: {str(e)}"
        }