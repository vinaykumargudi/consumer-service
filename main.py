
import os
import logging
import json
import asyncio
from kafka import KafkaConsumer
from fastapi import FastAPI, WebSocket
import boto3
import pandas as pd
import numpy as np
from io import StringIO
from collections import defaultdict
from asyncio import Queue
# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# AWS Credentials
AWS_ACCESS_KEY_ID = "AKIAUNQ3Y2OW2P7FI2UM"
AWS_SECRET_ACCESS_KEY = "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"
AWS_DEFAULT_REGION = "us-east-1"

os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
os.environ["AWS_DEFAULT_REGION"] = AWS_DEFAULT_REGION

# Kafka Configuration
KAFKA_TOPIC = "stock_transactions"
KAFKA_USERNAME = "doadmin"
#CA_CERT_PATH = "/Users/gkumar/producer-service/crt.pem"
CA_CERT_PATH = "/app/crt.pem"
KAFKA_PASSWORD = "AVNS_D6sGVMQYojYg6VUSdZE"
KAFKA_BROKER = "db-kafka-nyc3-35655-do-user-23722714-0.j.db.ondigitalocean.com:25073"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id="kafka-consumer-group",
    security_protocol="SASL_SSL",
    ssl_cafile=CA_CERT_PATH,
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

# # FastAPI WebSocket setup
app = FastAPI()
connected_clients = {}
user_message_buffer = defaultdict(list)

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await websocket.accept()
    connected_clients[user_id] = websocket
    if user_id in user_message_buffer:
        for message in user_message_buffer[user_id]:
            await websocket.send_text(message)
        del user_message_buffer[user_id]
    try:
        while True:
            await websocket.receive_text()
    except Exception as e:
        connected_clients.pop(user_id, None)
        logger.error(f"WebSocket connection for user {user_id} closed: {e}")

async def send_message_to_user(user_id: str, data):
    json_data = json.dumps(data)
    if user_id in connected_clients:
        await connected_clients[user_id].send_text(json_data)
    else:
        user_message_buffer[user_id].append(json_data)

async def consume_kafka_messages():
    while True:
        try:
            messages = consumer.poll(timeout_ms=3000)
            if not messages:
                await asyncio.sleep(1)
                continue
            for _, records in messages.items():
                for msg in records:
                    message = msg.value.decode("utf-8")
                    logger.info(f"Received Kafka message: {message}")
                    data = json.loads(message)
                    user_id = data.get("user_id")
                    await process_message(data, user_id)
        except Exception as e:
            logger.error(f"Error consuming Kafka message: {e}")
            await asyncio.sleep(5)



async def process_message(data, user_id):
    s3_file_location = data.get("s3_file_location")
    if s3_file_location:
        bucket_name, object_name = s3_file_location.split("/", 1)
        try:
            response = boto3.client("s3").get_object(Bucket=bucket_name, Key=object_name)
            csv_data = response["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_data))
            total_rows = len(df)
            chunk_size = max(1, total_rows // 20)
            for i in range(0, total_rows, chunk_size):
                    chunk = df.iloc[i:i + chunk_size]
                    summary, monthly = await process_data(chunk, data, user_id)
                    # Send progress and data to the user
                    await send_message_to_user(user_id, {
                            "progress": f"{(i + chunk_size) / total_rows * 100:.1f}% completed",
                            "summary": summary.to_dict(),
                        })

                    await asyncio.sleep(5) 
            df = pd.read_csv(StringIO(csv_data))
            summary_df , monthly_df= await process_data(df, data, user_id)
            await upload_processed_data(user_id, df, summary_df, monthly_df)

        except Exception as e:
            error_message = f"Error fetching file from S3 or processing the file websockets: {str(e)}"
            logger.error(error_message)
            await upload_error_log(user_id, error_message)

async def process_data(df, data, user_id):
    #df = pd.read_csv(StringIO(csv_data))
    #total_rows = len(df)
    #chunk_size = max(1, total_rows // 20)  # 5% chunks to show progress
    #print(len(df))
    try:
        # Rename columns dynamically
        df.rename(columns={
            data["buy_price"]: "buy_price",
            data["sell_price"]: "sell_price",
            data["quantity"]: "quantity",
            data["buy_date"]: "buy_date",
            data["sell_date"]: "sell_date",
            data["company"]: "company"
        }, inplace=True)
        print(data)
        if data.get("Stocks")==["all"]:
            print("alsjdnvosdbdhsiobvdshvdsiobvhdsiobvdsbviods")
            pass
        else: 

            # Filter the dataframe to only keep rows where the 'company' column value is in the provided list
            companies_to_keep = data.get("Stocks", [])  # This should be a list of companies
            
            # Filter the DataFrame based on the 'company' column
            df = df[df['company'].isin(companies_to_keep)]
            print(len(df))

                # Convert date columns to datetime
        df["buy_date"] = pd.to_datetime(df["buy_date"])
        df["sell_date"] = pd.to_datetime(df["sell_date"])

        # Extract year, month, and day for Buying & sell_dates
        df["Buy Year"] = df["buy_date"].dt.year
        df["Buy Month"] = df["buy_date"].dt.month
        df["Sell Year"] = df["sell_date"].dt.year
        df["Sell Month"] = df["sell_date"].dt.month

        # Calculate trade duration
        df["Time in Trade Days"] = (df["sell_date"] - df["buy_date"]).dt.days
        df["Time in Trade Months"] = df["Time in Trade Days"] / 30
        df["Time in Trade Years"] = df["Time in Trade Days"] / 365

        # Calculate invested amount
        df["Invested Amount"] = df["buy_price"] * df["quantity"]

        # Calculate profit/loss
        df["Profit/Loss"] = (df["sell_price"] * df["quantity"]) - df["Invested Amount"]
        df["Profit/Loss Percent"] = ((df["Profit/Loss"] / df["Invested Amount"]) * 100).round(2)

        # Aggregate profit/loss by day, month, and year
        daily_profit_loss = df.groupby("sell_date")["Profit/Loss"].sum()
        monthly_profit_loss = df.groupby(["Sell Year", "Sell Month"])["Profit/Loss"].sum()
        yearly_profit_loss = df.groupby("Sell Year")["Profit/Loss"].sum()

        daily_profit_loss_percent = df.groupby("sell_date")["Profit/Loss Percent"].sum()
        monthly_profit_loss_percent = df.groupby(["Sell Year", "Sell Month"])["Profit/Loss Percent"].sum()
        yearly_profit_loss_percent = df.groupby("Sell Year")["Profit/Loss Percent"].sum()

        # Calculate max drawdown (largest peak-to-trough drop in cumulative profit)
        cumulative_profit = daily_profit_loss.cumsum()
        rolling_max = cumulative_profit.cummax()
        drawdown = rolling_max - cumulative_profit
        max_drawdown = drawdown.max()
        max_drawdown_percent = (max_drawdown / rolling_max.max() * 100) if rolling_max.max() > 0 else 0

        # Calculate max drawdown recovery days
        recovery_index = np.where(drawdown == max_drawdown)[0]
        if len(recovery_index) > 0:
            recovery_start = recovery_index[0]
            recovery_end = np.where(cumulative_profit[recovery_start:] >= rolling_max[recovery_start])[0]
            max_drawdown_recovery_days = recovery_end[0] if len(recovery_end) > 0 else None
        else:
            max_drawdown_recovery_days = None

        # Calculate expectancy (Expected value per trade)
        win_trades = df[df["Profit/Loss"] > 0]
        loss_trades = df[df["Profit/Loss"] < 0]

        expectancy = (win_trades["Profit/Loss"].mean() * win_trades.shape[0] + loss_trades["Profit/Loss"].mean() * loss_trades.shape[0]) / df.shape[0]

        # Average profit on winning days, months, and years
        avg_profit_on_win_days = round(daily_profit_loss[daily_profit_loss > 0].mean(), 2)
        avg_profit_on_win_months = round(monthly_profit_loss[monthly_profit_loss > 0].mean(), 2)
        avg_profit_on_win_years = round(yearly_profit_loss[yearly_profit_loss > 0].mean(), 2)

        # Average loss on losing days, months, and years
        avg_loss_on_loss_days = round(daily_profit_loss[daily_profit_loss < 0].mean(), 2)
        avg_loss_on_loss_months = round(monthly_profit_loss[monthly_profit_loss < 0].mean(), 2)
        avg_loss_on_loss_years = round(yearly_profit_loss[yearly_profit_loss < 0].mean(), 2)

        # Compute summary metrics
        summary = {
            "stocks": ", ".join(data.get("Stocks")),
            "Total Money Invested": round(df["Invested Amount"].sum(), 2),
            "Avg Money Invested per Trade": round(df["Invested Amount"].mean(), 2),
            "Total Profit/Loss": round(df["Profit/Loss"].sum(), 2),
            "Total Profit/Loss Percent": round(df["Profit/Loss Percent"].sum(), 2),
            "Max Profit": round(df["Profit/Loss"].max(), 2),
            "Max Loss": round(df["Profit/Loss"].min(), 2),
            "Max Profit Percent": round(df["Profit/Loss Percent"].max(), 2),
            "Max Loss Percent": round(df["Profit/Loss Percent"].min(), 2),
            "Avg Trade Time (Days)": round(df["Time in Trade Days"].mean(), 2),
            "Avg Trade Time (Months)": round(df["Time in Trade Months"].mean(), 2),
            "Avg Trade Time (Years)": round(df["Time in Trade Years"].mean(), 2),
            "Avg Profit/Loss per Day": round(daily_profit_loss.mean(), 2),
            "Avg Profit/Loss per Month": round(monthly_profit_loss.mean(), 2),
            "Avg Profit/Loss per Year": round(yearly_profit_loss.mean(), 2),
            "Avg Profit/Loss Percent per Day": round(daily_profit_loss_percent.mean(), 2),
            "Avg Profit/Loss Percent per Month": round(monthly_profit_loss_percent.mean(), 2),
            "Avg Profit/Loss Percent per Year": round(yearly_profit_loss_percent.mean(), 2),
            "Max Drawdown": round(max_drawdown, 2),
            "Max Drawdown Percent": round(max_drawdown_percent, 2),
            "Max Drawdown Recovery Days": max_drawdown_recovery_days,
            "Expectancy (Avg Expected Profit per Trade)": round(expectancy, 2),
            "Avg Profit on Win Days": avg_profit_on_win_days,
            "Avg Profit on Win Months": avg_profit_on_win_months,
            "Avg Profit on Win Years": avg_profit_on_win_years,
            "Avg Loss on Loss Days": avg_loss_on_loss_days,
            "Avg Loss on Loss Months": avg_loss_on_loss_months,
            "Avg Loss on Loss Years": avg_loss_on_loss_years,
        }

        summary_df = pd.DataFrame([summary])


        # Aggregate profit/loss percentage by month and year
        monthly_profit_loss_percent = df.groupby(["Sell Year", "Sell Month"]) ["Profit/Loss Percent"].sum().reset_index()
        monthly_profit_loss_pivot = monthly_profit_loss_percent.pivot(index="Sell Year", columns="Sell Month", values="Profit/Loss Percent").fillna(0)
        monthly_profit_loss_pivot.columns = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        monthly_profit_loss_pivot.reset_index(inplace=True)


        print("\n📊 Monthly Profit/Loss Percentage by Year:")
        print(monthly_profit_loss_pivot)

        # df["profit"] = (df["sell_price"] - df["buy_price"]) * df["quantity"]
        # df["invested"] = df["buy_price"] * df["quantity"]
        # df["date"] = pd.to_datetime(df["date"], errors='coerce')
        # df.dropna(subset=["date"], inplace=True)
        # df["year"] = df["date"].dt.year
        # df["month"] = df["date"].dt.month
        # df["weekday"] = df["date"].dt.day_name()

        # Allow WebSocket messages to be processed

        # Upload all dataframes to S3
        #await upload_processed_data(user_id, df, summary_df, monthly_profit_loss_pivot)
        
    except Exception as e:
        error_message = f"Processing error: {str(e)}"
        logger.error(error_message)
        await upload_error_log(user_id, error_message)
    
    return summary_df, monthly_profit_loss_pivot

async def delete_s3_folder(user_id):
    try:
        # List all objects within the user's folder
        s3_client = boto3.client("s3")
        result = s3_client.list_objects_v2(Bucket="publicpayvin", Prefix=f"{user_id}/")
        
        if "Contents" in result:
            # Delete all objects in the folder
            delete_objects = {"Objects": [{"Key": obj["Key"]} for obj in result["Contents"]]}
            s3_client.delete_objects(Bucket="publicpayvin", Delete=delete_objects)
            logger.info(f"Deleted existing folder for user {user_id}")
        else:
            logger.info(f"No existing folder found for user {user_id}")
    
    except Exception as e:
        logger.error(f"Error deleting folder for user {user_id}: {str(e)}")

async def upload_processed_data(user_id, df, summary_df, monthly_profit_loss_pivot):
    try:
        # Delete existing folder if present
        await delete_s3_folder(user_id)

        # Convert and upload processed data (df)
        processed_csv = StringIO()
        df.to_csv(processed_csv, index=False)
        processed_csv.seek(0)
        processed_s3_path = f"{user_id}/processed_data.csv"
        boto3.client("s3").put_object(
            Bucket="publicpayvin", Key=processed_s3_path, Body=processed_csv.getvalue()
        )

        # Upload summary_df
        summary_csv = StringIO()
        summary_df.to_csv(summary_csv, index=False)
        summary_csv.seek(0)
        summary_s3_path = f"{user_id}/summary_data.csv"
        boto3.client("s3").put_object(
            Bucket="publicpayvin", Key=summary_s3_path, Body=summary_csv.getvalue()
        )

        # Upload monthly_profit_loss_pivot profit data
        monthly_profit_loss_pivot_csv = StringIO()
        monthly_profit_loss_pivot.to_csv(monthly_profit_loss_pivot_csv)
        monthly_profit_loss_pivot_csv.seek(0)
        monthly_profit_loss_pivot_s3_path = f"{user_id}/monthly_profit_loss_pivot_profit.csv"
        boto3.client("s3").put_object(
            Bucket="payvin", Key=monthly_profit_loss_pivot_s3_path, Body=monthly_profit_loss_pivot_csv.getvalue()
        )



        logger.info(f"Successfully uploaded all data for user {user_id}.")

    except Exception as e:
        error_message = f"Error uploading processed data for user {user_id}: {str(e)}"
        logger.error(error_message)
        await upload_error_log(user_id, error_message)

async def upload_error_log(user_id, error_message):
    try:
        error_log = StringIO()
        error_log.write(error_message)
        error_log.seek(0)
        error_log_path = f"{user_id}/error_log.txt"
        boto3.client("s3").put_object(
            Bucket="payvin", Key=error_log_path, Body=error_log.getvalue()
        )
        logger.info(f"Error log uploaded for user {user_id}.")
    except Exception as e:
        logger.error(f"Failed to upload error log for user {user_id}: {str(e)}")

# Start the Kafka message consumer in the background when the FastAPI app starts

# Start Kafka consumer in a separate thread


@app.on_event("startup")
async def startup():
    asyncio.create_task(consume_kafka_messages())



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

