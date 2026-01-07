import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

BRONZE_BUCKET = "uber-data-lake-bronze"
ACTIVE_TRIPS_TABLE = "active_trips"

table = dynamodb.Table(ACTIVE_TRIPS_TABLE)

def lambda_handler(event, context):
    body = json.loads(event["body"])

    body["ingestion_id"] = str(uuid.uuid4())
    body["ingested_at"] = datetime.utcnow().isoformat()

    date = body["ingested_at"][:10]
    hour = body["ingested_at"][11:13]

    s3_key = f"trips/date={date}/hour={hour}/{body['ingestion_id']}.json"

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=json.dumps(body)
    )

    table.put_item(
        Item={
            "trip_id": body["trip_id"],
            "driver_id": body["driver_id"],
            "status": body["status"],
            "city": body["city"],
            "updated_at": body["ingested_at"]
        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Event ingested"})
    }
