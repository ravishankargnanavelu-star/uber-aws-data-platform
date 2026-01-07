import psycopg2

def lambda_handler(event, context):
    conn = psycopg2.connect(
        host="redshift-cluster.endpoint",
        database="analytics",
        user="awsuser",
        password="password"
    )

    cursor = conn.cursor()

    cursor.execute("""
        COPY fact_trips
        FROM 's3://uber-data-lake-gold/fact_trips/'
        IAM_ROLE 'arn:aws:iam::123456789012:role/redshift-role'
        FORMAT AS PARQUET;
    """)

    conn.commit()
    cursor.close()
    conn.close()

    return {"status": "Redshift load complete"}
