CREATE TABLE fact_trips (
  trip_id VARCHAR(50),
  driver_id VARCHAR(50),
  city VARCHAR(50),
  fare DECIMAL(10,2),
  distance FLOAT,
  event_ts TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (city)
SORTKEY (event_ts);
