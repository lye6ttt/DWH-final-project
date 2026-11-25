import time
from datetime import datetime, timezone
from confluent_kafka import Consumer
from storage import s3_client, create_bucket, BUCKET

s3 = s3_client()
while True:
    try:
        create_bucket(s3)
        break
    except Exception as error:
        print(error, flush=True)
        time.sleep(3)

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "cinema_archive",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})
consumer.subscribe(["cinema.events", "cinema.profile_changes"])

try:
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            print(msg.error(), flush=True)
            continue
        key = f"raw/{msg.topic()}/{msg.partition()}/{msg.offset():020d}.json"
        s3.put_object(
            Bucket=BUCKET, Key=key, Body=msg.value(),
            Metadata={"loaded-at": datetime.now(timezone.utc).isoformat()}
        )
        consumer.commit(message=msg, asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
