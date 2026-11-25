from storage import s3_client, keys

s3 = s3_client()
for topic in ["cinema.events", "cinema.profile_changes"]:
    print(topic, len(keys(s3, "raw/" + topic + "/")))
