import sys
from storage import s3_client, create_bucket, BUCKET

s3 = s3_client()
create_bucket(s3)
s3.upload_file(sys.argv[1], BUCKET, "catalogue/catalogue.jsonl")
print("Каталог загружен")
