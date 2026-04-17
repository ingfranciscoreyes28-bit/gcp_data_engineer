from google.cloud import storage
from google.api_core.exceptions import Conflict
import sys

def create_bucket(bucket_name):
    client = storage.Client()

    # Verifica si existe
    if client.lookup_bucket(bucket_name):
        print(f"⚠️ El bucket '{bucket_name}' ya existe")
        return

    try:
        bucket = client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"

        new_bucket = client.create_bucket(bucket, location="US-CENTRAL1")
        print(f"✅ Bucket {new_bucket.name} creado")
    except Conflict:
        print(f"⚠️ El bucket '{bucket_name}' ya existe (global)")

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        name = sys.argv[1]
    else:
        name = input("Nombre del bucket: ").strip()

    create_bucket(name)