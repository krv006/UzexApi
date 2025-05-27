from minio import Minio

client = Minio(
    "minio-cdn.uzex.uz",
    access_key="cotton",
    secret_key="xV&q+8AHHXBSK}",
    secure=True
)

try:
    objects = client.list_objects("cotton", recursive=True)
    for obj in objects:
        print(f"📄 {obj.object_name}")
except Exception as e:
    print(f" errors {e}")
