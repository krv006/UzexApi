from minio import Minio

client = Minio(
    "minio-cdn.uzex.uz",
    access_key="cotton",
    secret_key="xV&q+8AHHXBSK}",
    secure=True
)

bucket_name = "uzex-files"  # <<< BU YERGA HAQIQIY BUCKET NOMINI QO‘Y
object_name = "Templates/c0f74eba-afdf-449f-0e67-08dd96cf2567/2025/05/19/30dc2da13f9c478e84cf60a95660f93e"
# object_name1 = "Records/13b5db46-3cae-4b30-9864-c28f368e0eb4/2025.05.23/51fd9e1028b746afbb9b3319504a63b5"
download_path = "file_from_minio.xlsx"

try:
    client.fget_object(bucket_name, object_name, download_path)
    print(f"✅ Fayl yuklab olindi: {download_path}")
except Exception as e:
    print(f"❌ Xatolik: {e}")
