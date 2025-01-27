import boto3
from botocore import UNSIGNED
from botocore.client import Config

# Kimlik doğrulama olmadan S3 istemcisi oluşturma
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# Bucket adı ve yol
bucket_name = "overturemaps-us-west-2"
prefix = "release/2024-12-18.0/theme=addresses/type=address/"

# Dizin içeriğini listeleme
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if 'Contents' in response:
    print("Dosyalar:")
    for obj in response['Contents']:
        print(f"- {obj['Key']}")
else:
    print("Dizin boş veya erişim yetkisi yok.")
