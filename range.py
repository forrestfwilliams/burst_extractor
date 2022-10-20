import boto3

# s3 = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = 'ffwilliams2-shenanigans'
data = 'bursts/S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.zip'
obj = s3.Object(bucket, data)
breakpoint()

# with open('test.zip', 'wb') as f:
    # s3.download_fileobj(bucket, data, f)
    # obj = s3.get_object(Bucket=bucket, Key=data, Range='Range: bytes=0-1000000000')
    # result = obj['Body'].read()
    # s3.download_fileobj(bucket, data, f, Range='Range: bytes=0-1000000000')
