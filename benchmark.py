import time

import boto3

# Run on a r5d.xlarge EC2 instance in the same region as the S3 bucket (us-west-2)
# Public access is enabled on the bucket
if __name__ == '__main__':
    bucket = 'ffwilliams2-shenanigans'
    key = 'bursts/swath.tif'


    # Direct GET (completes in 39s)
    start = time.time()
    s3 = boto3.client('s3')
    with open('get.tif', 'wb') as f:
        response = s3.get_object(Bucket=bucket, Key=key)
        f.write(response['Body'].read())
    end = time.time()
    print(f'Get downloaded in {end-start:.2f} seconds')

    # Multipart (completes 3.7s)
    start = time.time()
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    with open('multipart.tif', 'wb') as data:
        obj.download_fileobj(data)
    end = time.time()
    print(f'Multipart downloaded in {end-start:.2f} seconds')
