import asyncio
import math
import time

from aiobotocore.session import get_session
import boto3


def get_chunks(start, length, chunk_size=10 * 1024):
    n_chunks = math.floor(length / chunk_size)
    n_chunks = min(n_chunks, 3)
    starts = [start + chunk_size * i for i in range(n_chunks)] + [chunk_size * n_chunks]
    stops = [(start + chunk_size * (i + 1)) - 1 for i in range(n_chunks)] + [length]
    return starts, stops


async def get_async(bucket, key, start, stop):
    session = get_session()
    async with session.create_client('s3', region_name='us-west-2') as client:
        resp = await client.get_object(Bucket=bucket, Key=key, Range=f'bytes={start}-{stop}')
        async with resp['Body'] as stream:
            body = await stream.read()
    return body


async def download_range_async(bucket, key, start, length, chunk_size=10 * 1024):
    starts, stops = get_chunks(start, length, chunk_size)
    jobs = [get_async(bucket, key, i, j) for i, j in zip(starts, stops)]
    byte_list = await asyncio.gather(*jobs)
    return b''.join(byte_list)


# Run on a r5d.xlarge EC2 instance in the same region as the S3 bucket (us-west-2)
# Public access is enabled on the bucket
if __name__ == '__main__':
    bucket = 'ffwilliams2-shenanigans'
    key = 'bursts/swath.tif'

    # # Direct GET (completes in 39s)
    # start = time.time()
    # s3 = boto3.client('s3')
    # response = s3.get_object(Bucket=bucket, Key=key)
    # with open('get.tif', 'wb') as f:
    #     f.write(response['Body'].read())
    # end = time.time()
    # print(f'Get downloaded in {end-start:.2f} seconds')

    # # Multipart (completes 3.7s)
    # start = time.time()
    # s3 = boto3.resource('s3')
    # obj = s3.Object(bucket, key)
    # with open('multipart.tif', 'wb') as data:
    #     obj.download_fileobj(data)
    # end = time.time()
    # print(f'Multipart downloaded in {end-start:.2f} seconds')

    # Async GET (completes in 39s)
    start = time.time()
    s3 = boto3.client('s3')
    content_length = s3.get_object(Bucket=bucket, Key=key)['ContentLength']
    breakpoint()
    content = asyncio.run(download_range_async(bucket, key, 0, content_length))
    with open('async.tif', 'wb') as f:
        f.write(content)
    end = time.time()
    print(f'Get downloaded in {end-start:.2f} seconds')
