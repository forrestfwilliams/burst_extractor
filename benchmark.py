import asyncio
import math
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import boto3.session
from aiobotocore.session import get_session
from botocore.config import Config

KB = 1024
MB = KB * KB


def get_chunks(start, length, chunk_size):
    n_chunks = math.floor(length / chunk_size)
    starts = [start + chunk_size * i for i in range(n_chunks)] + [chunk_size * n_chunks]
    stops = [(start + chunk_size * (i + 1)) - 1 for i in range(n_chunks)] + [length]
    return starts, stops


async def get_async(client, bucket, key, start, stop):
    resp = await client.get_object(Bucket=bucket, Key=key, Range=f'bytes={start}-{stop}')
    body = await resp['Body'].read()
    return body


async def download_range_async(bucket, key, start, length, chunk_size):
    starts, stops = get_chunks(start, length, chunk_size)
    session = get_session()
    config = Config(max_pool_connections=100)
    async with session.create_client('s3', region_name='us-west-2', config=config) as client:
        jobs = [get_async(client, bucket, key, i, j) for i, j in zip(starts, stops)]
        byte_list = await asyncio.gather(*jobs)
    return b''.join(byte_list)


def get_s3(args):
    client, bucket, key, range_header = args
    resp = client.get_object(Bucket=bucket, Key=key, Range=range_header)
    body = resp['Body'].read()
    return body


def thread_pool_get(s3_client, bucket, key, start, length, chunk_size):
    # Define some work to be done, this can be anything
    starts, stops = get_chunks(start, length, chunk_size=chunk_size)
    my_tasks = [[s3_client, bucket, key, f'bytes={i}-{j}'] for i, j in zip(starts, stops)]

    # Dispatch work tasks with our s3_client
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(get_s3, my_tasks)

    content = b''.join(results)
    return content


# Run on a r5d.xlarge EC2 instance in the same region as the S3 bucket (us-west-2)
# Public access is enabled on the bucket
if __name__ == '__main__':
    bucket = 'ffwilliams2-shenanigans'
    key = 'bursts/swath.tif'
    # key = 'bursts/file.txt'

    # Direct GET (completes in 15s)
    start = time.time()
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    with open('get.tif', 'wb') as f:
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

    # Async GET (Completes in 6.8s)
    start = time.time()
    s3 = boto3.client('s3')
    content_length = s3.get_object(Bucket=bucket, Key=key)['ContentLength']
    content = asyncio.run(download_range_async(bucket, key, 0, content_length, chunk_size=25 * MB))
    with open('async.tif', 'wb') as f:
        f.write(content)
    end = time.time()
    print(f'Async downloaded in {end-start:.2f} seconds')

    # ThreadPoolExecutor Get (Completes in 4.7s)
    start = time.time()
    s3 = boto3.client('s3')
    content_length = s3.get_object(Bucket=bucket, Key=key)['ContentLength']
    content = thread_pool_get(s3, bucket, key, 0, content_length, chunk_size=25 * MB)
    with open('threadpool.tif', 'wb') as f:
        f.write(content)
    end = time.time()
    print(f'ThreadPool downloaded in {end-start:.2f} seconds')
