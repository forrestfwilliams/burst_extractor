import math
from concurrent.futures import ThreadPoolExecutor

import boto3

KB = 1024
MB = KB * KB


def calculate_range_parameters(offset, length, chunk_size):
    num_parts = int(math.ceil(length / float(chunk_size)))
    range_params = []
    for part_index in range(num_parts):
        start_range = (part_index * chunk_size) + offset
        if part_index == num_parts - 1:
            end_range = str(length + offset - 1)
        else:
            end_range = start_range + chunk_size - 1

        range_params.append(f'bytes={start_range}-{end_range}')
    return range_params


def s3_ranged_get(args):
    s3_client, bucket, key, range_header = args
    resp = s3_client.get_object(Bucket=bucket, Key=key, Range=range_header)
    body = resp['Body'].read()
    return body


def threaded_s3_get(s3_client, bucket, key, offset, length, chunksize=10 * MB):
    args_list = [(s3_client, bucket, key, x) for x in calculate_range_parameters(offset, length, chunksize)]

    # Dispatch work tasks with our client
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(s3_ranged_get, args_list)

    content = b''.join(results)
    return content


s3 = boto3.client('s3')
bucket = ''
key = ''

content = threaded_s3_get(s3, bucket, key, 1 * MB, 101 * MB)
with open('data.tif', 'wb') as f:
    f.write(content)
