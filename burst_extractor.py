import boto3
import io
import struct
import zipfile
import zlib
from IPython import embed

s3 = boto3.client('s3')

EOCD_RECORD_SIZE = 22
ZIP64_EOCD_RECORD_SIZE = 56
ZIP64_EOCD_LOCATOR_SIZE = 20

MAX_STANDARD_ZIP_SIZE = 4_294_967_295

def get_zip_file(bucket, key):
    file_size = get_file_size(bucket, key)
    eocd_record = fetch(bucket, key, file_size - EOCD_RECORD_SIZE, EOCD_RECORD_SIZE)
    if file_size <= MAX_STANDARD_ZIP_SIZE:
        print('accessing zip')
        cd_start, cd_size = get_central_directory_metadata_from_eocd(eocd_record)
        central_directory = fetch(bucket, key, cd_start, cd_size)
        return zipfile.ZipFile(io.BytesIO(central_directory + eocd_record)), cd_start
    else:
        print('accessing zip64')
        zip64_eocd_record = fetch(bucket, key,
                                  file_size - (EOCD_RECORD_SIZE + ZIP64_EOCD_LOCATOR_SIZE + ZIP64_EOCD_RECORD_SIZE),
                                  ZIP64_EOCD_RECORD_SIZE)
        zip64_eocd_locator = fetch(bucket, key,
                                   file_size - (EOCD_RECORD_SIZE + ZIP64_EOCD_LOCATOR_SIZE),
                                   ZIP64_EOCD_LOCATOR_SIZE)
        cd_start, cd_size = get_central_directory_metadata_from_eocd64(zip64_eocd_record)
        central_directory = fetch(bucket, key, cd_start, cd_size)
        return zipfile.ZipFile(io.BytesIO(central_directory + zip64_eocd_record + zip64_eocd_locator + eocd_record)), cd_start


def get_file_size(bucket, key):
    head_response = s3.head_object(Bucket=bucket, Key=key)
    return head_response['ContentLength']

def fetch(bucket, key, start, length):
    end = start + length - 1
    response = s3.get_object(Bucket=bucket, Key=key, Range="bytes=%d-%d" % (start, end))
    return response['Body'].read()

def get_central_directory_metadata_from_eocd(eocd):
    cd_size = parse_little_endian_to_int(eocd[12:16])
    cd_start = parse_little_endian_to_int(eocd[16:20])
    return cd_start, cd_size

def get_central_directory_metadata_from_eocd64(eocd64):
    cd_size = parse_little_endian_to_int(eocd64[40:48])
    cd_start = parse_little_endian_to_int(eocd64[48:56])
    return cd_start, cd_size

def parse_little_endian_to_int(little_endian_bytes):
    format_character = "i" if len(little_endian_bytes) == 4 else "q"
    return struct.unpack("<" + format_character, little_endian_bytes)[0]

def print_zip_content(zip_file):
    files = [zi.filename for zi in zip_file.filelist]
    print(f"Files: {files}")

def parse_short(bytes):
    return ord(bytes[0:1]) + (ord(bytes[1:2]) << 8)

def extract_file(bucket, key, cd_start, filename):
    zi = [zi for zi in zip_file.filelist if zi.filename == filename][0]
    file_head = fetch(bucket, key, cd_start + zi.header_offset + 26, 4)
    name_len = parse_short(file_head[0:2])
    extra_len = parse_short(file_head[2:4])

    content_offset = cd_start + zi.header_offset + 30 + name_len + extra_len
    content = fetch(bucket, key, content_offset, zi.compress_size)
    if zi.compress_type == zipfile.ZIP_DEFLATED:
        content = zlib.decompressobj(-zlib.MAX_WBITS).decompress(content)

    return content

if __name__ == '__main__':
    import cProfile
    import pstats

    bucket = 'ffwilliams2-shenanigans'
    data = 'bursts/S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.zip'
    filename = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/measurement/s1a-iw1-slc-vh-20200604t022252-20200604t022317-032861-03ce65-001.tiff'
    with cProfile.Profile() as pr:
        zip_file, cd_start = get_zip_file(bucket, data)
        with open('test.tif', 'wb') as f:
            data = extract_file(bucket, data, cd_start, filename)
            f.write(data)

stats = pstats.Stats(pr)
stats.sort_stats(pstats.SortKey.TIME)
stats.print_stats()
stats.dump_stats(filename='profile.prof')
