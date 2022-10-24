import io
import math
import struct
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from isal import isal_zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from osgeo import gdal

EOCD_RECORD_SIZE = 22
ZIP64_EOCD_RECORD_SIZE = 56
ZIP64_EOCD_LOCATOR_SIZE = 20
MAX_STANDARD_ZIP_SIZE = 4_294_967_295
KB = 1024
MB = KB * KB
MULTIPART_THRESHOLD = 8 * MB
MULTIPART_CHUNKSIZE = 8 * MB
ZLIB_MAX_WBITS = 15


def get_zip_file(s3_client, bucket, key):
    file_size = get_file_size(s3_client, bucket, key)
    eocd_record = fetch(s3_client, bucket, key, file_size - EOCD_RECORD_SIZE, EOCD_RECORD_SIZE)
    if file_size <= MAX_STANDARD_ZIP_SIZE:
        print('accessing zip')
        cd_start, cd_size = get_central_directory_metadata_from_eocd(eocd_record)
        central_directory = fetch(s3_client, bucket, key, cd_start, cd_size)
        return zipfile.ZipFile(io.BytesIO(central_directory + eocd_record)), cd_start
    else:
        print('accessing zip64')
        zip64_eocd_record = fetch(
            s3_client,
            bucket,
            key,
            file_size - (EOCD_RECORD_SIZE + ZIP64_EOCD_LOCATOR_SIZE + ZIP64_EOCD_RECORD_SIZE),
            ZIP64_EOCD_RECORD_SIZE,
        )
        zip64_eocd_locator = fetch(
            s3_client, bucket, key, file_size - (EOCD_RECORD_SIZE + ZIP64_EOCD_LOCATOR_SIZE), ZIP64_EOCD_LOCATOR_SIZE
        )
        cd_start, cd_size = get_central_directory_metadata_from_eocd64(zip64_eocd_record)
        central_directory = fetch(s3_client, bucket, key, cd_start, cd_size)
        return (
            zipfile.ZipFile(io.BytesIO(central_directory + zip64_eocd_record + zip64_eocd_locator + eocd_record)),
            cd_start,
        )


def get_file_size(s3_client, bucket, key):
    head_response = s3_client.head_object(Bucket=bucket, Key=key)
    return head_response['ContentLength']


def calculate_range_parameters(total_size, offset, chunk_size):
    num_parts = int(math.ceil(total_size / float(chunk_size)))
    range_params = []
    for part_index in range(num_parts):
        start_range = (part_index * chunk_size) + offset
        if part_index == num_parts - 1:
            end_range = str(total_size + offset - 1)
        else:
            end_range = start_range + chunk_size - 1
        range_params.append(f'bytes={start_range}-{end_range}')
    return range_params


def threaded_s3_get(args):
    s3_client, bucket, key, range_header = args
    resp = s3_client.get_object(Bucket=bucket, Key=key, Range=range_header)
    body = resp['Body'].read()
    return body


def threaded_s3_get_workflow(s3_client, bucket, key, offset, file_size, chunk_size):
    # Define some work to be done, this can be anything

    my_tasks = [[s3_client, bucket, key, i] for i in calculate_range_parameters(file_size, offset, chunk_size)]

    # Dispatch work tasks with our s3_client
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(threaded_s3_get, my_tasks)

    content = b''.join(results)
    return content


def fetch(s3_client, bucket, key, start, length):
    if length <= MULTIPART_THRESHOLD:
        end = start + length - 1
        response = s3_client.get_object(Bucket=bucket, Key=key, Range="bytes=%d-%d" % (start, end))
        content = response['Body'].read()
    else:
        content = threaded_s3_get_workflow(s3_client, bucket, key, start, length, MULTIPART_CHUNKSIZE)
    return content


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


def parse_short(in_bytes):
    return ord(in_bytes[0:1]) + (ord(in_bytes[1:2]) << 8)


def extract_file(s3_client, bucket, key, cd_start, filename):
    zi = [zi for zi in zip_file.filelist if zi.filename == filename][0]
    file_head = fetch(s3_client, bucket, key, cd_start + zi.header_offset + 26, 4)
    name_len = parse_short(file_head[0:2])
    extra_len = parse_short(file_head[2:4])

    content_offset = cd_start + zi.header_offset + 30 + name_len + extra_len
    content = fetch(s3_client, bucket, key, content_offset, zi.compress_size)
    if zi.compress_type == zipfile.ZIP_DEFLATED:
        content = isal_zlib.decompressobj(-15).decompress(content)

    return content


def extract_xml(s3_client, bucket, key, cd_start, filename):
    content = extract_file(s3_client, bucket, key, cd_start, filename)
    xml = ET.parse(io.BytesIO(content)).getroot()
    return xml


class BurstMetadata:
    def __init__(self, swath_path: str, annotation: ET.Element, burst_number):
        self.swath_path = swath_path
        self.annotation = annotation
        self.burst_number = burst_number

        self.burst = self.annotation.findall('.//{*}burst')[self.burst_number]
        n_lines = int(self.annotation.findtext('.//{*}linesPerBurst'))
        n_samples = int(self.annotation.findtext('.//{*}samplesPerBurst'))

        first_valid_samples = [int(val) for val in self.burst.find('firstValidSample').text.split()]
        last_valid_samples = [int(val) for val in self.burst.find('lastValidSample').text.split()]

        first_valid_line = [x >= 0 for x in first_valid_samples].index(True)
        n_valid_lines = [x >= 0 for x in first_valid_samples].count(True)
        last_line = first_valid_line + n_valid_lines - 1

        first_valid_sample = max(first_valid_samples[first_valid_line], first_valid_samples[last_line])
        last_sample = min(last_valid_samples[first_valid_line], last_valid_samples[last_line])

        self.first_valid_line = first_valid_line
        self.last_valid_line = last_line
        self.first_valid_sample = first_valid_sample
        self.last_valid_sample = last_sample
        self.shape = (n_lines, n_samples)

    def slc_to_vrt_file(self, out_path):
        '''Write burst to VRT file.
        Parameters:
        -----------
        out_path : string
            Path of output VRT file.
        need: burst_number, self.shape, last_valid_sample, first_valid_sample, last_valid_line, first_valid_line
        '''
        line_offset = self.burst_number * self.shape[0]

        inwidth = self.last_valid_sample - self.first_valid_sample
        inlength = self.last_valid_line - self.first_valid_line + 1
        outlength, outwidth = self.shape
        yoffset = line_offset + self.first_valid_line
        localyoffset = self.first_valid_line
        xoffset = self.first_valid_sample
        gdal_obj = gdal.Open(self.swath_path, gdal.GA_ReadOnly)
        fullwidth = gdal_obj.RasterXSize
        fulllength = gdal_obj.RasterYSize

        # TODO maybe cleaner to write with ElementTree
        tmpl = f'''<VRTDataset rasterXSize="{outwidth}" rasterYSize="{outlength}">
    <VRTRasterBand dataType="CInt16" band="1">
        <NoDataValue>0.0</NoDataValue>
        <SimpleSource>
            <SourceFilename relativeToVRT="1">{self.swath_path}</SourceFilename>
            <SourceBand>1</SourceBand>
            <SourceProperties RasterXSize="{fullwidth}" RasterYSize="{fulllength}" DataType="CInt16"/>
            <SrcRect xOff="{xoffset}" yOff="{yoffset}" xSize="{inwidth}" ySize="{inlength}"/>
            <DstRect xOff="{xoffset}" yOff="{localyoffset}" xSize="{inwidth}" ySize="{inlength}"/>
        </SimpleSource>
    </VRTRasterBand>
</VRTDataset>'''

        with open(out_path, 'w') as fid:
            fid.write(tmpl)

    def slc_to_file(self, out_path: str, fmt: str = 'GTiff'):
        '''Write burst to GTiff file.

        Parameters:
        -----------
        out_path : string
            Path of output GTiff file.
        '''
        # get output directory of out_path
        dst_dir = str(Path(out_path).parent)

        # create VRT; make temporary if output not VRT
        if fmt != 'VRT':
            temp_vrt = tempfile.NamedTemporaryFile(dir=dst_dir)
            vrt_fname = temp_vrt.name
        else:
            vrt_fname = out_path
        self.slc_to_vrt_file(vrt_fname)

        if fmt == 'VRT':
            return

        # open temporary VRT and translate to GTiff
        src_ds = gdal.Open(vrt_fname)
        gdal.Translate(out_path, src_ds, format=fmt)

        # clean up
        del src_ds


if __name__ == '__main__':
    s3 = boto3.client('s3')

    bucket = 'ffwilliams2-shenanigans'
    data = 'bursts/S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.zip'
    swath_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/measurement/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.tiff'
    annotation_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/annotation/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.xml'
    zip_file, cd_start = get_zip_file(s3, bucket, data)
    annotation = extract_xml(s3, bucket, data, cd_start, annotation_path)

    swath_bytes = extract_file(s3, bucket, data, cd_start, swath_path)
    with open('swath.tif', 'wb') as f:
        f.write(swath_bytes)

    burst_number = 7
    burst = BurstMetadata('swath.tif', annotation, burst_number)
    burst.slc_to_file(f'burst_0{burst_number+1}.tif')

    # for burst_number in range(9):
    #     burst = BurstMetadata('swath.tif', annotation, burst_number)
    #     burst.slc_to_file(f'burst_0{burst_number+1}.tif')
