import io
import math
import struct
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import requests
from isal import isal_zlib
from osgeo import gdal

# from uuid import uuid4


KB = 1024
MB = KB * KB


def bytes_to_xml(in_bytes):
    xml = ET.parse(io.BytesIO(in_bytes)).getroot()
    return xml


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


class S3Zip:
    def __init__(self, client, bucket, key, multipart_threshold=8 * MB, multipart_chunksize=8 * MB):
        self.EOCD_RECORD_SIZE = 22
        self.ZIP64_EOCD_RECORD_SIZE = 56
        self.ZIP64_EOCD_LOCATOR_SIZE = 20
        self.MAX_STANDARD_ZIP_SIZE = 4_294_967_295
        self.ZLIB_MAX_WBITS = 15

        self.client = client
        self.bucket = bucket
        self.key = key
        self.multipart_threshold = multipart_threshold
        self.multipart_chunksize = multipart_chunksize
        self.url = f'https://{self.bucket}.s3.us-west-2.amazonaws.com/{self.key}'

        self.zip_dir, self.cd_start = self.get_zip_dir()

    def parse_short(self, in_bytes):
        return ord(in_bytes[0:1]) + (ord(in_bytes[1:2]) << 8)

    def parse_little_endian_to_int(self, little_endian_bytes):
        format_character = "i" if len(little_endian_bytes) == 4 else "q"
        return struct.unpack("<" + format_character, little_endian_bytes)[0]

    def get_central_directory_metadata_from_eocd(self, eocd):
        cd_size = self.parse_little_endian_to_int(eocd[12:16])
        cd_start = self.parse_little_endian_to_int(eocd[16:20])
        return cd_start, cd_size

    def get_central_directory_metadata_from_eocd64(self, eocd64):
        cd_size = self.parse_little_endian_to_int(eocd64[40:48])
        cd_start = self.parse_little_endian_to_int(eocd64[48:56])
        return cd_start, cd_size

    def print_zip_content(self):
        files = [zi.filename for zi in self.zip_dir.filelist]
        print(f"Files: {files}")

    def get_file_size(self):
        # file_size = self.client.head_object(Bucket=self.bucket, Key=self.key)['ContentLength']
        file_size = int(self.client.head(self.url).headers['content-length'])
        return file_size

    def ranged_s3_get(self, range_header):
        resp = self.client.get_object(Bucket=self.bucket, Key=self.key, Range=range_header)
        body = resp['Body'].read()
        return body

    def ranged_http_get(self, range_header):
        resp = self.client.get(self.url, headers={'Range': range_header})
        body = resp.content
        return body

    def threaded_get(self, offset, file_size):
        range_params = calculate_range_parameters(file_size, offset, self.multipart_chunksize)

        # Dispatch work tasks with our s3_client
        with ThreadPoolExecutor(max_workers=20) as executor:
            # results = executor.map(self.ranged_s3_get, range_params)
            results = executor.map(self.ranged_http_get, range_params)

        content = b''.join(results)
        return content

    def get(self, start, length):
        if length <= self.multipart_threshold:
            end = start + length - 1
            # content = self.ranged_s3_get(f'bytes={start}-{end}')
            content = self.ranged_http_get(f'bytes={start}-{end}')
        else:
            content = self.threaded_get(start, length)
        return content

    def get_zip_dir(self):
        file_size = self.get_file_size()
        eocd_record = self.get(file_size - self.EOCD_RECORD_SIZE, self.EOCD_RECORD_SIZE)
        if file_size <= self.MAX_STANDARD_ZIP_SIZE:
            print('accessing zip')
            cd_start, cd_size = self.get_central_directory_metadata_from_eocd(eocd_record)
            central_directory = self.get(cd_start, cd_size)
            return zipfile.ZipFile(io.BytesIO(central_directory + eocd_record)), cd_start
        else:
            print('accessing zip64')
            zip64_eocd_record = self.get(
                file_size - (self.EOCD_RECORD_SIZE + self.ZIP64_EOCD_LOCATOR_SIZE + self.ZIP64_EOCD_RECORD_SIZE),
                self.ZIP64_EOCD_RECORD_SIZE,
            )
            zip64_eocd_locator = self.get(
                file_size - (self.EOCD_RECORD_SIZE + self.ZIP64_EOCD_LOCATOR_SIZE),
                self.ZIP64_EOCD_LOCATOR_SIZE,
            )
            cd_start, cd_size = self.get_central_directory_metadata_from_eocd64(zip64_eocd_record)
            central_directory = self.get(cd_start, cd_size)
            return (
                zipfile.ZipFile(io.BytesIO(central_directory + zip64_eocd_record + zip64_eocd_locator + eocd_record)),
                cd_start,
            )

    def extract_file(self, filename, outname=None):
        zi = [zi for zi in self.zip_dir.filelist if zi.filename == filename][0]
        file_head = self.get(self.cd_start + zi.header_offset + 26, 4)
        name_len = self.parse_short(file_head[0:2])
        extra_len = self.parse_short(file_head[2:4])

        content_offset = self.cd_start + zi.header_offset + 30 + name_len + extra_len
        content = self.get(content_offset, zi.compress_size)
        if zi.compress_type == zipfile.ZIP_DEFLATED:
            content = isal_zlib.decompressobj(-1 * self.ZLIB_MAX_WBITS).decompress(content)

        if outname:
            with open(outname, 'wb') as f:
                f.write(content)
            return outname

        return content


class BurstMetadata:
    def __init__(self, swath_name: str, annotation_name: str, burst_number: int):
        '''A class containing the metadata and routines need to create an ISCE-compatible burst geotiff.
        Parameters:
        -----------
        swath_name : path of input swath tif from SAFE file (unzipped)
        annotation_name : path of input annotation xml from SAFE file (unzipped)
        burst_number: burst number to be extracted (0-indexed by order in annotation file)
        '''
        self.swath_name = swath_name
        self.annotation = ET.parse(annotation_name).getroot()
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

    def slc_to_vrt_file(self, out_path: str):
        '''Writes an ISCE-compatible burst to VRT a file.
        Parameters:
        -----------
        out_path : path of output VRT file.
        '''
        line_offset = self.burst_number * self.shape[0]

        inwidth = self.last_valid_sample - self.first_valid_sample
        inlength = self.last_valid_line - self.first_valid_line + 1
        outlength, outwidth = self.shape
        yoffset = line_offset + self.first_valid_line
        localyoffset = self.first_valid_line
        xoffset = self.first_valid_sample
        gdal_obj = gdal.Open(self.swath_name, gdal.GA_ReadOnly)
        fullwidth = gdal_obj.RasterXSize
        fulllength = gdal_obj.RasterYSize

        # TODO maybe cleaner to write with ElementTree
        tmpl = f'''<VRTDataset rasterXSize="{outwidth}" rasterYSize="{outlength}">
    <VRTRasterBand dataType="CInt16" band="1">
        <NoDataValue>0.0</NoDataValue>
        <SimpleSource>
            <SourceFilename relativeToVRT="1">{self.swath_name}</SourceFilename>
            <SourceBand>1</SourceBand>
            <SourceProperties RasterXSize="{fullwidth}" RasterYSize="{fulllength}" DataType="CInt16"/>
            <SrcRect xOff="{xoffset}" yOff="{yoffset}" xSize="{inwidth}" ySize="{inlength}"/>
            <DstRect xOff="{xoffset}" yOff="{localyoffset}" xSize="{inwidth}" ySize="{inlength}"/>
        </SimpleSource>
    </VRTRasterBand>
</VRTDataset>'''

        with open(out_path, 'w') as f:
            f.write(tmpl)

    def slc_to_file(self, out_path: str, fmt: str = 'GTiff'):
        '''Write a burst raster to file by creating temporary VRT, then translating to desired format.
        Parameters:
        -----------
        out_path : path of output burst file.
        fmt: output format specified using gdal driver name (see https://gdal.org/drivers/raster/index.html)
        '''
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

        src_ds = gdal.Open(vrt_fname)
        gdal.Translate(out_path, src_ds, format=fmt)
        del src_ds


if __name__ == '__main__':

    bucket = 'ffwilliams2-shenanigans'
    key = 'bursts/S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.zip'
    swath_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/measurement/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.tiff'
    annotation_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/annotation/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.xml'

    # s3 = boto3.client('s3')
    # safe_zip = S3Zip(s3, bucket, key)
    http = requests.session()
    safe_zip = S3Zip(http, bucket, key)

    annotation_out = 'annotation.xml'
    annotation_bytes = safe_zip.extract_file(annotation_path, outname=annotation_out)
    breakpoint()

    swath_out = 'swath.tif'
    swath_bytes = safe_zip.extract_file(swath_path, outname=swath_out)

    # annotation = bytes_to_xml(safe_zip.extract_file(annotation_path))
    # swath_name = f'/vsimem/{uuid4().hex}'
    # gdal.FileFromMemBuffer(swath_name, swath_bytes)

    burst_number = 7
    burst = BurstMetadata(swath_out, annotation_out, burst_number)
    burst.slc_to_file(f'burst_0{burst_number+1}.tif')

    # for burst_number in range(9):
    #     burst = BurstMetadata('swath.tif', annotation, burst_number)
    #     burst.slc_to_file(f'burst_0{burst_number+1}.tif')
