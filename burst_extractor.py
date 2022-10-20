import io
import struct
import xml.etree.ElementTree as ET
import zipfile
import zlib

import boto3
from osgeo import gdal

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
        zip64_eocd_record = fetch(
            bucket,
            key,
            file_size - (EOCD_RECORD_SIZE + ZIP64_EOCD_LOCATOR_SIZE + ZIP64_EOCD_RECORD_SIZE),
            ZIP64_EOCD_RECORD_SIZE,
        )
        zip64_eocd_locator = fetch(
            bucket, key, file_size - (EOCD_RECORD_SIZE + ZIP64_EOCD_LOCATOR_SIZE), ZIP64_EOCD_LOCATOR_SIZE
        )
        cd_start, cd_size = get_central_directory_metadata_from_eocd64(zip64_eocd_record)
        central_directory = fetch(bucket, key, cd_start, cd_size)
        return (
            zipfile.ZipFile(io.BytesIO(central_directory + zip64_eocd_record + zip64_eocd_locator + eocd_record)),
            cd_start,
        )


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


def parse_short(in_bytes):
    return ord(in_bytes[0:1]) + (ord(in_bytes[1:2]) << 8)


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


def extract_xml(bucket, key, cd_start, filename):
    content = extract_file(bucket, key, cd_start, filename)
    xml = ET.parse(io.BytesIO(content)).getroot()
    return xml


class BurstMetadata:
    def __init__(self, annotation: ET.Element, burst_number):
        self.annotation = annotation
        self.burst_number = burst_number

        self.burst = self.annotation.findall('.//{*}burst')[self.burst_number]
        self.lines = int(self.annotation.findtext('.//{*}linesPerBurst'))
        self.samples = int(self.annotation.findtext('.//{*}samplesPerBurst'))

        firstValidSample = [int(val) for val in self.annotation.find('.//{*}firstValidSample').text.split()]
        lastValidSample = [int(val) for val in self.annotation.find('.//{*}lastValidSample').text.split()]
        first = False
        last = False
        for ii, val in enumerate(firstValidSample):
            if (val >= 0) and (not first):
                first = True
                self.firstValidLine = ii

            if (val < 0) and (first) and (not last):
                last = True
                self.numValidLines = ii - self.firstValidLine

        self.lastValidLine = self.firstValidLine + self.numValidLines - 1

        self.firstValidSample = max(firstValidSample[self.firstValidLine], firstValidSample[self.lastValidLine])
        self.lastValidSample = min(lastValidSample[self.firstValidLine], lastValidSample[self.lastValidLine])

        self.numValidSamples = self.lastValidSample - self.firstValidSample


def createBurstVRT(swath_path, out_path, burst):
    '''
    Create a VRT file representing a single burst.
    '''
    src = gdal.Open(swath_path, gdal.GA_ReadOnly)
    fullWidth = src.RasterXSize
    fullLength = src.RasterYSize
    del src
    yoffset = (burst.burst_number) * burst.lines

    rdict = {
        'inwidth': burst.lastValidSample - burst.firstValidSample,
        'inlength': burst.lastValidLine - burst.firstValidLine,
        'outwidth': burst.samples,
        'outlength': burst.lines,
        'filename': swath_path,
        'yoffset': yoffset + burst.firstValidLine,
        'localyoffset': burst.firstValidLine,
        'xoffset': burst.firstValidSample,
        'fullwidth': fullWidth,
        'fulllength': fullLength,
    }

    tmpl = '''<VRTDataset rasterXSize="{outwidth}" rasterYSize="{outlength}">
    <VRTRasterBand dataType="CFloat32" band="1">
        <NoDataValue>0.0</NoDataValue>
        <SimpleSource>
            <SourceFilename relativeToVRT="1">{filename}</SourceFilename>
            <SourceBand>1</SourceBand>
            <SourceProperties RasterXSize="{fullwidth}" RasterYSize="{fulllength}" DataType="CInt16"/>
            <SrcRect xOff="{xoffset}" yOff="{yoffset}" xSize="{inwidth}" ySize="{inlength}"/>
            <DstRect xOff="{xoffset}" yOff="{localyoffset}" xSize="{inwidth}" ySize="{inlength}"/>
        </SimpleSource>
    </VRTRasterBand>
</VRTDataset>'''

    with open(out_path, 'w') as f:
        f.write(tmpl.format(**rdict))


if __name__ == '__main__':
    # import cProfile
    # import pstats

    bucket = 'ffwilliams2-shenanigans'
    data = 'bursts/S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.zip'
    swath_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/measurement/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.tiff'
    annotation_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/annotation/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.xml'
    burst_number = 7
    # with cProfile.Profile() as pr:
    zip_file, cd_start = get_zip_file(bucket, data)
    annotation = extract_xml(bucket, data, cd_start, annotation_path)
    burst = BurstMetadata(annotation, burst_number)

    swath_bytes = extract_file(bucket, data, cd_start, swath_path)
    with open('swath.tif', 'wb') as f:
        f.write(swath_bytes)

    createBurstVRT('swath.tif', f'burst_0{burst_number+1}.vrt', burst)
    src = gdal.Open(f'burst_0{burst_number+1}.vrt')
    src = gdal.Translate(f'burst_0{burst_number+1}.tif', src, format='GTiff')
    del src

    # stats = pstats.Stats(pr)
    # stats.sort_stats(pstats.SortKey.TIME)
    # stats.print_stats()
    # stats.dump_stats(filename='profile.prof')
