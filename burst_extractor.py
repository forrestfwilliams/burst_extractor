from pathlib import Path

import boto3

from burst_downloader import S3Zip
from burst_translater import BurstMetadata

if __name__ == '__main__':

    bucket = 'ffwilliams2-shenanigans'

    # Reference
    key = 'bursts/S1A_IW_SLC__1SDV_20200616T022252_20200616T022319_033036_03D3A3_5D11.zip'
    swath_path = 'S1A_IW_SLC__1SDV_20200616T022252_20200616T022319_033036_03D3A3_5D11.SAFE/measurement/s1a-iw2-slc-vv-20200616t022254-20200616t022319-033036-03d3a3-005.tiff'
    annotation_path = 'S1A_IW_SLC__1SDV_20200616T022252_20200616T022319_033036_03D3A3_5D11.SAFE/annotation/s1a-iw2-slc-vv-20200616t022254-20200616t022319-033036-03d3a3-005.xml'
    burst_number = 7

    client = boto3.client('s3')
    safe_zip = S3Zip(client, bucket, key)

    annotation_name = safe_zip.extract_file(annotation_path, outname='reference.xml')
    swath_name = safe_zip.extract_file(swath_path, outname='reference_swath.tif')
    burst = BurstMetadata(swath_path, annotation_path, burst_number)
    burst.slc_to_file(str(Path(swath_path).name))

    # Secondary
    key = 'bursts/S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.zip'
    swath_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/measurement/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.tiff'
    annotation_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/annotation/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.xml'
    burst_number = 7

    client = boto3.client('s3')
    safe_zip = S3Zip(client, bucket, key)

    annotation_name = safe_zip.extract_file(annotation_path, outname='secondary.xml')
    swath_name = safe_zip.extract_file(swath_path, outname='secondary_swath.tif')
    burst = BurstMetadata(swath_path, annotation_path, burst_number)
    burst.slc_to_file(str(Path(swath_path).name))
