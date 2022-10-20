import s3fs

fs = s3fs.S3FileSystem()
gb = 1000000000
key = 'ffwilliams2-shenanigans/bursts/S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.zip'
with fs.open(key, 'rb') as f, open('test.zip', 'wb') as out_f:
    out_f.write(f.read(gb))
