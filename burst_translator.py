import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

from osgeo import gdal


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

        burst_xmls = self.annotation.findall('.//{*}burst')
        self.burst = burst_xmls[self.burst_number]
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
        
    def get_bounds(self):
        return self.first_valid_sample, self.last_valid_sample, self.first_valid_line, self.last_valid_line

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

    def slc_to_file(self, out_name: str, fmt: str = 'GTiff'):
        '''Write a burst raster to file by creating temporary VRT, then translating to desired format.
        Parameters:
        -----------
        out_path : path of output burst file.
        fmt: output format specified using gdal driver name (see https://gdal.org/drivers/raster/index.html)
        '''
        dst_dir = str(Path(out_name).parent)

        # create VRT; make temporary if output not VRT
        if fmt != 'VRT':
            temp_vrt = tempfile.NamedTemporaryFile(dir=dst_dir)
            vrt_fname = temp_vrt.name
        else:
            vrt_fname = out_name
        self.slc_to_vrt_file(vrt_fname)

        if fmt == 'VRT':
            return

        src_ds = gdal.Open(vrt_fname)
        gdal.Translate(out_name, src_ds, format=fmt)
        del src_ds


if __name__ == '__main__':
    # Reference
    swath_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/measurement/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.tiff'
    annotation_path = 'S1A_IW_SLC__1SDV_20200604T022251_20200604T022318_032861_03CE65_7C85.SAFE/annotation/s1a-iw2-slc-vv-20200604t022253-20200604t022318-032861-03ce65-005.xml'
    burst_number = 7
    burst = BurstMetadata(swath_path, annotation_path, burst_number)
    burst.slc_to_vrt_file(f'{str(Path(swath_path).stem)}.vrt')
    burst.slc_to_file(str(Path(swath_path).name))

    # Secondary
    swath_path = 'S1A_IW_SLC__1SDV_20200616T022252_20200616T022319_033036_03D3A3_5D11.SAFE/measurement/s1a-iw2-slc-vv-20200616t022254-20200616t022319-033036-03d3a3-005.tiff'
    annotation_path = 'S1A_IW_SLC__1SDV_20200616T022252_20200616T022319_033036_03D3A3_5D11.SAFE/annotation/s1a-iw2-slc-vv-20200616t022254-20200616t022319-033036-03d3a3-005.xml'
    burst_number = 7
    burst = BurstMetadata(swath_path, annotation_path, burst_number)
    burst.slc_to_vrt_file(f'{str(Path(swath_path).stem)}.vrt')
    burst.slc_to_file(str(Path(swath_path).name))
