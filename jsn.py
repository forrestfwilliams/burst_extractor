#!/usr/bin/env python3

# Example code to extract all bursts from a single Sentinel SLC measurement tiff.

from collections import namedtuple

import numpy as np
from lxml import etree
from osgeo import gdal

offsets = namedtuple('offsets', 'start end')

# annotation xml
tree = etree.parse('s1a-iw1-slc-vh-20220302t135243-20220302t135308-042143-05057e-001.xml')

# load in the tiff itself with GDAL
measurement = gdal.Open('s1a-iw1-slc-vh-20220302t135243-20220302t135308-042143-05057e-001.tiff')

# number of lines in burst
frame_lines = int(tree.xpath('./swathTiming/linesPerBurst/text()')[0])

# for each burst
for index, burst in enumerate(tree.xpath('./swathTiming/burstList/burst')):
    # all offsets, even invalid offsets
    offsets_range = offsets(
        np.array([int(val) for val in burst.xpath('firstValidSample/text()')[0].split()]),
        np.array([int(val) for val in burst.xpath('lastValidSample/text()')[0].split()]),
    )

    # returns the indices of lines containing valid data
    lines_with_valid_data = np.flatnonzero(offsets_range.end - offsets_range.start)

    # get first and last sample with valid data per line
    # x-axis, range
    valid_offsets_range = offsets(
        offsets_range.start[lines_with_valid_data].min(),
        offsets_range.end[lines_with_valid_data].max(),
    )

    # get the first and last line with valid data
    # y-axis, azimuth
    valid_offsets_azimuth = offsets(
        lines_with_valid_data.min(),
        lines_with_valid_data.max(),
    )

    # x-length
    length_range = valid_offsets_range.end - valid_offsets_range.start
    # y-length
    length_azimuth = len(lines_with_valid_data)

    # n-th burst * total lines + first azimuth
    # y-offset
    azimuth_start = index * frame_lines + valid_offsets_azimuth.start

    # [x-offset, y-offset, x-length, y-length]
    window = [valid_offsets_range.start, azimuth_start, length_range, length_azimuth]
