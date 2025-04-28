"""
ERSSTv5 Pangeo Forge Recipe
"""

import apache_beam as beam
from leap_data_management_utils.data_management_transforms import get_catalog_store_urls
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

# Parse catalog store locations
catalog_store_urls = get_catalog_store_urls('feedstock/catalog.yaml')

###########################
## ERSSTv5 Single File  ###
###########################

input_urls = [
    "https://downloads.psl.noaa.gov/Datasets/noaa.ersst.v5/sst.mnmean.nc"
]

# Since it's a single file, no concat_dim needed
file_pattern = pattern_from_file_sequence(input_urls)

ersstv5_recipe = (
    beam.Create(file_pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray()
    | StoreToZarr(
        store_name='ERSSTv5.zarr',     # Must match meta.yaml
        combine_dims=file_pattern.combine_dim_keys,
        target_chunks={'time': -1, 'lat': 90, 'lon': 180},  # Example chunking for better performance
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
)
