"""
Integration between Daft DataFrame and DeltaCAT storage formats.

This module provides a monkey patch to extend Daft DataFrames with the ability
to write data directly to DeltaCAT's storage formats via the Writer interface.
"""

from daft import DataFrame
from .monkey_patch import write_deltacat_df_method

# Make sure our key modules are available for import
from . import execution_handlers
from . import monkey_patch

def patch_daft():
    """
    Apply monkey patches to Daft to add DeltaCAT integration.
    
    This adds a write_deltacat method to Daft's DataFrame class, which can be used
    with any implementation of DeltaCAT's Writer interface, such as IcebergWriter
    or RivuletWriter.
    
    Returns:
        bool: True if the patch was applied successfully
    """
    DataFrame.write_deltacat = write_deltacat_df_method
    return True