"""Calculate network summaries for CONUS.

Description
----------
Uses xstrm python package and networks stored in HDF format from
'nhdplusv2_build_network.py' to calculate network susmmaries for
streams in CONUS.

Accepts a CSV file with local segment values for CONUS. This CSV
can optionally contain a segment weight (e.g. area, length).

User must run 'nhdplusv2_build_network.py' in same directory before
using nhdplusv2_calc.py. In addition variables in this python file
must be updated before running.  User must specify the CSV
file with local segment data and has the option to specify
other variables as well if different than default values.

Returns
----------
csv output: csv
    CSV file for segment network summaries and if specified
    also contains percent of segment network not containing
    data.


Notes
----------
Note to developers
Missing data calculations do not currently include international
drainages. Drainages that cross international boundaries will
need to be updated to account for drainage areas in other countries.
nhdplusv2_build_network.py should be updated.

"""
import timeit
import os
import sys
import pandas as pd
from xstrm import build_network
from xstrm import network_calc


# User must define following variables
#############################################################################
local_data_csv = "test_local.csv"

# User can optionally define following variables
#############################################################################
network_dir = ["up"]  # network direction, Options include "up" and "down"
id_col_name = "comid"  # Note this is case sensitive
drop_cols = []  # Default use all cols, list quoted column names to not calc
weight_col_name = None  # Default None, common weights are area and length
calc_type = "sum"  # options "sum", "min", "max", "weighted_avg"
include_missing = True  # True or False, True calculates
precision = 3  # Precision of output calculations (number decimals retained)
num_proc = 7  # Number of cores to use for processing


def naming(local_data_csv):
    """Make sure file exists."""
    if os.path.isfile(local_data_csv):
        in_dir, in_file = os.path.split(local_data_csv)
        return in_dir, in_file
    else:
        sys.exit(
            "Please verify file path and name. Include extension .csv"
        )


def out_file_name(in_dir, in_file, calc_type, direction):
    """Set file name for outfile."""
    if in_dir == "":
        out_name = f"{in_dir}n_{direction}_{calc_type}_{in_file}"
    else:
        out_name = f"{in_dir}/n_{direction}_{calc_type}_{in_file}"
    return out_name


if __name__ == '__main__':
    flow_tofrom = pd.read_pickle("data/nhdplusv2_network_data.pkl")
    vpu_list = flow_tofrom['vpuid'].drop_duplicates().to_list()
    data_df = pd.read_csv(local_data_csv, dtype={id_col_name: str})

    for direction in network_dir:
        all_vpu_dfs = []
        for vpu in vpu_list:
            print (f"Starting VPU: {vpu}")
            t = timeit.default_timer()

            hdf_file = f"data/{vpu}_{direction}_network.hd5"

            vpu_tofrom = flow_tofrom.loc[flow_tofrom['vpuid'] == vpu]

            indx_df_file = f"data/{vpu}_{direction}_indx.pkl"
            indx_df = pd.read_pickle(indx_df_file)
            indx_comids = indx_df['comid'].drop_duplicates().to_list()

            data_df_vpu = data_df[data_df['comid'].isin(indx_comids)]

            local_df = network_calc.get_local_vars_df(
                data_df_vpu, indx_df, id_col_name, weight_col_name, drop_cols
            )

            summary_file = f"data/{vpu}_{direction}_summary.pkl"
            summary = pd.read_pickle(summary_file)
            summary.num_proc = num_proc

            summary.add_processing_details(
                local_df, calc_type, include_missing, hdf_file
            )

            summary.calc_one_parent()
            summary.calc_no_parent()
            summary.calc_mult_parent_mp()
            summary.combine_dfs()

            # replace indx with user supplied identifier
            final_df = build_network.indx_to_id(
                summary.final_df, indx_df, id_col_name, need="id_col_name"
            )
            final_df.set_index([id_col_name], inplace=True)
            final_df.index.rename(id_col_name, inplace=True)

            all_vpu_dfs.append(final_df)

            seconds = (timeit.default_timer() - t)
            m = f"{vpu} process completed in approximately {seconds} seconds"
            print (m)

        all_df = pd.concat(all_vpu_dfs, sort=True)
        # rename index to be initial id submitted
        all_df = all_df.round(precision)
        in_dir, in_file = os.path.split(local_data_csv)
        out_name = out_file_name(in_dir, in_file, calc_type, direction)
        all_df.to_csv(out_name)
