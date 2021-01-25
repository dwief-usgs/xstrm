"""Export NHDPlusV2 upstream and downstream networks to HDF.

Description
----------
Use xstrm python package to build upstream and downstream networks
for the NHDPlusV2.1 dataset. This uses corrections to flow direction
identified in the ENHDPlusV2_us Routing Database. The
user can specify in the code if they would like the segment to be
included in the network by setting the include_seg variable before
running.

Data are stored by major drainages of CONUS. By default, for each
drainage and network direction (upstream and downstream) an HDF
file is exported to store index values for a segment and a list
of segments in the network. Also for each drainage a pkl file
is exported to store relationship between the index values and
COMIDs. This process only needs to be ran once, or until the
network changes.

To run this file in command line change directories to working
directory. Copy this file into the directory. Then run
"python nhdplusv2_build_netowrk.py". No variables need to
be set to run this file, although there is an option to manually
set "include_seg" and "network_dir".

Notes
----------
This was tested using conda prompt on Windows 10 with Python 3.8.
With defaults (up and downstream) this process took roughly
90 minutes to complete on (Intel Core i7-6600U CPU @ 2.6 GHz)
and required 15.3 GB.

Note to developers
Need to add options for divergence routing by querying VAA.

"""
import urllib.request as ur
import pandas as pd
import pickle
import os
import timeit
from xstrm import build_network


def to_pickle(file_name, data_obj):
    """Export Python Object to Pickle."""
    with open(file_name, 'wb') as handle:
        pickle.dump(data_obj, handle)


def create_folder(name):
    """If folder does not exist create folder."""
    if not os.path.exists(name):
        os.makedirs(name)


def get_enhdplusv2():
    """Get best known routing data from ENHDPlusV2 dataset.

    Returns
    ----------
    flow: df
        Pandas dataframe with ENDPlusV2_us data
        Formats COMID to str data type and renames a few fields

    Notes
    ----------
    ENHDPlusV2_us Routing Database corrects some to from data in the NHDPlusV2
    dataset but does not change any geospatial representations of the data.
    We encourage the use of this best known routing information when using
    NHDPlusV2. ENHDPlusV2_us data can be accessed
    https://doi.org/10.5066/P9PA63SM.

    """
    # Access ENHDPlusV2_us Routing Data (nhdplusv2 with routing corrections)
    download_url = "https://www.sciencebase.gov/catalog/file/get/5b92790be4b0702d0e809fe5?f=__disk__14%2F68%2F4e%2F14684ed52f9eb9cd0cbbcb92c3b8ae47c3404fb1"
    download_file = "data/ENHDPlusV2_us.zip"
    ur.urlretrieve(download_url, download_file)
    include_fields = ["COMID", "CFROMNODE", "CTONODE"]
    flow = pd.read_csv(download_file, usecols=include_fields)
    flow = flow.rename(
            {"COMID": "comid",
             "CFROMNODE": "fromnode",
             "CTONODE": "tonode"
             }, axis="columns"
        )
    flow["comid"] = flow["comid"].astype("int")
    flow["comid"] = flow["comid"].astype("str")
    return flow


def get_nhdplus2vaa():
    """Get NHDPlusV2 VAA network data.

    Returns
    ----------
    df: df
        Pandas dataframe with subset of NHDPlusV2 VAA data
        comid formatted to str data type, and removes coastline data

    Notes
    ----------
    Uses a parquet file of VAA to help with read efficiency.

    """
    # Get VAA file for NHDPlusV2
    v2_file = 'https://www.hydroshare.org/resource/6092c8a62fac45be97a09bfd0b0bf726/data/contents/nhdplusVAA.parquet'
    include = ['comid', 'rpuid', 'vpuid', 'divergence',
               'areasqkm', 'lengthkm', 'ftype']
    dl_df = pd.read_parquet(v2_file, columns=include)
    df = dl_df.loc[~dl_df['ftype'].isin(['Coastline'])].copy()

    df["comid"] = df["comid"].astype("int")
    df["comid"] = df["comid"].astype("str")
    return df


if __name__ == '__main__':

    # User must define if they wish to include segment in network parent list
    include_seg = True
    # User must define if they want to run up or downstream or both
    network_dir = ["up", "down"]

    flow = get_enhdplusv2()
    df = get_nhdplus2vaa()

    # Combine VAA and ENHDPlusV2
    flow_tofrom = pd.merge(df, flow, how='left', on='comid')

    # Update VPU assignment to have one VPU for each the entire MS and CO
    # drainages MS drainage represented by '08' and CO represented by '14'
    update_vpu = {'05': '08', '06': '08', '07': '08',
                  '10L': '08', '10U': '08', '11': '08', '15': '14'}
    flow_tofrom["vpuid"].replace(update_vpu, inplace=True)

    # First create folder for data if doesnt exist in working dir
    create_folder("data")

    # Export as pickle file, not needed for process but could be helpful
    flow_tofrom.to_pickle("data/nhdplusv2_network_data.pkl")

    # Get list of drainages in CONUS, based on VPU, see update_vpu
    vpu_list = flow_tofrom['vpuid'].drop_duplicates().to_list()

    # For direction and drainage build network, export to hdf
    for direction in network_dir:
        for vpu in vpu_list:
            t = timeit.default_timer()
            sub = flow_tofrom.loc[flow_tofrom['vpuid'] == vpu]
            if direction == "up":
                build_network_data = build_network.df_transform(
                    sub, "comid", "tonode", "fromnode"
                )
            elif direction == "down":
                build_network_data = build_network.df_transform(
                    sub, "comid", "fromnode", "tonode"
                )

            traverse_queue = build_network.build_network_setup(
                build_network_data[0]
            )

            # Build the network, export to hdf5 and build summary object
            # containing information for network calculations
            hdf_file = f"data/{vpu}_{direction}_network.hd5"

            network = build_network.build_hdf_network(
                traverse_queue, hdf_file, include_seg
            )

            # Export summary file to use in network calculations
            summary_file = f"data/{vpu}_{direction}_summary.pkl"
            to_pickle(summary_file, network)

            # Export index file to store index comid relationship
            indx_df_file = f"data/{vpu}_{direction}_indx.pkl"
            build_network_data[1].to_pickle(indx_df_file)

            seconds = (timeit.default_timer() - t)
            print (f"{vpu} and {direction} processed in {seconds} seconds")
