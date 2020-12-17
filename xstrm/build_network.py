"""Summarize upstream catchment information.

    Description
    ----------
    Module that helps support summarization of upstream network
    information from "local" segments of the network.
    Methods included focus on building of a network and requires
    to and from node information for each stream segment.

    Notes
    ----------
    Throughout this module 'seg' represents a segment of a stream
    network. Segment can represent a line or polygon.

"""

import pandas as pd
import warnings
import sys
import numpy as np
import h5py
import fastparquet
from xstrm import network_calc


class StreamSegment:
    """Build and store network information of a stream segments.

    Description
    ----------
    Used to help track network information for a stream segment.
    Methods to initialize stream segment and update based on known
    information about the network.

    Intended to be used with def build_network_setup() and
    with def build_network().

    Parameters
    ----------
    xstrm_id : str
        identifier of local segment of a network
        int or int represented as string

    Other Object Attributes
    ----------
    children : list
        list of upstream xstrm_ids
    parents : list
        list of downstream xstrm_ids
    visited_parent_cnt: int
        number of total parents visited
    all_parents: set
        unique set of xstrm_ids

    """

    def __init__(self, xstrm_id):
        self.xstrm_id = int(xstrm_id)
        self.children = []
        self.parents = []
        self.visited_parent_cnt = 0
        self.all_parents = {}

    def update_all_parents(self):
        """Update parent list with list of upstream seg ids."""
        parent_list = sorted(set([p for p in self.all_parents]))
        self.all_parents = parent_list

    # def __repr__(self):
    #     """Prints string representation of stream segment information."""
    #     p_cnt = len(self.all_parents)
    #     xstrm_id = self.xstrm_id
    #     return f"StreamSegment(xstrm_id: {xstrm_id}, parent cnt: {p_cnt})"


def build_network_setup(df):
    """Build a queue of Stream Segments for upstream summarization.

    Description
    ----------
    Accepts dataframe with each row documenting a relation between
    a segment and upstream segment (using ids)
    We use to/from nodes to built this relationship

    Parameters
    ----------
    df: df
        pandas dataframe with xstrm_id as index, and column 'up_xstrm_id'

    Returns
    ----------
    traverse_queue: list
        list of StreamSegment objects
        This list contains objects with no upstream segments

    """
    segments = {}
    for row in df.itertuples():
        xstrm_id = str(row.Index)
        up_xstrm_id = str(row.up_xstrm_id)

        if xstrm_id not in segments:
            # create new stream unit
            new_seg = StreamSegment(xstrm_id)
            segments[xstrm_id] = new_seg
            seg = new_seg
        else:
            seg = segments[xstrm_id]

        # create new upper stream unit if it was not created yet
        if up_xstrm_id != "0" and up_xstrm_id not in segments:
            up_seg = StreamSegment(up_xstrm_id)
            segments[up_xstrm_id] = up_seg
            up_seg.children.append(seg)
            seg.parents.append(up_seg)
        elif up_xstrm_id != "0":
            up_seg = segments[up_xstrm_id]
            up_seg.children.append(seg)
            seg.parents.append(up_seg)

    # Build starting point for aggregation
    # Only include headwaters (no parent upstream)
    traverse_queue = [x for x in segments.values() if not x.parents]
    return traverse_queue


def build_network(traverse_queue, include_seg=True):
    """Build the network, return full network as list of objects.

    Description
    ----------
    Builds the network and returns it as a list of objects.
    Intended to provide a build_network option with maximum
    flexibility of use and no direct connection to network calc.
    For large networks, complex networks we recommend using
    build_network_hdf as this method is memory expensive!

    Uses traverse_queue from def build_network_setup() to
    build out entire network. When building upstream networks
    this process starts at headwater streams and
    traverses the network adding new segments to the queue as
    all of the segments parents become accounted for.
    This process returns the entire network as a list of segments,
    where each segment is represented by a StreamSegment object.

    Parameters
    ----------
    traverse_queue: list
        list of StreamSegment objects
        This list contains objects with no upstream segments
    include_seg: bool
        where True means add segment to parent list

    Returns
    ----------
    traverse_queue: list
        complete list of StreamSegment objects for the network
        each object contains a list of upstream segment ids

    """
    traverse_queue_indx = 0

    while traverse_queue_indx < len(traverse_queue):
        seg = traverse_queue[traverse_queue_indx]
        traverse_queue_indx += 1
        traverse_queue = transfer_seg_data(seg, traverse_queue)
        if include_seg:
            seg.all_parents[seg.xstrm_id] = seg
        seg.update_all_parents()
        seg.children = None
        seg.parents = None
    return traverse_queue


def build_calc_network(traverse_queue, include_seg=True, num_proc=4):
    """Build the network, return network in processing ready form.

    Description
    ----------
    Uses traverse_queue from def build_network_setup() to
    build out entire network.  Starts at headwater streams and
    traverses the network adding new segments to the queue as
    all of the segments parents become accounted for.
    This process returns entire network as a list of segments,
    where each segment is represented by a StreamSegment object.

    Parameters
    ----------
    traverse_queue: list
        list of StreamSegment objects
        This list contains objects with no upstream segments
    include_seg: bool
        where True means add segment to parent list
    num_proc: int
        Number of worker processes to use in multiprocessing

    Returns
    ----------
    summary: NetworkCalc object
        Initialized NetworkCalc object to support processing

    """
    traverse_queue_indx = 0
    summary = network_calc.NetworkCalc(num_proc)

    while traverse_queue_indx < len(traverse_queue):
        seg = traverse_queue[traverse_queue_indx]
        traverse_queue_indx += 1
        traverse_queue = transfer_seg_data(seg, traverse_queue)

        if include_seg:
            seg.all_parents[seg.xstrm_id] = seg
        all_parents = [p for p in seg.all_parents]

        summary.add_seg(seg.xstrm_id, all_parents, include_seg=include_seg)

        # Help manage memory
        seg.all_parents = None
        seg.children = None
        seg.parents = None

    return summary


def build_hdf_network(traverse_queue, hdf_file, include_seg=True):
    """Traverse and build the network starting with headwater segments.

    Description
    ----------
    Uses traverse_queue from def build_network_setup() to
    build out entire network.  Starts at headwater streams and
    traverses the network adding new segments to the queue as
    all of the segments parents become accounted for.
    This process returns entire network as a list of segments,
    where each segment is represented by a StreamSegment object.

    Parameters
    ----------
    traverse_queue: list
        list of StreamSegment objects
        This list contains objects with no upstream segments
    hdf_file: str
        file name including .hd5 extenstion
    networkcalc_file: str
        file name for networkcalc object (parquet)
        must include extension .parquet
    include_seg: bool
        where True means add segment to parent list

    Returns
    ----------
    summary: obj
        NetworkCalc object containing info for network calc
        including 3 lists (no parents, one parent and multiple parents)
    hdf_file: hd5
        local hdf5 file for storage and access of network

    """
    f = h5py.File(hdf_file , 'a')
    traverse_queue_indx = 0
    summary = network_calc.NetworkCalc()

    while traverse_queue_indx < len(traverse_queue):
        seg = traverse_queue[traverse_queue_indx]
        traverse_queue = transfer_seg_data(seg, traverse_queue)

        # Set parent list
        if include_seg:
            seg.all_parents[seg.xstrm_id] = seg
        all_parents = sorted(set([p for p in seg.all_parents]))
        summary.add_hdf_seg(seg.xstrm_id, all_parents, include_seg=include_seg)

        my_id = np.int(seg.xstrm_id)
        # Create a HDF5 group for the given ID number
        grp = f.create_group(str(my_id))
        np_all_parents = np.zeros(len(all_parents), dtype=np.int) + all_parents

        # Create dataset for parent list, compress only if effective (>256 parents)
        if (len(all_parents) == 0):
            grp.create_dataset('all_parents', data=[])  # Write an empty list
        elif (len(all_parents) > 256):
            # Write the parent IDs using GZIP compression and Byte order shuffling
            grp.create_dataset(
                'all_parents',
                data=np_all_parents,
                compression="gzip",
                compression_opts=6,
                shuffle=True
            )
        elif (len(all_parents) > 0):
            grp.create_dataset('all_parents', data=np_all_parents)

        # Help manage memory
        seg.all_parents = None
        seg.children = None
        seg.parents = None

        # Advance queue
        traverse_queue_indx += 1

    # Close hdf
    f.close()

    return summary


def df_transform(df, id_col_name, to_node_col, from_node_col):
    """Format start dataframe for network build.

    Description
    ----------
    Format dataframe containing segments, to nodes and from nodes.
    Reformats data to be excepted by get_data.build_network_setup().

    Parameters
    ----------
    df: df
        pandas dataframe containing segment id, to node, from node
    id_col_name: str
        string representation of name of the identifier column
        values in this column can represent str or num
    to_node_col: str
        string representation of name of the column containing
        to node information for the network
        values in this column can represent str or num
    from_node_col: str
        string representation of name of the column containing
        from node information for the network
        values in this column can represent str or num

    Returns
    ----------
    network_df: df
        pandas dataframe intended for use in
        network_summary.py of the summarize_upstream
        structured as xstrm_id as index with up_xstrm_id column
        both columns stored as str and up_xstrm_id null values
        are stored as 0
    indx_df: df
        pandas dataframe that relates temporary index ids
        with user provided id (id_col)

    """
    if id_col_name == "xstrm_id":
        m = "id_col can not be called xstrm_id"
        sys.exit(m)
    keep = [str(id_col_name), str(to_node_col), str(from_node_col)]
    df = df[keep]

    field_names = {
        str(to_node_col): "to_node",
        str(from_node_col): "from_node"
    }

    df = df.rename(field_names, axis="columns")
    df["xstrm_id"] = df.index + 1

    indx_df = df[[id_col_name, "xstrm_id"]]
    # indx_df.set_index(["xstrm_id"], inplace=True)

    network_id_temp = df[["xstrm_id", "to_node"]]
    network_seg_list = pd.merge(
        df, network_id_temp, left_on="from_node", right_on="to_node", how="left"
    )
    network_df = network_seg_list.rename(
        {"xstrm_id_x": "xstrm_id", "xstrm_id_y": "up_xstrm_id"}, axis="columns"
    )
    network_df = network_df[["xstrm_id", "up_xstrm_id"]]
    network_df.set_index("xstrm_id", inplace=True)

    # If nan values exist fill with 0 and give user message
    cnt_na = network_df.isna().sum().sum()
    m = f"{cnt_na} start segments identified (headwater in upstream)."
    warnings.warn(m)

    network_df = network_df.fillna(0)
    network_df["up_xstrm_id"] = network_df["up_xstrm_id"].astype(int)

    return network_df, indx_df


def import_tofrom_csv(file_name, id_col_name, to_node_col, from_node_col):
    """Import and format text file with stream network info.

    Description
    ----------
    Imports CSV file containing segments along with to and from nodes.
    Reformats data to be excepted by get_data.build_network_setup().

    Parameters
    ----------
    file_name: str
        string representation of file name including directory and extension
        e.g. 'data/my_network_data.csv'
    id_col_name: str
        string representation of name of the identifier column
        values in this column can represent str or num
    to_node_col: str
        string representation of name of the column containing
        to node information for the network
        values in this column can represent str or num
    from_node_col: str
        string representation of name of the column containing
        from node information for the network
        values in this column can represent str or num

    Returns
    ----------
    network_df: df
        pandas dataframe intended for use in
        network_summary.py of the summarize_upstream
        structured as xstrm_id as index with up_xstrm_id column
        both columns stored as str and up_xstrm_id null values
        are stored as 0
    indx_df: df
        pandas dataframe that relates temporary index ids
        with user provided id (id_col)

    """
    use_cols = [
        str(id_col_name), str(to_node_col), str(from_node_col)
    ]

    df = pd.read_csv(file_name, usecols=use_cols, dtype=str)
    network_df, indx_df = df_transform(
        df, id_col_name, to_node_col, from_node_col
    )
    return network_df, indx_df


def transfer_seg_data(seg, traverse_queue):
    """Transfer data to child segments.

    Description
    ----------
    Transfers object information from current segment to
    child segments and updates visited parent list.  If all
    parents visited then add to build network queue.

    Parameters
    ----------
    seg: obj
        StreamSegment object
    traverse_queue: list
        list of object to process

    Returns
    ----------
    traverse_queue: list
        Updated list of objects to process
        this may or may not be the same as the input

    Notes
    ----------
    In addition to returning the queue, segment objects are updated
    In building upstream network, children would be downstream segments
    In building downstream network, children would be upstream segments
    """
    # For each child of segment
    for child in seg.children:
        # Add seg's all parent list and seg to childs all parent list
        child.all_parents.update(seg.all_parents)
        child.all_parents[seg.xstrm_id] = seg
        # Increase child's visited-parent-count by one
        child.visited_parent_cnt += 1
        # If child's visited-parent-count equals the number segments
        # in child' parent list all of child's parent segments
        # have been visited and child is inserted into queue
        if len(child.parents) == child.visited_parent_cnt:
            traverse_queue.append(child)
    return traverse_queue


def get_parents_hdf(hd5_open, xstrm_id):
    """Get np array of ids in network for xstrm_id.

    Parameters
    ----------
    hd5_open: object
        object representing opened hd5 file.
        "with h5py.File(file, 'r') as hd5_open"
    xstrm_id: str or int
        xstrm_id of the stream segment of interest

    Returns
    ----------
    parents: array
        numpy array of xstrm identifiers where each
        identifier is an integer

    """
    v = hd5_open.get(str(xstrm_id))
    try:
        parents = np.sort(
            np.array(v.get('all_parents'), dtype=np.int)
        )
    except Exception as e:
        e = f"{xstrm_id} is not represented in hdf file."
        sys.exit(e)

    return parents


def get_parent_list(network, xstrm_id):
    """Get list of ids in network for xstrm_id.

    Parameters
    ----------
    network: list
        complete list of StreamSegment objects for the network
        as returned in def build_network()
    xstrm_id: str
        id of the stream segment of interest

    Returns
    ----------
    parents: list
        list of stream segment identifiers in str format

    """
    # if seg is not found return None
    parents = next((
        s.all_parents for s in network if s.xstrm_id == xstrm_id
    ), None)

    if parents is not None:
        parents = sorted(list(parents))
    else:
        message = f"{xstrm_id} was not found in network."
        warnings.warn(message)

    return parents


def indx_to_id(data_df, indx_df, id_col_name, need="id_col_name"):
    """Replace processing index with user supplied identifier.

    Parameters
    ----------
    data_df: df
        pandas dataframe with_data
    indx_df: df
        pandas dataframe that relates temporary index ids
        with user provided id (id_col_name)
    id_col_name: str
        string representation of name of the identifier column
        values in this column can represent str or num
    need: str
        string representing which field
        options include "id_col_name" or "xstrm_id"

    Returns
    ----------
    related_df: df
        pandas dataframe with need column included

    """
    if need == "id_col_name":
        related_df = pd.merge(
            data_df, indx_df, on="xstrm_id", how="left"
        )
        related_df = related_df.drop(
            ["xstrm_id"], axis="columns"
        )
    elif need == "xstrm_id":
        related_df = pd.merge(
            data_df, indx_df, on=id_col_name, how="left"
        )
        related_df = related_df.drop(
            [id_col_name], axis="columns"
        )
    else:
        m = "Unexpected value for need in indx_to_id()"
        sys.exit(m)

    return related_df


def write_parquet(file_name, obj_name):
    # Export networkcalc object
    fastparquet.write(file_name, obj_name)


def read_parquet(file_name):
    pf = fastparquet.ParquetFile(file_name)
    return pf
