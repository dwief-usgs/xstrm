"""Use xstrm modules to create stream summaries."""

from xstrm import build_network
from xstrm import network_calc
import os.path
import sys


def network_calc_to_csv(
        to_from_csv,
        local_data_csv,
        id_col_name,
        to_node_col,
        from_node_col,
        weight_col_name=None,
        calc_type="sum",
        include_seg=True,
        include_missing=True,
        num_proc=4,
        precision=3,
        drop_cols=[]
):
    """Use xstrm package to process stream network summaries.

    Description
    ----------
    Use xstrm methods to process network summaries.
    This function builds a network from to from nodes
    and calculates network summaries for each segment,
    where a segment can be a stream segment or catchment
    associated with the nodes. Exports data to csv. This
    method is used in the network calculator and is intended
    to simplify use of xstrm on any stream network containing
    topology (to and from nodes).

    Parameters
    ----------
    to_from_csv: str
        String representation of file name including directory and extension
        e.g. 'data/my_network_data.csv'
    local_data_csv: list
        String representation of file name including directory and extension
        e.g. 'data/my_local_data.csv'
    id_col_name: str
        String representation of the column name for the identifier column.
        Values in this column can represent str or num.
    to_node_col: str
        String representation of column name for the column containing
        to node information for the network.
        Values in this column can represent str or num.
    from_node_col: str
        String representation of column name for the column containing
        from node information for the network.
        Values in this column can represent str or num.
    weight_col_name: str
        String representation of the column name for the column containing
        weights for network weighted averages.  This field is optional,
        and as default all segments have equal weights.
        Values should be int or float.
    calc_type: str
        Options include: 'sum','min','max','weighted_avg'
        See calc_* functions for more detail.
    include_seg: bool
        True means include processing segment in parent list.
        False means omit processing segment from parent list.
    include_missing: bool
        Where True summarizes percent of segment weight missing data
        at the local scale.  Where False does not calculate missing.
    num_proc: int
        Number of worker processes to use in multiprocessing.
    precision: int
        Number of decimal places to round results.
    drop_cols: list
        List of comma separated strings representing column names that
        should not be processes during network_calc operations.

    Returns
    ----------
    file: CSV
       Comma seperated file with all specified network calculations.
       Also includes user's segment id.

    Note
    ----------
    For NHD networks the to node is the downstream node.

    """
    if os.path.isfile(to_from_csv) and os.path.isfile(local_data_csv):
        pass
    else:
        sys.exit(
            "Please verify file path and name. Include extension .csv"
        )

    in_dir, in_file = os.path.split(local_data_csv)

    build_network_data = build_network.import_tofrom_csv(
        to_from_csv, id_col_name, to_node_col, from_node_col
    )

    traverse_queue = build_network.build_network_setup(
        build_network_data[0]
    )

    indx_df = build_network_data[1]

    local_df = network_calc.get_local_vars_csv(
        local_data_csv, indx_df, id_col_name, weight_col_name, drop_cols
    )

    summary = build_network.build_calc_network(
        traverse_queue, include_seg, num_proc=num_proc
    )

    summary.add_processing_details(
        local_df, calc_type, include_missing
    )

    summary.calc_one_parent()
    summary.calc_no_parent()
    summary.calc_mult_parent_mp()
    summary.combine_dfs()

    # rename index to be initial id submitted
    out_df = summary.final_df.round(precision)

    # replace indx with user supplied identifier
    final_df = build_network.indx_to_id(
        out_df, indx_df, id_col_name, need="id_col_name"
    )
    final_df.set_index([id_col_name], inplace=True)
    final_df.index.rename(id_col_name, inplace=True)

    if in_dir == "":
        out_name = f"{in_dir}n_{calc_type}_{in_file}"
    else:
        out_name = f"{in_dir}/n_{calc_type}_{in_file}"
    final_df.to_csv(out_name)
