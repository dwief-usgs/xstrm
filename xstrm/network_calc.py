"""Summarize upstream catchment information.

    Author
    ----------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ----------
    Module that helps support network summarization
    information from "local" segments of the network.
    Methods require information summarized to local catchments.

    Notes
    ----------
    Throughout this module 'seg' represents a segment of a stream
    network. Segment can represent a line or polygon and can have a
    length and or area associated with it.
"""

# import needed packages
import pandas as pd
import sys
from multiprocessing import Pool
from functools import partial
from xstrm import build_network


def get_local_vars_csv(file_name,
                       indx_df,
                       id_col_name,
                       weight_col_name=None,
                       drop_cols=[]):
    """Import and format text file with local segment data.

    Description
    ----------
    Imports text file into pandas that contains summaries of information
    at the local stream segment level.  This file should also contain the
    stream segment identifier and stream_segment weight (e.g. area for
    area weighted average or length for length weighted average.) If weight
    is not required use default.

    See file 'tests/test_local_data.csv' as example

    Parameters
    ----------
    file_name: str
        string representation of file name including directory and extension
        e.g. 'data/my_local_data.csv'
    indx_df: df
        pandas dataframe that relates temporary index ids
        with user provided id (id_col)
    id_col_name: str
        name of the identifier column
        values in this column can represent str or int
    weight_col_name: str
        name of the column containing weights for upstream average
        this field is optional, default all segments have equal
        weights. Values should be int or float
    drop_cols: list
        list of comma separated strings representing column names that
        should not be processes during network_calc operations

    Returns
    ----------
    df2: df
        pandas dataframe intended for use in
        network_summary.py of the summarize_upstream module

    """

    df = pd.read_csv(file_name, dtype={id_col_name: str})

    df = get_local_vars_df(
        df, indx_df, id_col_name, weight_col_name, drop_cols
    )

    return df


def get_local_vars_df(df,
                      indx_df,
                      id_col_name,
                      weight_col_name=None,
                      drop_cols=[]):
    """Format pandas df with local segment data for use of network_calc methods.

    Description
    ----------
    Formats a dataframe of data for local stream segments to be used by
    network_calc functions. This dataframe should contain the
    stream segment identifier and stream_segment weight (e.g. area for
    area weighted average or length for length weighted average.) If weight
    is not required use default which will evenly weight segments.

    Parameters
    ----------
    df: df
        dataframe containing data for each local stream segment
    indx_df: df
        pandas dataframe that relates temporary index ids
        with user provided id (id_col_name)
    id_col_name: str
        name of the identifier column
        values in this column can represent str or int
    weight_col_name: str
        name of the column containing weights for upstream average
        this field is optional, default all segments have equal
        weights. Values should be int or float
    drop_cols: list
        list of comma separated strings representing column names that
        should not be processes during network_calc operations, default
        is an empty list

    Returns
    ----------
    df2: df
        pandas dataframe intended for use in
        network_summary.py of the summarize_upstream module

    """
    if weight_col_name is not None:
        required_cols = drop_cols + [weight_col_name]
        field_names = {str(weight_col_name): "seg_weight"}
    else:
        df['seg_weight'] = 1

    # Make sure all user supplied columns are in the database
    not_included = [
        n for n in required_cols if n not in df.columns.to_list()
    ]

    if len(not_included) > 0:
        m = f"At least one variable supplied ({not_included}) was not in the dataset."
        sys.exit(m)

    if id_col_name not in indx_df.columns.to_list():
        m = "Id column name must match name used in network build."
        sys.exit(m)

    # Join local data to the index file (see build_network.df_transform)
    df = build_network.indx_to_id(
        df, indx_df, id_col_name, need="xstrm_id"
    )

    df2 = df.rename(field_names, axis="columns")
    df2.set_index("xstrm_id", inplace=True)

    # Drop columns specified by user
    df2 = df2.drop(drop_cols, axis="columns")
    df2 = df2.astype(float)

    return df2


class NetworkCalc:

    def __init__(self, num_proc=4):
        """Initialize summary by setting process lists.

        Description
        ----------
        Manages information associated with processing
        network calculations.

        Parameters
        ----------
        num_proc: int
            Number of worker processes to use in multiprocessing

        """
        self.num_proc = num_proc
        self.no_parent_ids = []
        self.multi_parent_ids = []
        self.one_parent_ids = []
        self.no_parent_df = None
        self.one_parent_df = None
        self.multi_parent_df = None
        self.final_df = None

    def add_seg(self, xstrm_id, all_parents, include_seg=True):
        """Add xstrm_id to processing lists as traverse network."""
        if len(all_parents) == 0:
            self.no_parent_ids.append(xstrm_id)
        elif include_seg and len(all_parents) == 1:
            self.one_parent_ids.append(xstrm_id)
        else:
            val = {"xstrm_id": xstrm_id, "parents": all_parents}
            self.multi_parent_ids.append(val)

    def add_hdf_seg(self, xstrm_id, all_parents, include_seg=True):
        """Add xstrm_id to processing lists as traverse network."""
        if len(all_parents) == 0:
            self.no_parent_ids.append(xstrm_id)
        elif include_seg and len(all_parents) == 1:
            self.one_parent_ids.append(xstrm_id)
        else:
            self.multi_parent_ids.append(xstrm_id)

    def add_processing_details(
        self, local_df, calc_type="sum", include_missing=True
    ):
        """Add details to processing object."""
        if isinstance(local_df, pd.DataFrame):
            self.local_df = local_df
        else:
            m = "Verify local_df is a dataframe"
            sys.exit(m)
        self.include_missing = include_missing
        self.set_calc_type(calc_type)
        self.get_var_names()

    def set_calc_type(self, calc_type='sum'):
        """Validate and format calc type of NetworkSummary Class.

        Parameters
        ----------
        calc_type: str
            options: 'sum','min','max','weighted_avg'
            see calc_* functions for more detail

        """
        options = ['sum', 'max', 'min', 'weighted_avg']
        if calc_type.lower() in options:
            self.calc_type = calc_type.lower()
        else:
            sys.exit(
                "Please use a supported calc type: sum, max, min or weighted_avg"
            )

    def get_var_names(self, drop_vars=['xstrm_id', 'seg_weight']):
        """Get column names to process (target) and output.

        Parameters
        ----------
        drop_vars: list
            list of column names not to include

        Returns
        ----------
        target_vars: list
            list of column names to perform summary calculations on
        out_vars: list
            list of column names (str) expected in network_calc output

        """
        all_vars = self.local_df.columns.to_list()

        target_vars = [
            t for t in all_vars if t not in drop_vars
        ]

        # names of output variables prefix n_
        out_vars = [
            "n_" + t for t in target_vars
        ]

        # if missing include add output variable prefix mn_
        if self.include_missing:
            out_mnvars = [
                "mn_" + t for t in target_vars
            ]
            out_vars = out_vars + out_mnvars

        self.target_vars = target_vars
        self.out_vars = out_vars

    def calc_one_parent(self):
        """Build dataframe from one parent id list."""
        if len(self.one_parent_ids) > 0:
            self.one_parent_df = one_parent_to_df(
                self.local_df,
                self.one_parent_ids,
                self.target_vars,
                include_missing=self.include_missing
            )

    def calc_no_parent(self):
        """Build dataframe from no parent id list."""
        if len(self.no_parent_ids) > 0:
            self.no_parent_df = no_parent_to_df(
                self.no_parent_ids, self.out_vars
            )

    def calc_mult_parent(self):
        """Build dataframe from mult parent id list, no mp."""
        seg_summaries = []
        for seg in self.multi_parent_ids:
            xstrm_id = seg["xstrm_id"]
            parents = seg["parents"]

            target_df = self.local_df[
                self.local_df.index.isin(parents)
            ]

            if self.calc_type == 'sum':
                seg_summary = calc_sum(
                    target_df, self.target_vars, self.include_missing
                )
            elif self.calc_type == 'max':
                seg_summary = calc_max(
                    target_df, self.target_vars, self.include_missing
                )
            elif self.calc_type == 'min':
                seg_summary = calc_min(
                    target_df, self.target_vars, self.include_missing
                )
            elif self.calc_type == 'weighted_avg':
                seg_summary = calc_weighted_avg(
                    target_df, self.target_vars, self.include_missing
                )
            seg_summary.update({'xstrm_id': xstrm_id})
            seg_summaries.append(seg_summary)

        all_summary_df = pd.DataFrame(seg_summaries)
        all_summary_df.set_index("xstrm_id", inplace=True)
        # rename target vars to add "n_" to represent network summary
        rename_fields = {
            i: 'n_' + i for i in all_summary_df.columns if i in self.target_vars
        }

        self.multi_parent_df = all_summary_df.rename(rename_fields, axis="columns")

    def calc_mult_parent_mp(self):
        """Build dataframe from mult parent id list, no mp."""
        p = Pool(self.num_proc)
        list_summaries = partial(self.seg_calc)
        result = p.map(list_summaries, self.multi_parent_ids)
        p.close()
        p.join()
        # Build dataframe from list of dictionaries from mp
        all_summary_df = pd.DataFrame(result)
        all_summary_df.set_index("xstrm_id", inplace=True)
        # rename target vars to add "n_" to represent network summary
        rename_fields = {
            i: 'n_' + i for i in all_summary_df.columns if i in self.target_vars
        }
        all_summary_df = all_summary_df.rename(rename_fields, axis="columns")
        self.multi_parent_df = all_summary_df

    def seg_calc(self, seg):
        """Segment network calculation for use with multiprocessing."""
        xstrm_id = seg['xstrm_id']
        parents = seg['parents']
        target_df = self.local_df[
            self.local_df.index.isin(parents)
        ]

        if self.calc_type == 'sum':
            seg_summary = calc_sum(
                target_df, self.target_vars, self.include_missing
            )
        elif self.calc_type == 'max':
            seg_summary = calc_max(
                target_df, self.target_vars, self.include_missing
            )
        elif self.calc_type == 'min':
            seg_summary = calc_min(
                target_df, self.target_vars, self.include_missing
            )
        elif self.calc_type == 'weighted_avg':
            seg_summary = calc_weighted_avg(
                target_df, self.target_vars, self.include_missing
            )
        seg_summary.update({'xstrm_id': xstrm_id})
        return (seg_summary)

    def combine_dfs(self, id_col_name="xstrm_id"):
        """Combine no parent, one parent, mult parent dataframes."""
        frames = []
        if self.one_parent_df is not None:
            frames.append(self.one_parent_df)
        if self.no_parent_df is not None:
            frames.append(self.no_parent_df)
        if self.multi_parent_df is not None:
            frames.append(self.multi_parent_df)

        if len(frames) > 0:
            self.final_df = pd.concat(frames, sort=True)
        # reset xstrm_id name to user input
        if self.final_df is not None and id_col_name != "xstrm_id":
            self.final_df.index.rename(id_col_name)

    def to_csv(self, name):
        """Export network calc output to CSV."""
        file_name = f"{name}_{self.calc_type}.csv"
        self.final_df.to_csv(file_name, sep=",", index=True)


def one_parent_to_df(
    local_df, one_parent_ids, target_vars, include_missing=True
):
    """Create dataframe for streams with one parent if including local segment value.

    Description
    ----------
    Use a list of identifiers to query local data and return same values
    for upstream calculation columns.  With these identifiers representing
    streams with one parent and parameters set to include local segment value the
    values for n_ fields will be equal to local segment values.
    if include_missing=True add columns to represent
    missing data and populate with 100.  100 is used to represent 100% of the
    network not having data.

    Parameters
    ----------
    local_df: df
        pandas dataframe intended for use in
        network_summary.py of the summarize_upstream module,
        contains values representing local segments in network
    one_parent_ids: list
        list of strings representing identifiers of segments with one parent
    target_vars: list
        list of column names to perform summary calculations on
    include_missing: bool
        where True summarizes percent of missing data based on seg_weight

    Returns
    ----------
    one_parent_df: df
        pandas dataframe with xstrm_id index and n_ values equal to local

    """
    one_parent_df = local_df[local_df.index.isin(one_parent_ids)]

    one_parent_df = one_parent_df[target_vars]
    one_parent_df = one_parent_df.add_prefix('n_')
    # For each variable in target_vars add missing_var
    if include_missing:
        for var in target_vars:
            n_var = f"n_{var}"
            m_var = f"mn_{var}"
            one_parent_df.loc[one_parent_df[n_var].isna(), m_var] = 100.0
            one_parent_df.loc[~one_parent_df[n_var].isna(), m_var] = 0.0

    # if calc_type == "weighted_avg":
    #     no_weight = one_parent_df.index[one_parent_df['seg_weight'].isna()].tolist()
    #     one_parent_df.loc[one_parent_df.index.isin(no_weight), :] = np.nan

    return one_parent_df


def no_parent_to_df(no_data_ids, out_vars):
    """Build out dataframe for segments with no data.

    Description
    ----------
    Use a list of identifiers to build out dataframe of no data where
    segment ids are used as index.

    Parameters
    ----------
    no_data_ids: list
        list of strings representing identifiers of segments with no data
    out_vars: list
        list of strings used as column names for the new dataframe

    Returns
    ----------
    df: df
        pandas dataframe with xstrm_id index and all values = nan

    """
    no_data_df = pd.DataFrame(index=no_data_ids, columns=out_vars)
    no_data_df.index.name = 'xstrm_id'
    return no_data_df


def calc_sum(target_df, target_vars, include_missing=True):
    """Sum values for each column in target vars list.

    Parameters
    ----------
    target_df: df
        pandas dataframe of each stream segment's local values
        for each segment in the network. Number of variables included
        is dependent on user.
    target_vars: list
        list of column names to perform summary calculations on
    include_missing: bool
        True include percent of segs (using weight) without data
        at the local scale

    Returns
    ----------
    summary: dict
        dictionary of calculated summary for each variable
        does not summarize ids or seg_weight

    """
    summary = target_df[target_vars].sum().to_dict()
    if include_missing:
        missing = get_missing_data(target_df, target_vars)
        summary.update(missing)

    return summary


def calc_max(target_df, target_vars, include_missing=True):
    """Max value for each column in target vars list.

    Parameters
    ----------
    target_df: df
        pandas dataframe of each stream segment's local values
        for each segment in the network. Number of variables included
        is dependent on user.
    target_vars: list
        list of column names to perform summary calculations on
    include_missing: bool
        True include percent of segs (using weight) without data
        at the local scale

    Returns
    ----------
    summary: dict
        dictionary of calculated summary for each variable
        does not summarize ids or seg_weight

    """
    summary = target_df[target_vars].max().to_dict()
    if include_missing:
        missing = get_missing_data(target_df, target_vars)
        summary.update(missing)
    return summary


def calc_min(target_df, target_vars, include_missing=True):
    """Min value for each column in target vars list.

    Parameters
    ----------
    target_df: df
        pandas dataframe of each stream segment's local values
        for each segment in the network. Number of variables included
        is dependent on user.
    target_vars: list
        list of column names to perform summary calculations on
    include_missing: bool
        True include percent of segs (using weight) without data
        at the local scale

    Returns
    ----------
    summary: dict
        dictionary of calculated summary for each variable
        does not summarize ids or seg_weight

    """
    summary = target_df[target_vars].min().to_dict()
    if include_missing:
        missing = get_missing_data(target_df, target_vars)
        summary.update(missing)
    return summary


def calc_weighted_avg(target_df, target_vars, include_missing=True):
    """Weighted average value for each column in target vars list.

    Parameters
    ----------
    target_df: df
        pandas dataframe of each stream segment's local values
        for each segment in the network. Number of variables included
        is dependent on user.
    target_vars: list
        list of column names to perform summary calculations on
    include_missing: bool
        True include percent of segs (using weight) without data
        at the local scale

    Returns
    ----------
    summary: dict
        dictionary of calculated summary for each variable
        does not summarize ids or seg_weight

    """
    # total_weight = target_df['seg_weight'].sum()
    seg_weighted_sum = target_df[
        target_vars
    ].multiply(target_df["seg_weight"], axis="index").sum()

    summary = {}
    for var in target_vars:
        total_weight = target_df.loc[~target_df[var].isna()]['seg_weight'].sum()
        if total_weight == 0:
            weighted = None
        else:
            weighted = (seg_weighted_sum[var] / total_weight)

        summary.update({var: weighted})

    # summary = (seg_weighted_sum / total_weight).to_dict()
    if include_missing:
        missing = get_missing_data(target_df, target_vars)
        summary.update(missing)

    return summary


def get_missing_data(target_df, target_vars):
    """Summarize weighted percent of local values missing for each column.

    Parameters
    ----------
    target_df: df
        pandas dataframe of each stream segment's local values
        for each segment in the network. Number of variables included
        is dependent on user.
    target_vars: list
        list of column names to perform summary calculations on

    Returns
    ----------
    missing: dict
        dictionary of calculated missing values for each variable
        does not summarize ids or seg_weight

    """
    target_df = target_df.assign(
        seg_weight_per=(
            (target_df["seg_weight"]) / (target_df["seg_weight"].sum()) * (100)
        )
    )

    missing = {
        "mn_" + x : target_df["seg_weight_per"].loc[
            target_df[x].isna()
        ].sum() for x in target_vars
    }

    return missing
