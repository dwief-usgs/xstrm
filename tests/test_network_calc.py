#!/usr/bin/env python

"""Tests for `network_calc` module."""

from xstrm import network_calc
from xstrm import build_network
import pandas as pd
import pytest

data = [
    {'xstrm_id': '1', 'seg_weight': 2, 'var1': 1, 'var2': 3},
    {'xstrm_id': '2', 'seg_weight': 3, 'var1': 2, 'var2': 4},
    {'xstrm_id': '3', 'seg_weight': 5, 'var1': None, 'var2': 5}
]
test_df = pd.DataFrame(data)
# test_df.set_index('xstrm_id', inplace=True)


@pytest.fixture(scope='module')
def test_target_vars():
    return ['var1', 'var2']


@pytest.fixture(scope='module')
def traverse_queue(build_network_data):
    return build_network.build_network_setup(build_network_data)


@pytest.fixture(scope='module')
def tcalc():
    # Test NetworkCalc class init, test calc
    expected_dict = {'num_proc': 4,
                     'no_parent_ids': [],
                     'multi_parent_ids': [],
                     'one_parent_ids': [],
                     'no_parent_df': None,
                     'one_parent_df': None,
                     'multi_parent_df': None,
                     'final_df': None}
    test_networkcalc = network_calc.NetworkCalc(4)
    assert test_networkcalc.__dict__ == expected_dict
    return test_networkcalc


def test_add_seg(tcalc):
    """Test adding segs specific to each id list."""
    xstrm_id1 = 2
    all_parents1 = []
    tcalc.add_seg(xstrm_id1, all_parents1)
    xstrm_id2 = 3
    all_parents2 = [3]
    tcalc.add_seg(xstrm_id2, all_parents2)
    xstrm_id3 = 4
    all_parents3 = [4, 5]
    tcalc.add_seg(xstrm_id3, all_parents3)
    assert tcalc.no_parent_ids == [2]
    assert tcalc.one_parent_ids == [3]
    assert tcalc.multi_parent_ids == [{"xstrm_id": 4, "parents": [4, 5]}]


def test_add_processing_details(tcalc, local_data):
    """Test add_processing_details."""
    tcalc.add_processing_details(local_data, "SUM")
    assert tcalc.calc_type == "sum"
    assert tcalc.local_df is not None
    assert isinstance(tcalc.local_df, pd.DataFrame)


def test_set_calc_type(tcalc):
    """Test set calc type."""
    tcalc.set_calc_type("Max")
    assert tcalc.calc_type == "max"
    tcalc.set_calc_type("SUM")
    assert tcalc.calc_type == "sum"
    tcalc.set_calc_type("Weighted_Avg")
    assert tcalc.calc_type == "weighted_avg"
    tcalc.set_calc_type("min")
    assert tcalc.calc_type == "min"
    # Note to self: Should add test to ensure sys.exit if bad type


def test_calc_one_parent(tcalc):
    """Test calc one parent only works when data."""
    tNone = network_calc.NetworkCalc(4)
    tNone.calc_one_parent()
    assert tNone.one_parent_df is None
    tcalc.calc_one_parent()
    assert isinstance(tcalc.one_parent_df, pd.DataFrame)


def test_calc_no_parent(tcalc):
    """Test calc no parent only works when data."""
    tNone = network_calc.NetworkCalc(4)
    tNone.calc_no_parent()
    assert tNone.no_parent_df is None
    tcalc.calc_no_parent()
    assert isinstance(tcalc.no_parent_df, pd.DataFrame)


def test_get_var_names(tcalc):
    tcalc.local_df = test_df
    tcalc.include_missing = True
    tcalc.get_var_names()
    assert tcalc.target_vars == ["var1", "var2"]
    assert tcalc.out_vars == ["n_var1", "n_var2",
                              "mn_var1", "mn_var2"]
    tcalc.include_missing = False
    tcalc.get_var_names()
    assert tcalc.target_vars == ["var1", "var2"]
    assert tcalc.out_vars == ["n_var1", "n_var2"]


def test_no_parent_to_df():
    """Test no data to dataframe function."""
    no_data_ids = ['1', '2']
    out_vars = ['n_var1', 'n_var2']
    out_df = network_calc.no_parent_to_df(no_data_ids, out_vars)

    expected = [{'xstrm_id': '1',
                 'n_var1': None,
                 'n_var2': None},
                {'xstrm_id': '2',
                 'n_var1': None,
                 'n_var2': None}]
    expected_df = pd.DataFrame(expected)
    expected_df.set_index('xstrm_id', inplace=True)
    assert expected_df.equals(out_df)


def test_calc_sum(test_target_vars):
    """Ensure sum values are properly returned."""
    out = network_calc.calc_sum(
        test_df, test_target_vars, include_missing=True
    )
    expected = {'var1': 3.0,
                'var2': 12.0,
                'mn_var1': 50.0,
                'mn_var2': 0.0}

    assert out == expected


def test_calc_max(test_target_vars):
    """Ensure sum values are properly returned."""
    out = network_calc.calc_max(
        test_df, test_target_vars, include_missing=True
    )
    expected = {'var1': 2.0,
                'var2': 5.0,
                'mn_var1': 50.0,
                'mn_var2': 0.0}

    assert out == expected


def test_calc_min(test_target_vars):
    """Ensure sum values are properly returned."""
    out = network_calc.calc_min(
        test_df, test_target_vars, include_missing=True
    )
    expected = {'var1': 1.0,
                'var2': 3.0,
                'mn_var1': 50.0,
                'mn_var2': 0.0}

    assert out == expected


def test_calc_weighted_avg(test_target_vars):
    """Ensure sum values are properly returned."""
    out = network_calc.calc_weighted_avg(
        test_df, test_target_vars, include_missing=True
    )
    expected = {'var1': 1.6,
                'var2': 4.3,
                'mn_var1': 50.0,
                'mn_var2': 0.0}

    assert out == expected


def test_get_missing_data(test_target_vars):
    """This function is tested in calc tests above."""
    pass


def test_one_parent_to_df(local_data, test_target_vars):
    """This tests one parent to dataframe function using sum."""
    one_parent_ids = [1, 2, 4, 5, 15, 17]
    df = network_calc.one_parent_to_df(
        local_data, one_parent_ids, test_target_vars, include_missing=True
    )

    expected = [{'xstrm_id': 1,
                 'n_var1': 2.0, 'n_var2': 16.0,
                 'mn_var1': 0.0, 'mn_var2': 0.0},
                {'xstrm_id': 2,
                 'n_var1': 1.0, 'n_var2': 15.0,
                 'mn_var1': 0.0, 'mn_var2': 0.0},
                {'xstrm_id': 4,
                 'n_var1': 3.0, 'n_var2': 13.0,
                 'mn_var1': 0.0, 'mn_var2': 0.0},
                {'xstrm_id': 5,
                 'n_var1': 6.0, 'n_var2': None,
                 'mn_var1': 0.0, 'mn_var2': 100.0},
                {'xstrm_id': 15,
                 'n_var1': 16.0, 'n_var2': 2.0,
                 'mn_var1': 0.0, 'mn_var2': 0.0},
                {'xstrm_id': 17,
                 'n_var1': 20.0, 'n_var2': 20.0,
                 'mn_var1': 0.0, 'mn_var2': 0.0}
                ]
    expected_df = pd.DataFrame(expected)
    expected_df['mn_var1'] = expected_df['mn_var1'].astype('float64')

    expected_df.set_index("xstrm_id", inplace=True)

    assert expected_df.equals(df)


def test_combine_dfs(build_network_data, local_data, expected_out):
    """Test combine dfs by assessing final output dataframe."""
    # Using multiprocessing
    traverse_queue = build_network.build_network_setup(build_network_data[0])
    summary = build_network.build_calc_network(traverse_queue, include_seg=True)
    summary.add_processing_details(local_data, calc_type="weighted_avg")
    summary.calc_one_parent()
    summary.calc_no_parent()
    summary.calc_mult_parent()
    summary.combine_dfs()
    out_df = summary.final_df.sort_index().round(5)
    assert expected_out['n_var1'].equals(out_df['n_var1'])
    assert expected_out['n_var2'].equals(out_df['n_var2'])
    assert expected_out['mn_var1'].equals(out_df['mn_var1'])
    assert expected_out['mn_var2'].equals(out_df['mn_var2'])
