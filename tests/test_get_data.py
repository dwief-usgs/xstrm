#!/usr/bin/env python

"""Tests for `get_data` module."""

from xstrm import get_data


def test_import_network_data():
    """Validate successful import and df characteristics of test data.

    Description
    ----------
    Assert df has correct field names.
    Assert that df has no nan values

    """
    test_file = 'tests/test_network.csv'
    df = get_data.import_network_data(test_file, 'seg', 'up_seg')
    assert df.columns.to_list() == ['seg_id', 'up_seg_id']
    assert (df.isnull().values.any()) == False


def test_import_local_data():
    """Validate successful import and df characteristics of test data.

    Description
    ----------
    Assert df has field_names 'seg_id' and 'seg_weight' are represented.

    """
    test_file = 'tests/test_local_data.csv'
    df = get_data.import_local_data(test_file, 'seg_id', 'area')
    df_cols = df.columns.to_list()
    list_wanted_cols = ['seg_id', 'seg_weight']
    assert all(x in df_cols for x in list_wanted_cols)
