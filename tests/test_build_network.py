#!/usr/bin/env python

"""Tests `build_network` module. conftest.py has data fixtures."""

import pytest
import pandas as pd
from xstrm import build_network
import os


@pytest.fixture(scope='module')
def traverse_queue(build_network_data):
    return build_network.build_network_setup(build_network_data[0])


@pytest.fixture(scope='module')
def network_include_seg(build_network_data):
    t = build_network.build_network_setup(build_network_data[0])
    return build_network.build_network(t, include_seg=True)


@pytest.fixture(scope='module')
def network_no_seg(build_network_data):
    t = build_network.build_network_setup(build_network_data[0])
    return build_network.build_network(t, include_seg=False)


def test_StreamSegment():
    # Test StreamSegment class init
    expected_dict = {'xstrm_id': 1,
                     'children': [],
                     'parents': [],
                     'visited_parent_cnt': 0,
                     'all_parents': {}
                     }
    seg = build_network.StreamSegment(1)
    assert seg.__dict__ == expected_dict
    # test update of seg
    seg.all_parents.update({seg.xstrm_id: seg})
    seg.update_all_parents()
    assert seg.all_parents == [1]


def test_build_network_setup(traverse_queue):
    traverse_queue1 = traverse_queue
    """Test build network for correct output queue."""
    # Queue should only represent headwater streams, correct cnt is 6
    assert len(traverse_queue1) == 6

    # Ensure all segs start out having
    # no parents, no all_parents, and visited parent count of 0
    p = [x.parents for x in traverse_queue1 if x.parents != []]
    assert len(p) == 0
    all_p = [x.all_parents for x in traverse_queue1 if x.all_parents != {}]
    assert len(all_p) == 0
    c = [x.children for x in traverse_queue1 if x.children != []]
    assert len(c) == 5


def test_build_network(network_include_seg, network_no_seg):
    """Test build network for correct output queue."""
    # Test that output is dataframe
    assert isinstance(network_include_seg, list)
    # Test length of list equals number of segments in network
    assert len(network_include_seg) == 17
    # Test parent list if include seg
    i = [s for s in network_include_seg if s.xstrm_id == 12][0]
    actual = i.all_parents
    expect = [1, 2, 3, 4, 5, 6, 7, 10, 8, 9, 12]
    assert sorted(expect) == actual

    # Test parent list if not include seg
    i = [s for s in network_no_seg if s.xstrm_id == 12][0]
    actual = i.all_parents
    expect = [1, 2, 3, 4, 5, 6, 7, 10, 8, 9]
    assert sorted(expect) == actual


def test_build_calc_network(build_network_data):
    """Test build_calc_network."""
    # Duplicative rebuild of queue but queue destructs in previous test
    traverse_queue2 = build_network.build_network_setup(build_network_data[0])
    network = build_network.build_calc_network(
        traverse_queue2, include_seg=True
    )
    assert network.no_parent_ids == []
    assert network.one_parent_ids == sorted([1, 2, 4, 5, 15, 17])
    expect_multi_parent = [{'xstrm_id': 3, 'parents': [1, 2, 3]},
                           {'xstrm_id': 6, 'parents': [4, 5, 6]},
                           {'xstrm_id': 14, 'parents': [15, 14]},
                           {'xstrm_id': 7, 'parents': [4, 5, 6, 7]},
                           {'xstrm_id': 8, 'parents': [4, 5, 6, 8]},
                           {'xstrm_id': 10, 'parents': [1, 2, 3, 4, 5, 6, 7, 10]},
                           {'xstrm_id': 9, 'parents': [4, 5, 6, 8, 9]},
                           {'xstrm_id': 11, 'parents': [4, 5, 6, 8, 11]},
                           {'xstrm_id': 12, 'parents': [1, 2, 3, 4, 5, 6, 7, 10, 8, 9, 12]},
                           {'xstrm_id': 13, 'parents': [4, 5, 6, 8, 11, 1, 2, 3, 7, 10, 9, 12, 13]},
                           {'xstrm_id': 16, 'parents': [15, 14, 4, 5, 6, 8, 11, 1, 2, 3, 7, 10, 9, 12, 13, 16]}]
    assert expect_multi_parent == network.multi_parent_ids


def test_build_hdf_network(build_network_data):
    # Duplicative rebuild of queue but queue destructs in previous test
    traverse_queue3 = build_network.build_network_setup(build_network_data[0])
    outhdf = 'test.hd5'
    summary = build_network.build_hdf_network(
        traverse_queue3, outhdf, include_seg=True
    )
    expected_no = []
    expected_multi = sorted([3, 6, 14, 7, 8, 10, 9, 11, 12, 13, 16])
    expected_one = sorted([1, 2, 4, 5, 15, 17])
    assert sorted(summary.no_parent_ids) == expected_no
    assert sorted(summary.multi_parent_ids) == expected_multi
    assert sorted(summary.one_parent_ids) == expected_one
    os.remove(outhdf)


def test_import_tofrom_csv(build_network_data):
    """Test import from tofrom has correct format."""
    # Test that output is dataframe
    assert isinstance(build_network_data[0], pd.DataFrame)
    # Test output has xstrm_id as index and 'up_xstrm_id' as column
    assert build_network_data[0].index.name == 'xstrm_id'
    assert build_network_data[0].columns == ['up_xstrm_id']
    assert build_network_data[0].shape[1] == 1
    assert sorted(build_network_data[1].columns.to_list()) == sorted(['xstrm_id', 'seg_id'])
    assert build_network_data[1].shape[1] == 2


def test_get_parent_list(build_network_data):
    """Test get network list using known network."""
    traverse_queue = build_network.build_network_setup(build_network_data[0])
    network = build_network.build_network(traverse_queue)
    expected = [1, 10, 12, 2, 3, 4, 5, 6, 7, 8, 9]
    plist = build_network.get_parent_list(network, 12)
    assert sorted(expected) == plist


def test_indx_to_id():
    """Test index_to_id method."""
    t_data_df = pd.DataFrame([{"tid": "001", "var1": 10.0},
                              {"tid": "020", "var1": 11.0}])
    t_indx_df = pd.DataFrame([{"xstrm_id": 1, "var1": 10.0},
                              {"xstrm_id": 3, "var1": 11.0}])
    indx_df = pd.DataFrame([{"tid": "001", "xstrm_id": 1},
                            {"tid": "020", "xstrm_id": 3}])
    test1 = build_network.indx_to_id(t_indx_df, indx_df, "tid", need="id_col_name")
    assert sorted(test1.columns.to_list()) == sorted(["var1", "tid"])
    test2 = build_network.indx_to_id(t_data_df, indx_df, "tid", need="xstrm_id")
    assert sorted(test2.columns.to_list()) == sorted(["var1", "xstrm_id"])
