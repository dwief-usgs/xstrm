"""Builds out fixtures of test data for package tests."""
import pytest
from xstrm import build_network
from xstrm import network_calc


@pytest.fixture(scope="session")
def build_network_data():
    """Import to from data. Ensure warning is passed for nan."""
    with pytest.warns(None) as record:
        test_file = "tests/test_local_data.csv"
        return build_network.import_tofrom_csv(
            test_file, "seg_id", to_node_col="down_node", from_node_col="up_node"
        )
    assert len(record) == 1


@pytest.fixture(scope="session")
def local_data(build_network_data):
    """Imports test data with variable values of test network."""
    indx_df = build_network_data[1]
    test_file = "tests/test_local_data.csv"
    drop_vars = ['up_node', 'down_node', 'up_area', 'max_var1', 'min_var1',
                 'sum_var1', 'weighted_var1', 'max_var2', 'min_var2', 'sum_var2',
                 'weighted_var2', 'up_only_sum_var1', 'mn_var2', 'mn_var1']
    df = network_calc.get_local_vars_csv(test_file, indx_df, "seg_id", "area", drop_vars)
    return df


@pytest.fixture(scope="session")
def expected_out(build_network_data):
    """Imports test output data."""
    test_file = "tests/test_local_data.csv"
    indx_df = build_network_data[1]
    drop_vars = ['var1', 'var2', 'up_node', 'down_node', 'up_area', 'max_var1', 'min_var1',
                 'sum_var1', 'max_var2', 'min_var2', 'sum_var2' , 'up_only_sum_var1']
    df = network_calc.get_local_vars_csv(test_file, indx_df, "seg_id", "area", drop_vars)
    col_rename = {'weighted_var1': 'n_var1', 'weighted_var2': 'n_var2'}
    df = df.rename(col_rename, axis="columns")
    df = df.drop(['seg_weight'], axis="columns")
    df['mn_var1'] = df['mn_var1'].astype(float)
    df = df.sort_index().round(5)
    return df
