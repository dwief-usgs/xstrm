"""Command line tool for stream network summaries."""

from xstrm import xstrm
import click
import timeit


@click.command()
@click.option('--to_from_csv', required=True, help='Enter CSV file name, including extension. File should contain segment id, to_node, from_node.')
@click.option('--local_data_csv', required=True, help='Enter CSV file name, including extension. File should contain segment id and local segment values for each variable to calculate')
@click.option('--id_col_name', required=True, help='Name of column representing segment identifier. In NHDPlus this is COMID.')
@click.option('--to_node_col', required=True, help='Name of column representing from node. In NHD this is the downstream node.')
@click.option('--from_node_col', required=True, help='Name of column representing from node. In NHD this is the upstream node.')
@click.option('--weight_col_name', required=False, show_default=True, default=None, help='If applicable name of weight column (e.g. area or length)')
@click.option('--calc_type', required=False, show_default=True, default='sum', help='Calculation type: options include sum, max, min, weighted_avg')
@click.option('--include_seg', required=False, show_default=True, default=True, help='Include processing segment as part of the network')
@click.option('--include_missing', required=False, show_default=True, default=True, help='Include calculation for percent of network with missing data for each variable')
@click.option('--num_proc', required=False, show_default=True, default=4, help='Number of worker processes to use in multiprocessing')
@click.option('--precision', required=False, show_default=True, default=3, help='Number of decimals to round float data')
@click.option('--drop_cols', required=False, show_default=True, default=[], help='List of comma seperated strings reprenting field names to not include in network summary but are found in the local_data.csv file')
def handle_data(
    to_from_csv,
    local_data_csv,
    id_col_name,
    to_node_col,
    from_node_col,
    weight_col_name,
    calc_type,
    include_seg,
    include_missing,
    num_proc,
    precision,
    drop_cols
):
    """Command line tool for up or down stream network summaries.

    Description:
    Command line tool for processing network summaries. This tool
    accepts a CSV with topology (i.e. to from nodes) and a CSV with
    local segment data. It uses this information to calculate network
    summaries for each segment, where a segment can be a stream
    segment or catchment associated with the nodes. Parameters can
    be used to control how the network is built, calculation type,
    and processing details. The tool exports data to a CSV file.

    """
    t = timeit.default_timer()
    click.echo("Processing now...")
    click.echo("Depending on network size and hardware this could take several hours.")

    xstrm.network_calc_to_csv(
        to_from_csv,
        local_data_csv,
        id_col_name,
        to_node_col,
        from_node_col,
        weight_col_name,
        calc_type,
        include_seg,
        include_missing,
        num_proc,
        precision,
        drop_cols)

    seconds = (timeit.default_timer() - t)
    click.echo(f"Process completed in approximately {seconds} seconds")


if __name__ == "__main__":
    handle_data()
