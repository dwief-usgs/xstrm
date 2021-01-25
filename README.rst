================
xstrm
================

Python package to assist with stream network summarization.

* Free software: unlicense

Contact
--------
* Daniel Wieferich (dwieferich@usgs.gov)

Purpose
--------
Abundant data are currently available to describe the landscapes around us, yet the raw forms of these data are not always useful for scientific research and needs to be processed into appropriate spatial units for analyses. While studying streams, research suggests a stream and its condition can be characterized by accounting for the landscape draining to a stream segment and the landscape upstream and or downstream of the stream segment (network). 

This Python package and associated command line tool, 'network_calculator', is intended to assist with up and down stream network summarization of variables assigned to a stream segment. Methods are built in a generalized way and are intended to support efforts for any stream network having general topology i.e. to and from nodes.

Specifically, this package was built to support fisheries based analyses using multiple versions of the National Hydrography Database Plus (NHDPlus) that represent streams within the United States along with HydroBasins which represent global drainage areas.

Terminology 
-----------
* Segment: The smallest unit represented within a stream network. This could represent a stream segment (i.e. line segment) or local drainage unit (i.e. polygon).

* Network: All stream segments that relate to a given segment, where the relationship can be specified as all segments that flow to the segment of interest (i.e. upstream) or all segments that receive flow from the segment of interest (i.e. downstream).

* Children: In upstream implementations children are segments directly downstream of the segment of interest.  In downstream implementations children are segments directly upstream of the segment of interest.

* Parents: In upstream implementations parents are segments directly upstream of the segment of interest.  In downstream implementations parents are segments directly downstream of the segment of interest.

* NHDPlus: The National Hydrography Dataset Plus network is commonly used to represent streams in the United States. There are several versions of this dataset, each having slightly different schemas.

* Downstream implementation: Building a relationship between a segment and all (downstream) segments that receive flow from the segment of interest.

* Upstream implementation: Building a relationship between a segment and all (upstream) segments that flow to the segment of interest.


Currently Included 
------------------
* Python methods (build_network.py, network_calc.py, xstrm.py) and command line tool (network_calculator.py) to support upstream or downstream summaries of information attributed to local stream segments or drainages. Summary types currently supported include sum, min, max, or weighted average.

* Ability to export a complete network to hdf5 file format. Note, networks are exported using index values to improve processing efficiency and reduce size of the hdf5 file.

* For a given network return all upstream or downstream segment or drainage identifiers.

* A mock network is included in tests folder for convenience of testing and understanding functionality. An image of the network, diagram_of_test_data.JPG, along with network data, test_local_data.csv, are included.

* common_networks folder, contains processing steps for commonly used stream networks such as NHDPlusV2.1.

Requirements
------------
Requirements.txt shows condensed version of packages, while requirements_dev shows a full list of packages used in development.

Getting Started
---------------
Install the package

pip install from main branch using the below command.  Users can also install working branches by adding @branch_name

* pip install git+https://github.com/dwief-usgs/xstrm.git


All of the examples below will run as-is assuming the file, 'tests/test_local_data.csv', is locally accessible in a folder named 'tests' under the working directory. These data correspond to the test network depicted in the file 'diagram_of_test_data.jpg'.  These data contain network to/from nodes alongside local data and expected data and therefore 'drop_cols' parameter is required to help remove unneeded information. 

**Example 1**  Using the network calculator command line tool. The network calculator is intended to simplify use of xstrm methods.  The calculator has been tested in Linux command line and anaconda prompt on Windows.

.. code-block::

    # Access the help menu to see all parameter options and brief description of each
    python -m xstrm.network_calculator --help

    # Example of how to run the code. This example uses the 'test_local_data.csv' where both network and local data are available.  The process runs a 'sum' calculation by default on 'var1' and 'var2' columns of data.  Note, a number of columns are included in the csv that depict results and therefor we need to specify drop_cols so that all columns are not calculated. 
    python -m xstrm.network_calculator --to_from_csv=tests/test_local_data.csv --local_data_csv=tests/test_local_data.csv --id_col_name=seg_id --to_node_col=down_node --from_node_col=up_node --weight_col_name=area --drop_cols=["up_node","down_node","up_area","max_var1","max_var2","min_var1","min_var2","sum_var1","sum_var2","weighted_var1","weighted_var2","up_only_sum_var1","mn_var1","mn_var2"]


**Example 2a** Use build_network methods to build and export the network to a local hdf file.

.. code-block:: python

    # Set user variables
    to_from_file = "tests/test_local_data.csv"
    id_col = "seg_id"
    to_node_col = "down_node"
    from_node_col = "up_node"
    hdf_file = "tests/test.hd5"
    include_seg = True

    # Get and prep network data
    build_network_data = build_network.import_tofrom_csv(
        to_from_file, id_col, to_node_col, from_node_col
    )

    travers_queue = build_network.build_network_setup(
        build_network_data[0]
    )

    # Build the network, export to hdf5 and build summary object containing information for network calculations
    network = build_network.build_hdf_network(
        traverse_queue, hdf_file, include_seg
    )

    # Print lists of segments with multiple parents, segments with one parent, and segments with no parents. Note in this example a parent represents upstream segments.  To/From nodes can be flipped in Example 2a to return parents representing downstream segments.
    print (f"List of segment indices with multiple parents: {network.multi_parent_ids}.")
    print (f"List of segment indices with one parent: {network.one_parent_ids}.")
    print (f"List of segment indices with no parents: {network.no_parent_ids}.")

    # Print relationship between index value ('xstrm_id') and user submitted identifier ('seg_id')
    print (build_network_data[1])


**Example 2a results** of print statements. Note these lists are index values (referenced as 'xstrm_id') that are related to user ids ('seg_id' in this case).  The relationship between the ids is captured in the variable build_network_data[1].

.. code-block::

    List of segment indices with multiple parents: [3, 6, 14, 7, 8, 10, 9, 11, 12, 13, 16].
    List of segment indices with one parent: [1, 2, 4, 5, 15, 17].
    List of segment indices with no parents: [].

        seg_id  xstrm_id
    0      01         1
    1      02         2
    2      03         3
    3      04         4
    4      05         5
    5      06         6
    6      07         7
    7      08         8
    8      09         9
    9      10        10
    10     11        11
    11     12        12
    12     13        13
    13     14        14
    14     15        15
    15     16        16
    16     17        17



**Example 2b** Retrieve numpy array of parents for a segment from hdf file.  

.. code-block:: python

    from xstrm import build_network
    import h5py

    # Use file from Example 2a
    hdf_file = "tests/test.hd5"

    with h5py.File(hdf_file,'r') as h5:
        l10 = build_network.get_parents_hdf(h5, 10)
    
    print (l10)


**Example 2b results** from print statement.  Displays list of parent index values for segment index 10. 

.. code-block::

    [ 1  2  3  4  5  6  7 10]



Copyright and License
---------------------
This USGS product is considered to be in the U.S. public domain, and is licensed under unlicense_

.. _unlicense: https://unlicense.org/

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.


Acknowledgements
----------------
* Original concepts from Tsang, Yin-Phan, Daniel Wieferich, Kuolin Fung, Dana M. Infante, and Arthur R. Cooper. 2014. An approach for aggregating upstream catchment information to support research and management of fluvial systems across large landscapes. SpringerPlus, vol. 3, no. 589. https://doi.org/10.1186/2193-1801-3-589
* This work was supported by funding from the USGS Community for Data Integration (CDI).  The CDI project (FY2016) National Stream Summarization: Standardizing Stream-Landscape Summaries Project and all those involved contributed guidance and concepts used in this effort.
* This work was supported by the USGS National Climate Adaptation Science Center


* This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
