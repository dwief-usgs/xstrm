================
xstrm
================

Python package to assist with stream network summarization.


* Free software: unlicense

Contact
--------
* Daniel Wieferich (dwieferich@usgs.gov)

Purpose
-------
Python package to assist with stream network summarization. This package is intended to support efforts for any stream network having general topology (i.e. to/from nodes). Specifically this package was built to support fisheries based analyses using multiple versions of the National Hydrography Database Plus (NHDPlus) representing streams within the United States along with HydroBasins which represent global drainage areas.

Terminology 
-----------
* Segment: The smallest unit represented within a network. This could represent a stream segment or local drainage unit.

* NHDPlus: The National Hydrography Dataset Plus network is commonly used to represent streams in the United States. There are several versions of this dataset, each having slightly different schemas.


Currently Included 
------------------
* Python methods and command line tool to support upstream or downstream summaries of information attributed to local stream segments or drainages. Summary types currently supported include sum, min, max, or weighted average.

* For a given network return all upstream or downstream segment or drainage identifiers.

* A mock network is included in tests folder for convenience of testing. An image of the network, diagram_of_test_data.JPG, along with network data, test_local_data.csv, are included.

Requirements
------------
Requirements.txt shows condensed version of packages, while requirements_dev shows a full list of packages used in development.

Getting Started
---------------
Install the package

Soon users will be able to pip install from main branch
* pip install git+https://github.com/dwief-usgs/xstrm.git


**Example 1**  Using the network calculator command line tool.

.. code-block::

    # Access the help menu to see all parameter options and brief description of each
    python -m xstrm.network_calculator --help

    # Example of how to run the code. User needs to update variables after each '='
    python -m xstrm.network_calculator --to_from_csv=data/tofrom_file.csv --local_data_csv=data/test_data.csv --id_col_name=COMID --to_node_col=ToNode --from_node_col=FromNode

Copyright and License
---------------------
This USGS product is considered to be in the U.S. public domain, and is licensed under unlicense_

.. _unlicense: https://unlicense.org/

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.

Acknowledgements
----------------
* Original concepts from https://doi.org/10.1186/2193-1801-3-589
* This work was supported by funding from the USGS Community of Data Integration (CDI).  The CDI project (FY2016) National Stream Summarization: Standardizing Stream-Landscape Summaries Project and all those involved contributed guidance and concepts used in this effort.
* This work was supported by the National Climate Adaptation Science Center

* This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
