Feature Factory (Fork Edition)
==============================

This project is forked from `Databricks Feature Factory Github Repo`_ and refactored to adopt some of the more modern libraries, tools and techniques.

Few of the notable changes are:

* Restructure the modules (directory-level only) to delineate the distinction between core framework code and demo examples
* Replace *setuptools* with *Poetry* for dependency management and packaging
* Adopt *pytest fixtures* over the classic xUnit style of setup/teardown functions to make tests more explicit, modular and scalable

Please note that the codebase here is not backward compatible with the original.
The directory structure has been altered as the skeleton structure is created by
leveraging Poetry.


Installation
------------
* Python 3.9+ (please confirm via *pyproject.toml*)
* Install `Poetry`_

Basic Usage
-----------

Here are the steps on how to run this project on your machine locally:

1. Clone this project
2. Navigate to the cloned project directory
3. Activate the virtual environment: ``poetry shell``
4. Install dependencies: ``poetry install``
5. Run all tests: ``poetry run pytest``
6. Build deployable package: ``poetry build``

That's pretty much it! Do check out `Poetry Basic Usage`_ for other command and more advanced usage.


How to run on Databricks
------------------------

Here are the steps to run a sample notebook leveraging this framework in Databricks:

1. Create a cluster in Databricks Workspace:

* I have only tested against Databricks Runtime Version 11.1 ML
* For other runtime, please ensure it matches dependencies noted in `pyproject.toml`_
* Poetry would ensure these compatibilities are honored (it will fail when you attempt to install incompatible new library)

2. In the new cluster, install new library by dropping the Python Whl file found in `dist` folder locally
3. In your own Users Workspace, import the notebook found in `Simple Example Notebook`_
4. Connect to your newly created cluster and click `Run All`


Troubleshooting
----------------

You may encounter the following exception when running tests::

  E   : java.net.BindException: Can't assign requested address: Service 'sparkDriver'
  failed after 16 retries (on a random free port)! Consider explicitly setting the appropriate
  binding address for the service 'sparkDriver' (for example spark.driver.bindAddress for SparkDriver)
  to the correct binding address.


To resolve, you can try the following on the command line::

  export SPARK_LOCAL_IP="127.0.0.1"

For further details, please `Spark BindException`_


Original README
---------------
For original readme file, please go to `Original README`_

Credit
------
I would like to thank the original contributor and author `GeekSheikh`_ and other contributors to this amazing project.



.. _Databricks Feature Factory Github Repo: https://github.com/databrickslabs/feature-factory
.. _Poetry: https://python-poetry.org/docs/
.. _Poetry Basic Usage: https://python-poetry.org/docs/basic-usage/
.. _GeekSheikh: https://github.com/GeekSheikh
.. _Spark BindException: https://github.com/dotnet/spark/issues/435
..  _Original README: ./original-README.md
.. _Simple Example Notebook: ./demo_databricks/Simple_Example.py
.. _pyproject.toml: ./pyproject.toml
