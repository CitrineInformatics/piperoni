.. Piperoni documentation master file, created by
   sphinx-quickstart on Fri Jul 10 11:49:51 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Piperoni's documentation!
======================================

Piperoni is a lightweight ETL framework for any data type, which allows you to make, track, and visualize atomic data transformations. 
Unlike some ETL tools, Piperoni relies on in-memory transformation, and thus is ideal for manipulating complex, diverse non-"big"-data.


Piperoni allows you to make and track atomic data transformations, ensures expected types are being passed from transformation to transformation, and allows you to easily see the state of the data at any point in time.
Piperoni is a great tool for collaborative data pipelines, where visibility into data transformations is key.

Piperoni allows you to:

- Chain custom and built-in :ref:`Operators <operators>` together to transform data.
- Use custom or built-in :ref:`Extractors <extractors>` to pull data from any format and transform it.
- Use custom or built-in :ref:`Transformers <transformers>` to manipulate data for featurization, normalization, etc.
- Create custom :ref:`Casters <casters>` to explicitly cast from one datatype to another (e.g. dict to pandas DataFrame).
- :ref:`Log <logging>` every operation before and after transformation.
- Converge and split many :ref:`Pipes <pipes>` to form a :ref:`Pipeline <pipelines>`.

Installation
------------

You can install piperoni via pip:

.. code::

   $ pip install piperoni

Table of Contents
-----------------

.. toctree::
   :maxdepth: 2
   :numbered: 4

   quickstart
   operators/index
   pipelines
   logging

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
