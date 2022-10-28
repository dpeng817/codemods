.. These are examples of badges you might want to add to your README:
   please update the URLs accordingly

    .. image:: https://api.cirrus-ci.com/github/<USER>/codemods.svg?branch=main
        :alt: Built Status
        :target: https://cirrus-ci.com/github/<USER>/codemods
    .. image:: https://readthedocs.org/projects/codemods/badge/?version=latest
        :alt: ReadTheDocs
        :target: https://codemods.readthedocs.io/en/stable/
    .. image:: https://img.shields.io/coveralls/github/<USER>/codemods/main.svg
        :alt: Coveralls
        :target: https://coveralls.io/r/<USER>/codemods
    .. image:: https://img.shields.io/pypi/v/codemods.svg
        :alt: PyPI-Server
        :target: https://pypi.org/project/codemods/
    .. image:: https://img.shields.io/conda/vn/conda-forge/codemods.svg
        :alt: Conda-Forge
        :target: https://anaconda.org/conda-forge/codemods
    .. image:: https://pepy.tech/badge/codemods/month
        :alt: Monthly Downloads
        :target: https://pepy.tech/project/codemods
    .. image:: https://img.shields.io/twitter/url/http/shields.io.svg?style=social&label=Twitter
        :alt: Twitter
        :target: https://twitter.com/codemods

.. image:: https://img.shields.io/badge/-PyScaffold-005CA0?logo=pyscaffold
    :alt: Project generated with PyScaffold
    :target: https://pyscaffold.org/

|

========
codemods
========

Modifies dagster code that uses solids/pipelines/modes into code that uses ops/jobs.

In order to use these codemods, editable install this repo, install the `run.sh` script using `chmod +x run.sh`, and run the script on the directory you would like to codemod.
