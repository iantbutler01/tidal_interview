## Basic Instructions

Replace the path at the top of cli.py with whatever the path to your python executable is.

Or at the very least run with python3.7

Install dependencies from the requirements.txt file.

`pip install -r requirements.txt`

If you've changed the path at the top of cli.py then mark the file executable with

`chmod +x ./cli.py`

And then run the pipeline with ./cli.py

Alternatively if you don't want to change the path just run cli.py with whatever your python3.7 path is.

E.g.

`python3 cli.py`

Assuming you've changed nothing else you can find the outputs of a successful run

at /tmp/tidal/comments and /tmp/tidal/submissions

Data taken from a sample is stored in tidal_data.zip and can be unzipped and explored with something like

parquet-tools.


