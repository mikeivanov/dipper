Dipper
======

Dipper is a tool for dumping data from SQL databases in a smart way.

Screenshots
-----------

Dumping a whole table:

    $ dipper --database mysql://test:secret@127.0.0.1:3306/test \
             --table things \
             --columns id,name
    1       Thing #1
    2       Thing #2

Using config files:

    $ cat dipper.ini
    [dipper]
    username = test
    password = secret

    $ dipper --database mysql://127.0.0.1/test \
             --table things \
             --config dipper.ini
    1       Thing #1
    2       Thing #2

Using environment variables for default values:

    $ export DIPPER_DATABASE=mysql://127.0.0.1/test

    $ dipper --config dipper.ini --table things
    1       Thing #1
    2       Thing #2

Receipts capture the last incremental dump state:

    $ dipper --config dipper.ini --table things \
             --incremental id \
             --receipt things.receipt
    1       Thing #1
    2       Thing #2

    $ cat things.receipt
    [receipt]
    table = things
    incremental = id
    last-value = 2

    $ dipper --config dipper.ini --table things \
             --incremental id \
             --receipt things.receipt

The last run had no output because all known record had already been dumped.
Lest's add a new one.

    $ mysql -e 'INSERT INTO things (id, name) VALUES (3, "No Thing")'

    $ dipper --config dipper.ini --table things \
             --incremental id \
             --receipt things.receipt
    3       No Thing

    $ cat things.receipt
    [receipt]
    table = things
    incremental = id
    last-value = 3


Supported Databases
-------------------

* MySQL - yes
* SQLite3 - yes
* PostgreSQL - not yet
* ODBC - planned

Usage
-----

    dipper [options]

    Options:
        --help                         Show this text and exit.
        --database=DATABASE            Database connection URI
        --table=TABLE                  Table name
        --columns=COL1,COL2,...,COLN   A comma-separated column list
        --incremental=COLUMN           Do an incremental dump using this column
        --last-value=VALUE             The last seen value in the incremental column
        --limit=N                      Limit results to the N first rows (debug only)
        --username=USERNAME            Log in to DATABASE as USERNAME
        --password=PASSWORD            Log in to DATABASE using PASSWORD
        --output=PATH                  Store results to PATH. Default: stdout
        --receipt=PATH                 Read and write receipt file at PATH
        --config=PATH                  Read config from PATH
