About the solr_stress utility
-----------------------------

The search stress demo is a benchmark tool to demonstrate the capabilities of your Search cluster. It will simulate a number of requests to read and write data to the Search cluster over a sequential or random set of iterations, as specified on a "test data" file.

Before starting this demo, be sure that you have started romankagan Enterprise with Solr enabled on one or more nodes. See "Starting DSE and DSE Search" in the romankagan documentation (http://docs.romankagan.com/en/romankagan_enterprise/latest/romankagan_enterprise/srch/srchInstall.html).

Directory structure
----------------

Directory structure should look as follows

├── resources
│   ├── schema
│   │   ├── create-schema-geo-rt.sh
│   │   ├── create-schema-geo.sh
│   │   ├── create-schema.sh
│   │   ├── create_table.cql
│   │   ├── create_table_geo.cql
│   │   ├── create_table_geo_rt.cql
│   │   ├── delete_index_demo_geo.cql
│   │   ├── delete_index_demo_geort.cql
│   │   ├── delete_index_demo_solr.cql
│   │   ├── delete-index-geo-rt.sh
│   │   ├── delete-index-geo.sh
│   │   └── delete-index.sh
│   ├── testCqlQuery.txt
│   ├── testCqlWrite.txt
│   ├── testGenerateIndexLatencyTest.txt
│   ├── testGenerateQueries.txt
│   ├── testLucRead.txt
│   ├── testLoadGeoCql.txt
│   ├── testLoadGeoHttp.txt
│   ├── testMixed.txt
│   ├── testQuery.txt
│   ├── testUpdate.txt
│   └── testWrite.txt
├── run-benchmark.sh
├── download-geonames.sh
└── solr_stress.jar

The 'resources' directory contains a set of example input files for solr_stress. Under 'schema' subdirectory there are scripts for creating CQL keyspaces, tables and Solr cores.

Running
----------------

1. Open a shell window or tab and make the solr_stress directory your current directory. The location of the demo directory depends on your platform:

RPM-Redhat or Debian installations:

    cd  /usr/share/demos/solr_stress

Tar distribution, such as Mac:

    cd ~/dse-*/demos/solr_stress


2. Open another shell window or tab and create the schema:

    cd resources/schema
    ./create-schema.sh [options]

    CQL Table Creation Options:
        -h HOST     hostname or IP for Solr HTTP requests
        --ssl       use SSL for Cassandra table creation over cqlsh
        -u USERNAME authentication username
        -p PASSWORD authentication password

3. Invoke run-benchmark.sh with no arguments to see the arguments and their documentation.

4. Writing a testData file:

Execution modes

You can specify RANDOM, SEQUENTIAL or SHARED_SEQUENTIAL execution modes for the list of commands:
    * RANDOM - a single command from a test file will be executed, chosen randomly.
    When ran with '--loops=N' flag, N commands will be executed instead.  The number of clients does not impact the number of commands run under RANDOM.
    * SEQUENTIAL - all commands from a test file will be executed in the order specified in a test file.
    When ran with '--loops=N' flag, the whole file will be executed N times. When ran with multiple clients '--clients=C', every client wil execute whole file N times. Overall every command will be executed up to N x C times.
    * SHARED_SEQUENTIAL - all commands from a test file will be executed, but the test file is shared between all clients. It means that multiple clients will concurrently execute a shared list of commands.
    When ran with '--loops=N' flag, the whole file will be executed N times. When ran with multiple clients '--clients=C', commands from a test file will be executed concurrently. Overall every command will be executed up to N times.

Available commands

    * HTTP read (HTTPREAD), see examples:
        ./run-benchmark.sh --url=http://localhost:8983 --test-data=resources/testQuery.txt --solr-core=demo.solr
        ./run-benchmark.sh --url=http://localhost:8983 --test-data=resources/testMixed.txt --solr-core=demo.solr
    you can specify the expected number of results such as: HTTPREAD|q=body:benchmark|100 (100 expected results)
    * CQL commands can be executed using the syntax `CQL|<CQL command>`, see examples:
        ./run-benchmark.sh --url=http://localhost:8983 --test-data=resources/testCqlQuery.txt --solr-core=demo.solr
        ./run-benchmark.sh --url=http://localhost:8983 --test-data=resources/testCqlWrite.txt --solr-core=demo.solr
    * P_STMNT will run CQL commands as prepared statements for you using the syntax `P_STMNT|<CQL command with ? markers>|value1|value2|valueN` such as
    `P_STMNT(3)|insert into demo.geo (id, timezone) values (?, ?)|'$RANDOM_UUID'|'America/Toronto'`, see example:
    	./run-benchmark.sh --url=http://localhost:8983 --test-data=resources/testCqlWrite.txt --solr-core=demo.solr
    * Lucene perf package test tasks (LUCPERF only some supported), see example:
        ./run-benchmark.sh --url=http://localhost:8983 --test-data=resources/testLucRead.txt --solr-core=demo.solr
    currently supports: High/Med/LowTerm, Prefix3, Wildcard, Fuzzy1/2, AndHigh/High/Med/Low, OrHigh/High/Med/Low, IntNRQ, HighPhrase, HighSloppyPhrase, MedPhrase, MedSloppyPhrase, LowPhrase and LowSloppyPhrase
  There're also commands which will result in generating a whole test case:
    * GENERATE_QUERIES, see example:
        ./run-benchmark.sh --url=http://localhost:8983 --test-data=resources/testGenerateQueries.txt --solr-core=demo.geo
    generates a random set of search queries (both term and boolean), using terms fetched from Solr's TermsComponent. Generated queries are saved to a file in a format readable for Solr stress tool, either as `CQL` or `HTTPREAD` commands.
    Example:
        SHARED_SEQUENTIAL
        GENERATE_QUERIES|fields=country,name,timezone,published|terms-limit=500|boolean-queries=25000|term-queries=5000|output=queries-http.txt|api=HTTP
        GENERATE_QUERIES|fields=country,name,timezone,published|terms-limit=500|boolean-queries=25|term-queries=25|output=queries-cql.txt|api=CQL
    The first command will create 30 000 queries (`boolean-queries=25000` + `term-queries=5000`) and store them to a file (`output=queries-http.txt`) as a set of `HTTPREAD` commands (`api=HTTP` parameter), whereas the second file will create 50 queries (`boolean-queries=25` + `term-queries=25`) and store them (`output=queries-cql.txt`) as a set of `CQL` commands (`api=CQL` parameter). In both cases, queries will be generated using 4 index fields `fields=country,name,timezone,published`. Generator will fetch up to 500 terms (`terms-limit=500`) for each field and construct queries from them. Later on both output files can be consumed by the Solr stress tool.
    Note: If other test file header than SHARED_SEQUENTIAL were used, this command would be repeated --clients or --loops times. To avoid redundant executions, use SHARED_SEQUENTIAL.
    * LOAD, bulk load via , see example:
        ./run-benchmark.sh --url=http://localhost:8983 --test-data=resources/testLoadGeoCql.txt --solr-core=demo.geo
        testLoadGeoCql.txt:
        SHARED_SEQUENTIAL
        LOAD|protocol=CQL|loader-class=GEO|input-file=allCountries.txt|limit=0|config-yaml={ "synthesize_primary_key":false, "starting_primary_key":0 }
        The "protocol" argument specifies the access method (CQL). The "loader-class" specifies the input reader to use to process the input file. Currently there is only a reader implemented for the geonames data set. You can implement your own reader and use the class name to have it instantiated via reflection. The "input-file" argument is the path to the input file. The "limit" argument specifies a limit on the number of lines to load with 0 mapping to Integer.MAX_VALUE. The geoname input reader enforces the limit and also supports synthesizing the primary key to extend the data set as well as specifying the starting primary key. This is done via the "config-yaml" argument which is YAML that is passed to the input reader implementation as a map and allows for input reader specific configuration.

Additional notes

- Each field can contain unique or randomly generated data via the $RANDOM_UUID, $RANDOM_XX, $IPSUM_XX or $ZIPF_XX notations. These will be substituted respectively by either a UUID, a random number up to the specified integer, a random number following a ZIPF distribution, or a random string of words from Lorem Ipsum vocabulary: $RANDOM_100 will get string replaced by an uniformly sampled random number between 0 and 100 each time that command is run. On the other hand, $ZIPF_100 will get string replaced by a random number between 1 and 100, but in this case lower ranking numbers (e.g., 1) will appear much more times than higher rank numbers (e.g., 69). Finally, $IPSUM_100 will generate a 100 word sentence with words randomly picked from a Lorem Ipsum dictionary. You can also specify a number of times a RANDOM should occur: $RANDOM_1000:10 to get 10 random numbers.
- You can specify a number of repetitions so that the same command gets executed N number of times: HTTPREAD(5) will run this read command 5 times
- You can add comments to your test data files via the # char
- You can escape the |, & and = special characters by doubling them ||, && and == if you need them as doc content. No need to escape them in cql queries

Let's see an example to stress test the demo.solr core.

SEQUENTIAL
HTTPREAD|wt=json&q=text:benchmark&facet=true&facet.field=type

5. Index latency probing

By default Solr Stress will probe indexing latency (time from write acknowledgement to write visiblity) using CQL as an access method.

A thread will sample a single write at a time if writes are performed via a bulk load, or CQL insert via a prepared statement. Non-prepared CQL is not probed because it would require parsing the CQL statement to find the values to probe with.

The thread will by default submit at most 200 queries/second and this can be configured with --indexing-probe-rate. Set to 0 to disable probing.

It is necessary to specify the column that is the unique field for documents if it is not "id" via the --indexing-probe-column parameter. If you don't specify it the benchmark will still run but probing will not be performed.

After the benchmark completes a histogram of documents that were sampled for indexing latency will be printed. Latency is measured from the time the write is acknowledged as committed and not from when it is submitted.

6. Reading the results:

When the test is done, you should see an output like this:

./run-benchmark.sh --loops=10000
Starting Benchmark...

        Date & Time GMT, Success total, Failure total, % complete, ops/second, 99%tile ms, 99.9%tile ms
2016.03.09-21:53:01.374,          6203,             0,      20.68,       1261,      7.683,       21.215
2016.03.09-21:53:06.385,         13551,             0,      45.17,       1466,      7.459,       19.263
2016.03.09-21:53:11.395,         20844,             0,      69.48,       1455,      7.223,       20.447
2016.03.09-21:53:16.400,         28138,             0,      93.79,       1457,      7.459,       20.367
2016.03.09-21:53:21.408,         35496,             0,     118.32,       1469,      7.367,       19.423
2016.03.09-21:53:26.416,         42261,             0,     140.87,       1350,      7.795,       26.719
2016.03.09-21:53:31.423,         49510,             0,     165.03,       1447,      7.547,       19.279
2016.03.09-21:53:36.433,         56627,             0,     188.76,       1420,      7.355,       13.135
2016.03.09-21:53:41.440,         63810,             0,     212.70,       1434,      7.435,       20.063
2016.03.09-21:53:46.446,         71059,             0,     236.86,       1448,      7.371,       20.607
2016.03.09-21:53:51.456,         78044,             0,     260.15,       1394,      7.535,       21.135
2016.03.09-21:53:56.461,         84809,             0,     282.70,       1351,      8.039,       25.439
2016.03.09-21:54:01.466,         91684,             0,     305.61,       1373,      7.643,       17.103
2016.03.09-21:54:06.474,         97921,             0,     326.40,       1245,      8.791,       30.191
2016.03.09-21:54:11.478,        104323,             0,     347.74,       1279,      8.431,       23.487

Committing
Finished at 2016.03.09-21:54:16.220 GMT, Final duration 1.33 minutes rate 1379.26 ops/second with 0 errors
Latency histogram in milliseconds
       Value     Percentile TotalCount 1/(1-Percentile)

       0.010 0.000000000000          5           1.00
       0.016 0.500000000000      55133           2.00
       0.034 0.750000000000      82978           4.00
       0.054 0.875000000000      96310           8.00
       6.799 0.937500000000     103148          16.00
       7.191 0.968750000000     106578          32.00
       7.643 0.984375000000     108296          64.00
       8.099 0.992187500000     109142         128.00
       9.479 0.996093750000     109572         256.00
      13.111 0.998046875000     109786         512.00
      22.063 0.999023437500     109893        1024.00
      35.007 0.999511718750     109947        2048.00
      38.591 0.999755859375     109974        4096.00
      48.511 0.999877929688     109987        8192.00
     121.791 0.999938964844     109994       16384.00
     160.767 0.999969482422     109997       32768.00
     169.983 0.999984741211     109999       65536.00
     171.647 0.999992370605     110000      131072.00
     171.647 1.000000000000     110000
#[Mean    =        0.716, StdDeviation   =        2.695]
#[Max     =      171.520, Total count    =       110000]
#[Buckets =           22, SubBuckets     =         2048]
Doc read count histogram, queries that read 0 docs 0
       Value     Percentile TotalCount 1/(1-Percentile)

  487935.000 0.000000000000        107           1.00
  537087.000 0.500000000000       5018           2.00
  563199.000 0.750000000000       7619           4.00
  575487.000 0.875000000000       8847           8.00
  581119.000 0.937500000000       9434          16.00
  584703.000 0.968750000000       9793          32.00
  586239.000 0.984375000000       9915          64.00
  587263.000 0.992187500000      10000         128.00
  587263.000 1.000000000000      10000
#[Mean    =   537246.861, StdDeviation   =    28888.649]
#[Max     =   586752.000, Total count    =        10000]
#[Buckets =           22, SubBuckets     =         2048]
Done. Check the file exceptions.log for any warnings.

The test will provide feedback of how many commands have been run every 5 seconds, progress towards completion, operations/second, and latency for P99 and P99.9. These statistics are calculated over the 5 second period since the last statistics were reported.
After the benchmark a summary histogram will be logged for operations across the entire benchmark along with a histogram of # of documents returned by read queries.
You can check the exceptions.log for any errors that might have occurred
