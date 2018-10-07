package com.romankagan.dse.demos.solr;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

import com.romankagan.driver.core.ConsistencyLevel;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.ParseException;
import io.airlift.airline.SingleCommand;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

import com.romankagan.dse.demos.solr.clients.Clients;

public class CommandLine
{

    public static Params parse(String[] args)
    {
        try
        {
            if (args.length == 0)
                args = new String[] { "--help" };

            Params params = SingleCommand.singleCommand(Params.class).parse(args);

            if (!params.helpOption.showHelpIfRequested())
                return params.complete();
        }
        catch (ParseException ex)
        {
            System.err.println(ex.getMessage());
            System.exit(1);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
        return null;
    }

    @Command(name = "run-benchmark.sh", description = "DSE Search stress tool")
    public static class Params
    {
        @Inject
        public HelpOption helpOption;

        @Option(required = false, name = {"--clients"}, description = "Default 1. Number of client threads to generate requests. You will need several as load generation is very CPU intensive.")
        public int clients = 1;

        @Option(required = false, title = "loops", name = {"--loops"}, description = "Default 1. Number times to run the input file per client if SEQUENTIAL, number of operations from input file if RANDOM.")
        public Long loopsRaw = null;
        public long loops = Long.MAX_VALUE;

        @Option(required = false, title = "duration", name = {"--duration"}, description = "Default not used. Period of time to run the benchmark specified as an integer suffixed with a time unit. D for days, H for hours, M for minutes, S for seconds. Default time unit is seconds.")
        public String durationRaw = null;
        public long durationNanos = Long.MAX_VALUE;

        @Option(required = false, name = {"--fetch-size"}, title = "fetch size", description = "Default Integer.MAX_VALUE. Number of results to fetch at a time.")
        public int fetchSize = Integer.MAX_VALUE;

        @Option(required = false, name = {"--solr-core"}, title = "index name", description = "Default \"demo.solr\". Solr core to query and commit.")
        public String indexName = "demo.solr";

        @Option(required = false, name = {"--url"}, title = "url1,url2,...urlN", description = "Default \"http://localhost:8983\". Solr core to query and commit.")
        private String urlsString;
        public Set<String> urls = new HashSet<>();

        @Option(required = false, name = {"--test-data"}, title = "test data file", description = "Default \"resources/testMixed.txt\". Test data file to read commands from.")
        public String testDataFileName = "resources/testMixed.txt";

        @Option(required = false, name = {"--random-seed"}, title = "random seed", description = "Defaults to a random seed. Integer seed for the RNG.")
        private Long randomSeed = null;
        public Supplier<Random> random;

        @Option(required = false, name = {"--qps"}, description = "Default Double.MAX_VALUE. Rate limit number of queries per second.")
        public int qps = -1;
        public final RateLimiter rateLimit = RateLimiter.create(Double.MAX_VALUE);

        @Option(required = false, name = {"--output-stats"}, title = "output file", description = "Default doesn't output a stats file. Output a stats file in addition to stdout. File will be truncated.")
        public String outputStatsFile = null;

        @Option(required = false, name = {"--report-interval"}, title = "report interval", description = "Default 5. Interval of time to report statistics.")
        public int reportInterval = 5;

        @Option(required = false, name = {"--report-interval-timeunit"}, title = "report interval unit", description = "Default \"SECONDS\". Unit of time report interval is defined in.")
        public String reportIntervalUnit = "SECONDS";

        @Option(required = false, name = {"--concurrency"}, title = "Maximum # of concurrent requests", description = "Default 128. Maximum number of concurrent requests at any given time.")
        public int concurrency = 128;

        @Option(required = false, name = {"--indexing-probe-rate"}, title = "Indexing probe query rate", description = "Default 0 (off). Limit on queries per second checking for indexed document visibility. Set to 0 to disable probing.")
        public Integer indexingLatencyProbeRate = 0;

        @Option(required = false, name = {"--indexing-probe-column"}, title = "Column name to use for indexing latency probe pkey", description = "Default \"id\". Name of the primary key field/column.")
        public String indexingLatencyProbeColumn = "id";

        @Option(required = false, name = {"--include-lucene-metrics"}, title = "Include Lucene metrics", description = "Default false. Includes Lucene metrics (e.g. IndexReader latencies) in the final test report.")
        public boolean includeLuceneMetrics;

        @Option(required = false, name = {"--jmxPort"}, title = "JMX port to connect to for metrics", description = "Default 7199. JMX port to connect to for metrics")
        public int jmxPort = 7199;

        @Option(required = false, name = {"--consistency-level", "-cl"}, title = "Consistency level to use for requests", description = "Default LOCAL_ONE. Consistency level to use for requests.")
        public String consistencyLevelString = "LOCAL_ONE";
        public ConsistencyLevel consistencyLevel;

        @Option(required = false, name = {"--cqlUsername"}, title = "Native protocol authentication username", description = "If your DSE cluster requires authentication, specify username and password using this parameter and --cqlPassword. Note that authentication credentials specified on the command line is insecure and can be inspected by other users logged into the machine and via the shell history.")
        public String cqlUsername;

        @Option(required = false, name = {"--cqlPassword"}, title = "Native protocol authentication password", description = "If your DSE cluster requires authentication, specify username and password using this parameter and --cqlUsername. Note that authentication credentials specified on the command line is insecure and can be inspected by other users logged into the machine and via the shell history.")
        public String cqlPassword;

        @Option(required = false, name = {"--cqlSSL"}, title = "Use SSL for native protocol", description = "Use this to enable SSL for native protocol.")
        public boolean cqlSSL;

        @Option(required = false, name = "--cqlCipherSuites", title = "Cipher suites to use")
        public String cqlCipherSuites;

        @Option(required = false, name = "--cqlSslProtocol", title = "SSL protocol to use")
        public String cqlSslProtocol = "TLS";

        @Option(required = false, name = "--cqlKeystore", title = "File containing the keystore")
        public String cqlKeystore;

        @Option(required = false, name = "--cqlKeystoreType", title = "Type of the keystore (defaults to JKS)")
        public String cqlKeystoreType = "JKS";

        @Option(required = false, name = "--cqlKeystorePassword", title = "Password required to access the keystore")
        public String cqlKeystorePassword;

        @Option(required = false, name = "--cqlTruststore", title = "File containing the truststore")
        public String cqlTruststore;

        @Option(required = false, name = "--cqlTruststoreType", title = "Type of the truststore (defaults to JKS)")
        public String cqlTruststoreType = "JKS";

        @Option(required = false, name = "--cqlTruststorePassword", title = "Password required to access the truststore")
        public String cqlTruststorePassword;

        private Params complete()
        {

            //Duration vs Loops defaults and processing
            if (loopsRaw != null && durationRaw != null)
            {
                throw new IllegalArgumentException("Can't specify both --loops and --duration");
            }

            //If nothing is set default to one loop
            if (loopsRaw == null && durationRaw == null)
            {
                loops = 1;
            }
            else
            {
                //If loops is what is set process that
                if (loopsRaw != null)
                {
                    loops = loopsRaw;
                    if (loopsRaw < 1)
                    {
                        throw new IllegalArgumentException(String.format("Loops specified as %d, but must be > 0", loopsRaw));
                    }
                }

                //If duration is what is set process that
                if (durationRaw != null)
                {
                    durationRaw = durationRaw.trim().toUpperCase();
                    if (durationRaw.isEmpty())
                    {
                        throw new IllegalArgumentException("Duration can't be empty");
                    }

                    char lastChar = durationRaw.charAt(durationRaw.length() - 1);
                    String numericPortion = durationRaw;
                    TimeUnit unit = TimeUnit.SECONDS;
                    if (Character.isAlphabetic(lastChar))
                    {
                        numericPortion = numericPortion.substring(0, numericPortion.length() - 1);
                        switch (lastChar)
                        {
                            case 'D':
                                unit = TimeUnit.DAYS;
                                break;
                            case 'H':
                                unit = TimeUnit.HOURS;
                                break;
                            case 'M':
                                unit = TimeUnit.MINUTES;
                                break;
                            case 'S':
                                unit = TimeUnit.SECONDS;
                                break;
                            default:
                                throw new IllegalArgumentException("\"" + lastChar + "\" is not a supported time unit. Use one of DHMS.");
                        }
                    }
                    durationNanos = unit.toNanos(Long.parseLong(numericPortion));
                }
            }

            if (concurrency < 1)
            {
                throw new IllegalArgumentException(String.format("--concurrency parameter must be >= 1, specified value was %d", concurrency));
            }

            if (concurrency < clients)
            {
                throw new IllegalArgumentException(String.format("--concurrency (%d) is < --clients (%d), this would mean only one client could run at a time", concurrency, clients));
            }

            if (indexingLatencyProbeRate < 0)
            {
                throw new IllegalArgumentException(String.format("--indexing-probe-rate must be >= 0. Value was %d", indexingLatencyProbeRate));
            }

            if (urlsString != null)
            {
                for (String url : urlsString.split(","))
                {
                    urls.add(url + "/solr/" + indexName);
                }
            }
            else
            {
                urls = ImmutableSet.of("http://localhost:8983/solr/" + indexName);
            }

            if (qps != -1)
            {
                rateLimit.setRate(qps);
            }

            if (randomSeed != null)
            {
                if (clients > 1)
                {
                    System.err.println("Setting Random seed doesn't work with multiple clients because the RNG is used concurrently from multiple threads");
                    System.exit(1);
                }
                System.out.printf("Using random seed %d%n", randomSeed);
                random = Suppliers.ofInstance(new Random(randomSeed));
            }
            else if (clients == 1)
            {
                long seed = ThreadLocalRandom.current().nextLong();
                System.out.printf("Using random seed %d%n", seed);
                random = Suppliers.ofInstance(new Random(seed));
            }
            else
            {
                random = new Supplier<Random>()
                {
                    @Override
                    public Random get()
                    {
                        return ThreadLocalRandom.current();
                    }
                };
            }

            consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelString.toUpperCase());

            reportIntervalUnit = reportIntervalUnit.toUpperCase();

            Clients.initHTTPClient(urls.size(), clients);

            return this;
        }
    }
}
