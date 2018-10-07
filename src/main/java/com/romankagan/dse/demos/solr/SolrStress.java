package com.romankagan.dse.demos.solr;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.romankagan.bdp.tools.ClientSecurityUtils;
import com.romankagan.dse.demos.solr.clients.Clients;
import com.romankagan.dse.demos.solr.probes.CqlIndexingLatencyProbe;
import com.romankagan.dse.demos.solr.probes.IndexingLatencyProbe;
import com.romankagan.dse.demos.solr.readers.InputReaderStrategy;
import com.romankagan.dse.demos.solr.readers.InputReaderStrategySelector;

import com.romankagan.dse.demos.solr.commands.CmdRunner;
import com.romankagan.dse.demos.solr.commands.CmdRunnerFactory;
import com.romankagan.dse.demos.solr.stats.JMXMetrics;
import com.romankagan.dse.demos.solr.stats.Metrics;

import com.google.common.util.concurrent.RateLimiter;

public class SolrStress
{
    //Shutdown tasks are run in reverse order of addition
    public static final Deque<Callable> shutdownTasks = new LinkedBlockingDeque<>();
    private static final AtomicInteger threadId = new AtomicInteger(0);
    private static final AtomicBoolean testRunHadExceptions = new AtomicBoolean(false);
    public static PrintStream exceptionStream;

    public static void logException(Throwable t)
    {
        testRunHadExceptions.lazySet(true);
        t.printStackTrace(exceptionStream);
    }

    public static void main(String[] args) throws Exception {
        try {
            solrStress(args);
        } catch (Throwable t) {
            System.err.println("Exception thrown:");
            t.printStackTrace();
            throw t;
        }
    }

    public static void solrStress(String[] args) throws Exception
    {
        try
        {
            ClientSecurityUtils.initClientSecurity();
        }
        catch (Exception e)
        {
            System.out.println("Fatal error when initializing Solr clients, exiting");
            e.printStackTrace();
            System.exit(1);
        }

        exceptionStream = new PrintStream(new FileOutputStream("exceptions.log", false), true, "UTF-8");

        CommandLine.Params params = CommandLine.parse(args);

        List<PrintStream> outputTargets = new ArrayList<>();
        outputTargets.add(System.out);
        if (params.outputStatsFile != null)
        {
            File statsFile = new File(params.outputStatsFile);

            // Output statistics to csv file
            System.out.println("Writing statistics file " + statsFile.getAbsolutePath());
            if (statsFile.getParent() != null)
            {
                statsFile.getParentFile().mkdirs();
            }
            PrintStream ps = new PrintStream(new FileOutputStream(statsFile), true, "UTF-8");
            outputTargets.add(ps);
        }

        InputReaderStrategy inputReader = new InputReaderStrategySelector(params.random, params.loops)
                .createInputReader(new File(params.testDataFileName));

        JMXMetrics jmxMetrics = new JMXMetrics(params);

        if (jmxMetrics.getLatest().isEmpty())
        {
            System.out.println(String.format("\nWARNING: Search index metrics are not available. The solr core %s may not be loaded.\n",
                                             params.indexName));
        }

        System.out.println("Starting Benchmark...\n");
        ExecutorService threadPool = Executors.newFixedThreadPool(params.clients);
        //On overflow use Long.MAX_VALUE, Java does specify overflow behavior is wrapping so this is safe.
        long startTimeNanos = System.nanoTime();
        long endTimeNanos = startTimeNanos + params.durationNanos < startTimeNanos ? Long.MAX_VALUE : startTimeNanos + params.durationNanos;
        Metrics metrics =
                new Metrics(params.reportInterval,
                            TimeUnit.valueOf(params.reportIntervalUnit),
                            inputReader.getExpectedEvents(params.clients),
                            outputTargets,
                            endTimeNanos,
                            jmxMetrics);
        IndexingLatencyProbe indexingLatencyProbe = new CqlIndexingLatencyProbe(params, metrics);

        for (int i = 0; i < params.clients; i++)
        {
            threadPool.execute(new StressRunner(params, inputReader, metrics, indexingLatencyProbe, endTimeNanos));
        }
        threadPool.shutdown();
        threadPool.awaitTermination(365, TimeUnit.DAYS);

        if (metrics.hasEvents() && !jmxMetrics.getLatest().isEmpty())
        {
            System.out.println("\nCommitting");
            for (String url : params.urls)
            {
                try
                {
                    Clients.getHttpSolrClient(url, params.consistencyLevel).commit(true, true);
                }
                catch(Exception e)
                {
                    logException(e);
                    e.printStackTrace();
                }
            }
        }

        Iterator<Callable> shutdownTaskIterator = shutdownTasks.descendingIterator();
        while (shutdownTaskIterator.hasNext())
        {
            try
            {
                shutdownTaskIterator.next().call();
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                logException(t);
            }
        }

        if (testRunHadExceptions.get())
        {
            System.out.println("\n\n***************************************************************************************");
            System.out.println("*** There were exceptions during the test run. Please check the exceptions.log file ***");
            System.out.println("***************************************************************************************\n\n");
        }

        metrics.finalReport();

        System.out.println("Done. Check the file exceptions.log for any warnings.");

        System.exit(0);
    }

    //Indicate the runner should terminate silently
    public static class TerminateException extends RuntimeException {}

    private static class StressRunner implements Runnable
    {
        private final int myThreadId = SolrStress.threadId.getAndIncrement();
        private final CommandLine.Params params;
        private final InputReaderStrategy inputReader;
        private final Metrics metrics;
        private final IndexingLatencyProbe indexingLatencyProbe;
        private final long endTimeNanos;

        public StressRunner(CommandLine.Params params, InputReaderStrategy inputReader, Metrics metrics, IndexingLatencyProbe indexingLatencyProbe, long endTimeNanos)
        {
            this.params = params;
            this.inputReader = inputReader;
            this.metrics = metrics;
            this.indexingLatencyProbe = indexingLatencyProbe;
            this.endTimeNanos = endTimeNanos;
        }

        @Override
        public void run()
        {
            HashMap<String, CmdRunner> cmdRunners = new HashMap<>();

            // First line of testData specifies random or sequential behavior
            RateLimiter rateLimiter = params.rateLimit;

            int acquiredPermits = 0;
            // In random Mode loops = number of commands to run.
            // In seq Mode loops = number of times to loop the whole testData

            List<String> cmd2Run;
            while (System.nanoTime() < endTimeNanos & !(cmd2Run = inputReader.getNextCommand()).isEmpty())
            {
                try
                {
                    String cmdType = CmdRunnerFactory.getCmdType(cmd2Run);
                    CmdRunner cmdRunner = null;
                    if ((cmdRunner = cmdRunners.get(cmdType)) == null)
                    {
                        try
                        {
                            cmdRunner = CmdRunnerFactory.getInstance(cmd2Run, params, metrics, indexingLatencyProbe);
                            cmdRunners.put(cmdType, cmdRunner);
                        }
                        catch (Throwable t)
                        {
                            //If a command fails to load it should fail fast so the config
                            //can be fixed
                            t.printStackTrace();
                            logException(t);
                            System.exit(0);
                        }
                    }

                    String repeatsStr = cmd2Run.get(0).replaceAll("[^\\d]", "");
                    long repeats = repeatsStr.equals("") ? 1 : Long.parseLong(repeatsStr);
                    while (repeats > 0)
                    {
                        //Batch acquisition of permits to amortize synchronization cost of rate limiting
                        if (acquiredPermits == 0)
                        {
                            int toAcquire = rateLimiter.getRate() <= 100 ? 1 : 5;
                            rateLimiter.acquire(toAcquire);
                            acquiredPermits = toAcquire;
                        }
                        acquiredPermits--;

                        cmdRunner.executeCommand(cmd2Run);
                        repeats--;
                    }
                    metrics.logExecutedCommands(1);
                }
                catch (TerminateException e)
                {
                    //No need to log, error is logged elsewhere
                    break;
                }
                catch (Throwable e)
                {
                    //Generally exceptions caught here mean the workload is corrupt and wasn't able
                    //to start. Can't connect, can't parse a command. Treat them as fatal.
                    //Errors executing a configured command are handled by CmdRunner.
                    e.printStackTrace();
                    exceptionStream.println("Exception processing this command: " + Arrays.toString(cmd2Run.toArray()));
                    logException(e);
                    exceptionStream.println("--------------------------------------------------------------------------");
                    System.exit(1);
                }
            }
        }
    }
}
