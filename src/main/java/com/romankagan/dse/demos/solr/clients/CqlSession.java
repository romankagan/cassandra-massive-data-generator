package com.romankagan.dse.demos.solr.clients;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import com.romankagan.driver.core.ConsistencyLevel;
import com.romankagan.driver.core.PreparedStatement;
import com.romankagan.driver.core.ResultSet;
import com.romankagan.driver.core.ResultSetFuture;
import com.romankagan.driver.core.Row;
import com.romankagan.driver.core.Session;
import com.romankagan.driver.core.Statement;
import com.romankagan.dse.demos.solr.SolrStress;
import com.romankagan.dse.demos.solr.Utils;
import com.romankagan.dse.demos.solr.probes.IndexingLatencyProbe;
import com.romankagan.dse.demos.solr.stats.Metrics;

public class CqlSession
{
    private static final Object zeroInflightCommandsCondition = new Object();
    private static final AtomicLong inflightCommands = new AtomicLong(0);

    private static final ExecutorService resultListenerExecutor =
            Utils.buildExecutor(
                    "cql-result-fetcher-",
                    Runtime.getRuntime().availableProcessors() * 2,
                    new LinkedBlockingQueue<Runnable>());

    final Metrics metrics;

    private final Session delegate;

    private final Semaphore inflightCommandLimit;

    CqlSession(Session delegate, Metrics metrics, int inflightCommandLimit)
    {
        this.delegate = delegate;
        this.metrics = metrics;
        this.inflightCommandLimit = new Semaphore(inflightCommandLimit);
    }

    /**
     * Asynchronously executes the statement and pages through the results if any are returned.
     *
     * Does not return a handle for each statement. Use blockOnInflightCommands() to find out
     * when all queued operations have completed.
     * @param statement Statement to execute
     * @param probeId String with the ID of the document probe to be used to probe for indexing latency
     * @param indexingLatencyProbe Indexing latency probe to use with the provided ID, can be null
     * @return The time submission began in nanoseconds as returned by System.nanoTime()
     */
    public long executeAsyncAndPageResults(Statement statement, String probeId, IndexingLatencyProbe indexingLatencyProbe)
    {
        long start = System.nanoTime();
        ResultSetFuture result = delegate.executeAsync(statement);
        inflightCommands.incrementAndGet();
        try
        {
            inflightCommandLimit.acquire();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        FutureCallback<ResultSet> fc = new PagingReadCallback(start, probeId, indexingLatencyProbe);
        Futures.addCallback(result, fc, Utils.DIRECT_EXECUTOR);
        return start;
    }

    /**
     * Asynchronously executes the statement.
     *
     * Does not return a handle for each statement. Use blockOnInflightCommands() to find out
     * when all queued operations have completed.
     * @param statement Statement to execute
     * @param probeId String with the ID of the document probe to be used to probe for indexing latency
     * @param indexingLatencyProbe Indexing latency probe to use with the provided ID, can be null
     * @return The time submission began in nanoseconds as returned by System.nanoTime()
     */
    public long executeAsync(Statement statement, String probeId, IndexingLatencyProbe indexingLatencyProbe)
    {
        long start = System.nanoTime();
        ResultSetFuture result = delegate.executeAsync(statement);
        inflightCommands.incrementAndGet();
        try
        {
            inflightCommandLimit.acquire();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        FutureCallback<ResultSet> fc = new ResultCallback(start, probeId, indexingLatencyProbe);
        Futures.addCallback(result, fc, Utils.DIRECT_EXECUTOR);
        return start;
    }

    public PreparedStatement prepare(String query)
    {
        return delegate.prepare(query);
    }

    public void blockOnInflightCommands() throws InterruptedException
    {
        synchronized (zeroInflightCommandsCondition)
        {
            while (inflightCommands.get() > 0)
            {
                zeroInflightCommandsCondition.wait();
            }
        }
    }

    public Session delegate()
    {
        return delegate;
    }

    private void decrementInflightCommands()
    {
        if (inflightCommands.decrementAndGet() == 0)
        {
            synchronized (zeroInflightCommandsCondition)
            {
                zeroInflightCommandsCondition.notifyAll();
            }
        }
        inflightCommandLimit.release();
    }

    /*
     * For reads that want to page through and fetch results. Hands off to a thread
     * pool if paging is necessary.
     */
    private class PagingReadCallback implements FutureCallback<ResultSet>
    {
        private final long start;
        private final String probeId;
        private final IndexingLatencyProbe indexingLatencyProbe;

        private PagingReadCallback(long start, String probeId, IndexingLatencyProbe indexingLatencyProbe)
        {
            this.start = start;
            this.probeId = probeId;
            this.indexingLatencyProbe = indexingLatencyProbe;
        }

        @Override
        public void onSuccess(@Nullable final ResultSet rs)
        {
            try
            {
                //Avoid the extra thread handoff if possible when results are fully fetched.
                if (rs.isFullyFetched())
                {
                    long nowNanos = System.nanoTime();
                    metrics.update(Metrics.StatsType.OP_LATENCY, start, nowNanos, true);
                    int numDocsRead = rs.getAvailableWithoutFetching();
                    metrics.update(Metrics.StatsType.DOC_COUNT, numDocsRead, true);
                    decrementInflightCommands();
                    if (indexingLatencyProbe != null)
                    {
                        indexingLatencyProbe.maybeProbe(probeId, nowNanos);
                    }
                }
                else
                {
                    //Slow path, have the executor fetch the results to count.
                    resultListenerExecutor.execute(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            boolean failure = true;
                            try
                            {
                                int numDocsRead = 0;
                                for (Row row : rs)
                                {
                                    numDocsRead++;
                                }
                                metrics.update(Metrics.StatsType.OP_LATENCY, start, System.nanoTime(), true);
                                metrics.update(Metrics.StatsType.DOC_COUNT, numDocsRead, true);
                                failure = false;
                            }
                            finally
                            {
                                if (failure)
                                {
                                    metrics.update(Metrics.StatsType.OP_LATENCY, start, System.nanoTime(), false);
                                }
                                decrementInflightCommands();
                            }
                        }
                    });
                }
            }
            catch (Throwable t)
            {
                try
                {
                    SolrStress.logException(t);
                    throw new RuntimeException(t);
                }
                finally
                {
                    decrementInflightCommands();
                }
            }
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            try
            {
                metrics.update(Metrics.StatsType.OP_LATENCY, start, System.nanoTime(), false);
                SolrStress.logException(throwable);
            }
            finally
            {
                decrementInflightCommands();
            }
        }

    }

    /*
     * For writes or non-paging reads just measure the time to initial response
     */
    private class ResultCallback implements FutureCallback<ResultSet>
    {
        private final long start;
        private final String probeId;
        private final IndexingLatencyProbe indexingLatencyProbe;

        private ResultCallback(long start, String probeId, IndexingLatencyProbe indexingLatencyProbe)
        {
            this.start = start;
            this.probeId = probeId;
            this.indexingLatencyProbe = indexingLatencyProbe;
        }

        @Override
        public void onSuccess(ResultSet result)
        {
            try
            {
                long nowNanos = System.nanoTime();
                metrics.update(Metrics.StatsType.OP_LATENCY, start, nowNanos, true);
                if (indexingLatencyProbe != null)
                    indexingLatencyProbe.maybeProbe(probeId, nowNanos);
            }
            finally
            {
                decrementInflightCommands();
            }
        }

        @Override
        public void onFailure(Throwable t)
        {
            try
            {
                metrics.update(Metrics.StatsType.OP_LATENCY, start, System.nanoTime(), false);
                SolrStress.logException(t);
            }
            finally
            {
                decrementInflightCommands();
            }
        }
    };

}
