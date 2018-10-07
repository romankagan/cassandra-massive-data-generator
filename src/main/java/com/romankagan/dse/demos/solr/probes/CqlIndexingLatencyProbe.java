package com.romankagan.dse.demos.solr.probes;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.romankagan.driver.core.BoundStatement;
import com.romankagan.driver.core.PreparedStatement;
import com.romankagan.driver.core.ResultSet;
import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.SolrStress;
import com.romankagan.dse.demos.solr.clients.Clients;
import com.romankagan.dse.demos.solr.clients.CqlSession;
import com.romankagan.dse.demos.solr.stats.Metric;
import com.romankagan.dse.demos.solr.stats.Metrics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class CqlIndexingLatencyProbe implements IndexingLatencyProbe
{
    private final RateLimiter queryRateLimiter;
    private final ExecutorService es =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().
                            setDaemon(true).
                            setNameFormat("Index Latency Probe").build());
    private Metrics metrics;
    private final AtomicBoolean currentlyProbing = new AtomicBoolean(false);

    private Supplier<CqlSession> session;
    private final Supplier<PreparedStatement> statement;
    private final String pkeyColumnName;


    public CqlIndexingLatencyProbe(CommandLine.Params params, Metrics metrics)
    {
        this.metrics = metrics;
        //Probing is disabled
        if (params.indexingLatencyProbeRate == 0)
        {
            queryRateLimiter = null;
            currentlyProbing.set(true);
        }
        else
        {
            queryRateLimiter = RateLimiter.create(params.indexingLatencyProbeRate);
        }
        session = Suppliers.memoize(() -> Clients.getCqlClient(params.urls, params.clients, params.concurrency, metrics,
                params.cqlUsername, params.cqlPassword, params.cqlSSL, params.cqlCipherSuites, params.cqlSslProtocol,
                params.cqlKeystore, params.cqlKeystoreType, params.cqlKeystorePassword, params.cqlTruststore, params.cqlTruststoreType, params.cqlTruststorePassword,
                params.consistencyLevel));
        //A bit messy with the suppliers because the error for the CQL client should be generated elsewhere
        //rather then here first so init them lazily
        pkeyColumnName = params.indexingLatencyProbeColumn;
        statement = Suppliers.memoize(() ->
                                      session.get().prepare(String.format("SELECT * FROM %s WHERE solr_query=? LIMIT 1", params.indexName)));
    }

    public void maybeProbe(String id, long startNanos)
    {
        //For HTTP the column may not have been specified as a parameter resulting in no value being found
        //In this situation just silently don't do any probing
        //Checking for null here rather then replicating the code at each HTTP call site
        if (id == null)
        {
            return;
        }

        if (!currentlyProbing.get())
        {
            if (!currentlyProbing.compareAndSet(false, true))
            {
                return;
            }

            es.execute(()->
            {
                //If pointing Solr Stress at actual Solr which doesn't support CQL this will fail.
                //Let it fail quietly to the log file, and leaving currently probing set to true.
                //If pointing at actual DSE it's also probably OK since CQL will be in use and
                //the error will be reported by the other commands.
                CqlSession session;
                try
                {
                    session = this.session.get();
                }
                catch (Throwable t)
                {
                    SolrStress.logException(t);
                    return;
                }

                boolean success = true;
                try
                {
                    while (true)
                    {
                        queryRateLimiter.acquire();
                        BoundStatement boundStatement = statement.get().bind(pkeyColumnName + ":" + id);
                        ResultSet queryResult = session.delegate().execute(boundStatement);
                        if (queryResult.iterator().hasNext())
                        {
                            break;
                        }
                    }
                }
                catch (Throwable t)
                {
                    success = false;
                    SolrStress.logException(t);
                }
                finally
                {
                    currentlyProbing.set(false);
                    metrics.update(Metrics.StatsType.INDEX_LATENCY, startNanos, System.nanoTime(), success);
                }
            });
        }
    }
}
