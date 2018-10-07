package com.romankagan.dse.demos.solr.load;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.romankagan.dse.demos.solr.probes.IndexingLatencyProbe;
import org.apache.cassandra.utils.Throwables;

import com.romankagan.driver.core.BoundStatement;
import com.romankagan.driver.core.PreparedStatement;
import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.Utils;
import com.romankagan.dse.demos.solr.clients.Clients;
import com.romankagan.dse.demos.solr.clients.CqlSession;
import com.romankagan.dse.demos.solr.stats.Metrics;

public class CQLLoadStrategy extends LoadStrategy
{

    private final ExecutorService submissionExecutor;
    private final CqlSession session;
    private final PreparedStatement insertStatement;

    public CQLLoadStrategy(CommandLine.Params params, Metrics metrics, InputLoader reader, IndexingLatencyProbe indexingLatencyProbe)
    {
        super(params, metrics, reader, indexingLatencyProbe);
        this.submissionExecutor = Utils.buildExecutor("cql-submission-", params.clients, new ArrayBlockingQueue<Runnable>(params.clients));
        this.session = Clients.getCqlClient(params.urls, params.clients, params.concurrency, metrics,
                params.cqlUsername, params.cqlPassword, params.cqlSSL, params.cqlCipherSuites, params.cqlSslProtocol,
                params.cqlKeystore, params.cqlKeystoreType, params.cqlKeystorePassword, params.cqlTruststore, params.cqlTruststoreType, params.cqlTruststorePassword,
                params.consistencyLevel);

        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ").append(params.indexName).append(" (");
        List<String> columns = reader.columns();
        for (int ii = 0; ii < columns.size(); ii++)
        {
            query.append(columns.get(ii));
            if (ii + 1 != columns.size())
            {
                query.append(", ");
            }
        }
        query.append(") VALUES (");
        for (int ii = 0; ii < columns.size(); ii++)
        {
            query.append("?");
            if (ii + 1 != columns.size())
            {
                query.append(", ");
            }
        }
        query.append(");");

        System.out.println("Preparing \"" + query + "\"");
        this.insertStatement = session.prepare(query.toString());
    }

    @Override
    public void execute()
    {
        Throwable mergedException = null;
        try
        {
            Iterator<Object[]> iterator = reader.iterator();
            Object[][] docs = new Object[20][];
            int arrayIndex = 0;
            while (iterator.hasNext())
            {
                docs[arrayIndex++] = iterator.next();
                if (arrayIndex % 20 == 0)
                {
                    addDocs(docs, arrayIndex);
                    docs = new Object[20][];
                    arrayIndex = 0;
                }
            }
            addDocs(docs, arrayIndex);
        }
        catch (Throwable thrown)
        {
            mergedException = Throwables.merge(mergedException, thrown);
        }
        finally
        {
            mergedException = Throwables.merge(mergedException, close(mergedException));
        }

        Throwables.maybeFail(mergedException);
    }

    private Throwable close(Throwable mergedException)
    {
        try
        {
            submissionExecutor.shutdown();
            submissionExecutor.awaitTermination(365, TimeUnit.DAYS);
        }
        catch (Throwable t)
        {
            return Throwables.merge(mergedException, t);
        }
        return mergedException;
    }

    private void addDocs(final Object[][] docs, final int docCount)
    {
        submissionExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                for (int ii = 0; ii < docCount; ii++)
                {
                    addDoc(docs[ii]);
                }
                //A white lie, they aren't executed yet, but close enough
                //to drive the progress indicator
                metrics.logExecutedCommands(docCount);
            }
        });
    }

    private void addDoc(final Object[] fields)
    {
        BoundStatement boundStatement = insertStatement.bind(fields);
        session.executeAsync(boundStatement, (String)fields[reader.pkeyColumnIndex()], indexingLatencyProbe);
    }

 }
