package com.romankagan.dse.demos.solr.stats;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.zip.DataFormatException;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.romankagan.bdp.search.solr.metrics.IndexReaderMetricsMXBean;
import com.romankagan.bdp.search.solr.metrics.UpdateMetrics.UpdatePhase;
import com.romankagan.bdp.search.solr.metrics.UpdateMetricsMXBean;
import com.romankagan.dse.demos.solr.CommandLine;
import org.HdrHistogram.Histogram;
import org.apache.lucene.index.ReaderEventListener.IndexReaderEvent;

/**
 * Helper to retrieve JMXMetrics from a Cassandra node. Not thread safe.
 */
public class JMXMetrics
{
    private static final Histogram EMPTY_HISTOGRAM = new Histogram(1L, 2L, 0);

    private final ExecutorService es = Executors.newFixedThreadPool(8, new ThreadFactoryBuilder().setNameFormat("JMX submit thread - %d").setDaemon(true).build());
    private final MBeanServerConnection connection;
    private final UpdateMetricsMXBean updateMetrics;
    private final IndexReaderMetricsMXBean indexSearcherMetrics;
    private final Map<Type, Histogram> totals = new ConcurrentHashMap<>();
    private final boolean collectLuceneMetrics;

    public JMXMetrics(CommandLine.Params params) throws Exception
    {
        String jmxUrl = "service:jmx:rmi:///jndi/rmi://";
        jmxUrl += new URI(params.urls.iterator().next()).getHost();
        jmxUrl += ":" + params.jmxPort + "/jmxrmi";
        JMXServiceURL jmxServiceURL = new JMXServiceURL(jmxUrl);
        connection = JMXConnectorFactory.connect(jmxServiceURL).getMBeanServerConnection();
        ObjectName updateMetricsObjectName = ObjectName.getInstance("com.romankagan.bdp:type=search,index=" + params.indexName + ",name=UpdateMetrics");
        updateMetrics = JMX.newMXBeanProxy(connection, updateMetricsObjectName, UpdateMetricsMXBean.class);
        ObjectName indexSearcherMetricsObjectName = ObjectName.getInstance("com.romankagan.bdp:type=search,index=" + params.indexName + ",name=IndexReaderMetrics");
        indexSearcherMetrics = JMX.newMXBeanProxy(connection, indexSearcherMetricsObjectName, IndexReaderMetricsMXBean.class);
        collectLuceneMetrics = params.includeLuceneMetrics;
    }

    /**
     * Get histograms containing values since the last time the values were checked.
     * Only valid to have one thread calling this.
     */
    public Map<Type, Histogram> getLatest()
    {
        Future<Histogram> updateQueue = fetchUpdateHistogram(Type.UPDATE_QUEUE, UpdatePhase.QUEUE);
        Future<Histogram> updatePrepare = fetchUpdateHistogram(Type.UPDATE_PREPARE, UpdatePhase.PREPARE);
        Future<Histogram> updateExecute = fetchUpdateHistogram(Type.UPDATE_EXECUTE, UpdatePhase.EXECUTE);
        Future<Histogram> indexReaderReopen = fetchIndexSearcherHistogram(Type.IR_REOPEN, IndexReaderEvent.READER_REOPEN);
        Future<Histogram> getIndexReader = fetchIndexSearcherHistogram(Type.IR_GET, IndexReaderEvent.GET_READER);
        Future<Histogram> applyDeletes = fetchIndexSearcherHistogram(Type.IR_APPLY_DELETES, IndexReaderEvent.APPLY_DELETES);

        try
        {
            ImmutableMap.Builder<Type, Histogram> builder = ImmutableMap.builder();
            builder.put(Type.UPDATE_QUEUE, updateQueue.get());
            builder.put(Type.UPDATE_PREPARE, updatePrepare.get());
            builder.put(Type.UPDATE_EXECUTE, updateExecute.get());
            builder.put(Type.IR_REOPEN, indexReaderReopen.get());
            builder.put(Type.IR_GET, getIndexReader.get());
            builder.put(Type.IR_APPLY_DELETES, applyDeletes.get());
            return builder.build();
        }
        catch (Throwable ignored)
        {
            return ImmutableMap.of();
        }
    }

    Map<Type, Histogram> getTotals()
    {
        if (getLatest().isEmpty())
        {
            return ImmutableMap.of();
        }
        return ImmutableMap.copyOf(totals);
    }

    private Future<Histogram> fetchUpdateHistogram(final Type type, final UpdatePhase phase)
    {
        return fetchHistogramAsync(() -> updateMetrics.getHdrHistogram(phase, null), type);
    }

    private Future<Histogram> fetchIndexSearcherHistogram(final Type type, final IndexReaderEvent phase)
    {
        if (!collectLuceneMetrics)
        {
            return CompletableFuture.completedFuture(incorporateHistogram(type, EMPTY_HISTOGRAM));
        }
        return fetchHistogramAsync(() -> indexSearcherMetrics.getHdrHistogram(phase), type);
    }

    private Future<Histogram> fetchHistogramAsync(Supplier<byte[]> latestHistogram, Type type) {
        return es.submit(() ->
        {
            byte[] histogramBytes = latestHistogram.get();
            Histogram histogram;
            try
            {
                histogram = Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(histogramBytes), 1);
            }
            catch (DataFormatException e)
            {
                throw new RuntimeException(e);
            }
            return incorporateHistogram(type, histogram);
        });
    }

    /*
     * Diff the last known values with the new ones, return the diff, store the new values.
     */
    private Histogram incorporateHistogram(Type type, Histogram histogram)
    {
        Histogram originalValues = totals.get(type);
        totals.put(type, histogram);
        if (originalValues == null)
        {
            return histogram;
        }
        else
        {
            Histogram copy = histogram.copy();
            copy.subtract(originalValues);
            return copy;
        }
    }

    public enum Type
    {
        UPDATE_QUEUE, UPDATE_PREPARE, UPDATE_EXECUTE, IR_REOPEN, IR_GET, IR_APPLY_DELETES
    }
}
