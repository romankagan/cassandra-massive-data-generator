package com.romankagan.dse.demos.solr.probes;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.romankagan.driver.core.ResultSet;
import com.romankagan.driver.core.Row;
import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.clients.Clients;
import com.romankagan.dse.demos.solr.clients.MockCqlClusterFactory;
import com.romankagan.dse.demos.solr.stats.Metric;
import com.romankagan.dse.demos.solr.stats.Metrics;
import org.HdrHistogram.AtomicHistogram;
import org.easymock.EasyMock;

public class CqlIndexingLatencyProbeTest
{
    static MockCqlClusterFactory mocks = new MockCqlClusterFactory();

    @BeforeClass
    public static void before() throws Throwable
    {
        Clients.CQL_CLUSTER_FACTORY = mocks;
    }

    /*
     * Test that the indexing latency probe runs and returns at least the expected amount of latency
     */
    @Test
    public void testIndexingLatencyProbe() throws Exception
    {
        Metrics metrics = new Metrics(1, TimeUnit.DAYS, 1, ImmutableList.<PrintStream>of(), Long.MAX_VALUE, null);
        IndexingLatencyProbe probe = new CqlIndexingLatencyProbe(new CommandLine.Params(), metrics);


        //Neither probe should complete
        probe.maybeProbe("foo", System.nanoTime());
        Thread.sleep(100);
        probe.maybeProbe("bar", System.nanoTime());
        Thread.sleep(100);

        //Mock up a responses from the server
        ResultSet rs = EasyMock.createMock(ResultSet.class);
        Iterator<Row> iterator = EasyMock.createMock(Iterator.class);
        EasyMock.expect(iterator.hasNext()).andReturn(false);
        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(rs.iterator()).andReturn(iterator);
        EasyMock.expect(rs.iterator()).andReturn(iterator);
        EasyMock.replay(rs, iterator);

        //Wait for the client to be created and mocked by the other thread
        long start = System.nanoTime();
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10))
        {
            if (!mocks.clusters.isEmpty())
            {
                if (!mocks.clusters.get(0).sessions.isEmpty())
                {
                    break;
                }
            }
            Thread.sleep(1);
        }

        //Offer the responses from the server
        MockCqlClusterFactory.MockSession session = mocks.clusters.get(0).sessions.get(0);
        session.resultSets.offer(rs);
        session.resultSets.offer(rs);

        //Wait for the latency to be recorded
        Metric indexLatency = metrics.getMetric(Metrics.StatsType.INDEX_LATENCY);
        start = System.nanoTime();
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10) && !indexLatency.hasEvents())
        {
            Thread.sleep(1);
        }
        Thread.sleep(100);
        AtomicHistogram histogram = indexLatency.getTotal();
        //Should have been held for a minimum of 200 milliseconds
        Assert.assertEquals(1, histogram.getTotalCount());
        Assert.assertTrue(TimeUnit.MICROSECONDS.toNanos(200) < histogram.getMaxValue());

        //Check the CQL statement that was generated
        Assert.assertEquals(1, session.queries.size());
        String query = "SELECT * FROM demo.solr WHERE solr_query=? LIMIT 1";
        Assert.assertEquals(query, session.queries.iterator().next());
        List<Object[]> boundQueries = session.boundQueries;
        Assert.assertEquals(2, boundQueries.size());
        for (Object[] boundQuery : boundQueries)
        {
            Assert.assertEquals(1, boundQuery.length);
            Assert.assertEquals("id:foo", boundQuery[0]);
        }

    }
}
