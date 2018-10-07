package com.romankagan.dse.demos.solr.load;

import java.io.File;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.romankagan.driver.core.ConsistencyLevel;
import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.SolrStress;
import com.romankagan.dse.demos.solr.clients.Clients;
import com.romankagan.dse.demos.solr.clients.MockCqlClusterFactory;
import com.romankagan.dse.demos.solr.stats.Metrics;

public class CqlLoadStrategyTest
{
    static MockCqlClusterFactory mocks = new MockCqlClusterFactory();

    @BeforeClass
    public static void before() throws Throwable
    {
        Clients.CQL_CLUSTER_FACTORY = mocks;
    }

    @Before
    public void setUp() throws Throwable
    {
        Clients.reset();
    }

    @After
    public void tearDown() throws Throwable
    {
        Iterator<Callable> i = SolrStress.shutdownTasks.descendingIterator();
        while (i.hasNext())
        {
            i.next().call();
        }
    }

    /*
     * Test a file can be loaded via CQL
     */
    @Test
    public void testCqlLoad() throws Throwable
    {
        // given:
        File geonames = GeonamesReaderTest.getClasspathResource("geonames.txt");

        // when:
        try (GeonamesLoader reader = new GeonamesLoader(geonames.toPath(), Integer.MAX_VALUE, ImmutableMap.of(), Suppliers.ofInstance(new Random(42))))
        {
            CommandLine.Params params = new CommandLine.Params();
            params.urls = ImmutableSet.of("foo");
            Metrics metrics = new Metrics(1, TimeUnit.HOURS, 0, ImmutableList.<PrintStream>of(), Long.MAX_VALUE, null);

            CQLLoadStrategy load = new CQLLoadStrategy(params, metrics, reader, null);
            load.execute();

            // then:
            assertEquals(1, mocks.clusters.size());
            MockCqlClusterFactory.MockCluster mc = mocks.clusters.get(0);
            assertEquals(1, mc.sessions.size());
            MockCqlClusterFactory.MockSession ms = mc.sessions.get(0);

            Clients.getCqlClient(params.urls, 1, Integer.MAX_VALUE, metrics, ConsistencyLevel.LOCAL_ONE).blockOnInflightCommands();

            //The prepared statement for loading + the query for committing
            assertEquals(1, ms.queries.size());
            //Expect 50 rows
            assertEquals(50, ms.boundQueries.size());
        }
    }
}
