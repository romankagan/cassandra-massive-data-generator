package com.romankagan.dse.demos.solr.clients;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import com.romankagan.dse.demos.solr.stats.Metrics;

public class ClientsTest
{

    private Metrics metricsA = new Metrics(1, TimeUnit.DAYS, 1, ImmutableList.<PrintStream>of(), Long.MAX_VALUE, null);
    private Metrics metricsB = new Metrics(1, TimeUnit.DAYS, 1, ImmutableList.<PrintStream>of(), Long.MAX_VALUE, null);

    @Test(expected = RuntimeException.class)
    public void testExpectRuntimeExceptionOnMismatchedEndpoints() throws Throwable
    {
        Clients.CQL_CLUSTER_FACTORY = new MockCqlClusterFactory();
        Clients.getCqlClient(ImmutableSet.of("0.0.0.0"), 1, Integer.MAX_VALUE, metricsA, null);
        Clients.getCqlClient(ImmutableSet.of("foobar"), 1, Integer.MAX_VALUE, metricsA, null);
    }

    @Test(expected = RuntimeException.class)
    public void testExpectRuntimeExceptionOnMismatchedMetrics() throws Throwable
    {
        Clients.CQL_CLUSTER_FACTORY = new MockCqlClusterFactory();
        Clients.getCqlClient(ImmutableSet.of("0.0.0.0"), 1, Integer.MAX_VALUE, metricsA, null);
        Clients.getCqlClient(ImmutableSet.of("0.0.0.0"), 1, Integer.MAX_VALUE, metricsB, null);
    }
}
