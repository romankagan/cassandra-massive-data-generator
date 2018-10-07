package com.romankagan.dse.demos.solr.clients;

import java.io.PrintStream;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.romankagan.driver.core.ConsistencyLevel;
import com.romankagan.driver.core.PreparedStatement;
import com.romankagan.dse.demos.solr.stats.Metrics;

public class TestCqlSession
{
    private static MockCqlClusterFactory mocks = new MockCqlClusterFactory();

    private Metrics metrics = new Metrics(1, TimeUnit.DAYS, 1, ImmutableList.<PrintStream>of(), Long.MAX_VALUE, null);

    @BeforeClass
    public static void before() throws Throwable
    {
        Clients.CQL_CLUSTER_FACTORY = mocks;
    }

    /*
     * Test that the limit is enforced by setting the limit to 1 and checking that only one thread ends up submitting
     * a query.
     */
    @Test
    public void testCqlSessionInFlightLimit() throws Throwable
    {
        final CqlSession session = Clients.getCqlClient(ImmutableSet.of("0.0.0.0"), 1, 1, metrics, ConsistencyLevel.LOCAL_ONE);
        final PreparedStatement statement = session.prepare("foo");

        //Drain all the permits and release exactly one so that only one thread can submit a query
        mocks.applyToSessions(new Function<MockCqlClusterFactory.MockSession, Object>()
        {

            @Nullable
            @Override
            public Object apply(@Nullable MockCqlClusterFactory.MockSession session)
            {
                session.queryPermits.drainPermits();
                session.queryPermits.release();
                return null;
            }
        });

        //Have two threads attempt to submit queries
        Runnable task = new Runnable()
        {
            @Override
            public void run()
            {
                session.executeAsync(statement.bind(), null, null);
            }
        };

        Thread t1 = new Thread(task);
        t1.start();
        Thread t2 = new Thread(task);
        t2.start();

        //Only one thread should be able to submit a query due to the concurrency limit
        Callable<Object> condition = new Callable<Object>()
        {

            @Override
            public Object call() throws Exception
            {
                final AtomicInteger count = new AtomicInteger(0);
                mocks.applyToSessions(new Function<MockCqlClusterFactory.MockSession, Object>()
                {

                    @Nullable
                    @Override
                    public Object apply(@Nullable MockCqlClusterFactory.MockSession session)
                    {
                        count.addAndGet(session.queryPermits.getQueueLength());
                        return null;
                    }
                });
                return count.get();
            }
        };
        spinAssertEquals(1, TimeUnit.SECONDS, 1, condition);

        //Should stay exactly one thread
        Thread.sleep(500);
        Assert.assertEquals(1, condition.call());

        //Release permit allowing the other thread to progress
        mocks.applyToSessions(new Function<MockCqlClusterFactory.MockSession, Object>()
        {

            @Nullable
            @Override
            public Object apply(@Nullable MockCqlClusterFactory.MockSession session)
            {
                session.queryPermits.release();
                return null;
            }
        });

        //Make sure both threads completed their queries
        long start = System.nanoTime();
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10) &&
                (t1.isAlive() || t2.isAlive()))
        {
            Thread.yield();
        }
        Assert.assertFalse(t1.isAlive());
        Assert.assertFalse(t2.isAlive());
    }

    /*
     * Spin the specified amount of time checking to see if an equality condition is true
     */
    public void spinAssertEquals(long timeout, TimeUnit unit, Object expected, Callable<Object> condition) throws Exception
    {
        timeout = unit.toNanos(timeout);
        long start = System.nanoTime();
        while (System.nanoTime() - start < timeout)
        {
            if (condition.call().equals(expected))
            {
                return;
            }
            Thread.yield();
        }
        Assert.fail("Was not equal before timeout");
    }

}
