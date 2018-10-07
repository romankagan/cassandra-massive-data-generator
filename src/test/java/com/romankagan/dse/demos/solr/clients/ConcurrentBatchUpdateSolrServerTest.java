package com.romankagan.dse.demos.solr.clients;


import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.cassandra.utils.Throwables;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConcurrentBatchUpdateSolrServerTest
{
    static final int THREAD_COUNT = 4;
    static MockSolrClientFactory mocks = new MockSolrClientFactory();
    static List<String> endpoints = ImmutableList.of("foo", "bar");

    ConcurrentBatchUpdateSolrServer solr;

    @BeforeClass
    public static void before() throws Throwable
    {
        Clients.SOLR_SERVER_FACTORY = mocks;
    }

    @Before
    public void setUp() throws Throwable
    {
        Clients.reset();
        solr = Clients.getConcurrentBatchUpdateSolrServer(endpoints, THREAD_COUNT, Integer.MAX_VALUE, null);
        Assert.assertEquals(mocks.mocks.size(), 2);
    }

    @After
    public void tearDown() throws Throwable
    {
        mocks.reset();
        Throwable mergedException = null;
        mergedException = solr.shutdownAndBlockUntilFinished(mergedException);
        Throwables.maybeFail(mergedException);
    }

    /*
     * Test that writes are asynchronous and more or less work
     */
    @Test
    public void testWrite() throws Throwable
    {
        //Check a basic async write completes
        ListenableFuture<Object> resultFuture = solr.add(new SolrInputDocument());
        mocks.expectWritesAndDrainPlusRelease(1);
        resultFuture.get();

        //Check the collection version completes
        resultFuture = solr.add(Collections.singleton(new SolrInputDocument()));
        //Make sure it doesn't return a fake response, needs to have the permit to complete
        Thread.sleep(10);
        Assert.assertFalse(resultFuture.isDone());
        mocks.expectWritesAndDrainPlusRelease(1);
        resultFuture.get();
    }

    /*
     * There is a feature to batch multiple document writes into a single write.
     * Make sure that it coalesces up to the batch size
     */
    @Test
    public void testWriteCoalescing() throws Throwable
    {
        //Block all the threads on an individual write
        addWrites(THREAD_COUNT * Clients.CONCURRENT_UPDATE_BATCH_SIZE * 2);

        //Wait for all the writers to be blocked and consume the writes
        mocks.assertWriteWaitersCount(4);
        mocks.assertAndDrainWrites(4);

        //Release one, to go coalesce a batch of writes
        mocks.mocks.get(0).writePermits.release();

        //Make sure it coalesced a full batch
        Assert.assertEquals(Clients.CONCURRENT_UPDATE_BATCH_SIZE, mocks.assertAndDrainWrites(1).get(0).size());
    }

    /*
     * Reads and writes are supposed to be balanced across nodes, and not just balanced but balanced
     * randomly in the event of a tie
     */
    @Test
    public void testLoadBalancing() throws Throwable
    {
        mocks.releaseReadPermits(10000);
        addWrites(1);
        mocks.assertWriteWaitersCount(1);
        //Plus two because the check to signal more threads is done before adding to the queue
        addWrites(THREAD_COUNT * Clients.CONCURRENT_UPDATE_BATCH_SIZE + 2);
        mocks.assertWriteWaitersCount(2);
        //Replace the batch consumed to spin up another thread
        addWrites(Clients.CONCURRENT_UPDATE_BATCH_SIZE);
        mocks.assertWriteWaitersCount(3);
        //Replace the batch consumed to spin up another thread
        addWrites(Clients.CONCURRENT_UPDATE_BATCH_SIZE);
        mocks.assertWriteWaitersCount(4);

        //Should be balanced
        Assert.assertEquals(mocks.mocks.get(0).writeRequests.size(), 2);
        Assert.assertEquals(mocks.mocks.get(1).writeRequests.size(), 2);

        //This magic number is simply all the writes done above
        mocks.expectWritesAndDrainPlusRelease(((THREAD_COUNT + 2) * Clients.CONCURRENT_UPDATE_BATCH_SIZE) + 3);

        //Use reads to check that it doesn't just go for the same node every time and balances
        for (int ii = 0; ii < 10000; ii++)
        {
            solr.query(new SolrQuery());
        }
        int firstReads = mocks.mocks.get(0).readRequests.size();
        int secondReads = mocks.mocks.get(1).readRequests.size();
        double delta = firstReads / (double)secondReads;
        Assert.assertTrue(delta > 0.95 && delta < 1.05);
    }

    /*
     * Test that a producer is kicked out with an exception if the client is shut down while it is
     * blocked.
     */
    @Test
    public void shutdownKicksBlockedProducers() throws Throwable
    {
        // Victim thread to block on producing
        final Semaphore threw = new Semaphore(0);
        final AtomicBoolean initialized = new AtomicBoolean();
        Thread producer = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    addWrites(1);
                    initialized.set(true);
                    addWrites(1000);
                }
                catch (RuntimeException e)
                {
                    if ("Already shut down".equals(e.getMessage()))
                    {
                        threw.release();
                    }
                    else
                    {
                        e.printStackTrace();
                    }
                }
            }
        };
        producer.start();

        //Wait for the victim to block
        while (producer.getState() != Thread.State.WAITING && !initialized.get())
        {
            Thread.sleep(1);
        }

        //Another victim thread to block on shutdown
        Thread shutdownThread = new Thread()
        {
            @Override
            public void run()
            {
                solr.shutdownAndBlockUntilFinished(null);
            }
        };
        shutdownThread.start();

        //Should quickly generate an exception for the producer
        Assert.assertTrue(threw.tryAcquire(1, 10, TimeUnit.SECONDS));
    }

    /*
     * Confirm that if concurrency is specified only that number of requests are submitted
     */
    @Test
    public void testQueryConcurrencyLimit() throws Throwable
    {
        Clients.reset();
        solr = Clients.getConcurrentBatchUpdateSolrServer(endpoints, THREAD_COUNT, 1, null);
        Runnable task = () -> {
            try
            {
                solr.query(new SolrQuery());
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        };
        Thread t1 = new Thread(task);
        t1.start();
        Thread t2 = new Thread(task);
        t2.start();

        mocks.assertReadWaitersCount(1);
        Thread.sleep(50);
        mocks.expectReadsAndDrainPlusRelease(1);
        mocks.assertReadWaitersCount(1);
        mocks.expectReadsAndDrainPlusRelease(1);

        long start = System.nanoTime();
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10) &&
                t1.isAlive() && t2.isAlive())
        {
            Thread.yield();
        }
        Assert.assertFalse(t1.isAlive());
        Assert.assertFalse(t2.isAlive());
    }

    /*
     * Confirm that if concurrency is specified only that number of requests are submitted
     */
    @Test
    public void testAddConcurrencyLimit() throws Throwable
    {
        Clients.reset();
        solr = Clients.getConcurrentBatchUpdateSolrServer(endpoints, THREAD_COUNT, 1, null);
        Runnable task = new Runnable()
        {
            @Override
            public void run()
            {

                solr.add(new SolrInputDocument());
            }
        };
        Thread t1 = new Thread(task);
        t1.start();
        Thread t2 = new Thread(task);
        t2.start();

        mocks.assertWriteWaitersCount(1);
        Thread.sleep(50);
        mocks.expectWritesAndDrainPlusRelease(1);
        mocks.assertWriteWaitersCount(1);
        mocks.expectWritesAndDrainPlusRelease(1);

        long start = System.nanoTime();
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10) &&
                (t1.isAlive() || t2.isAlive()))
        {
            Thread.yield();
        }
        Assert.assertFalse(t1.isAlive());
        Assert.assertFalse(t2.isAlive());
    }


    private void addWrites(int count)
    {
        for (int ii = 0; ii < count; ii++)
        {
            solr.add(new SolrInputDocument());
        }
    }
}
