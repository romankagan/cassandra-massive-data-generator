package com.romankagan.dse.demos.solr.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.romankagan.driver.core.ConsistencyLevel;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;

import com.romankagan.bdp.shade.com.google.common.base.Throwables;
import com.romankagan.bdp.shade.org.apache.http.client.HttpClient;

public class MockSolrClientFactory extends Clients.SolrClientFactory
{

    public final List<MockSolrClient> mocks = new ArrayList<>();

    @Override
    public SolrClient newSolrServer(String endpoint, HttpClient httpClient, ConsistencyLevel cl)
    {
        MockSolrClient mock = new MockSolrClient(endpoint, httpClient);
        mocks.add(mock);
        return mock;
    }

    public void reset()
    {
        for (MockSolrClient mock : mocks)
        {
            mock.writePermits.release(Integer.MAX_VALUE / 2);
        }
        mocks.clear();
    }

    public void expectWritesAndDrainPlusRelease(int numDocuments)
    {
        long start = System.nanoTime();
        int docsFound = 0;
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5))
        {
            if (numDocuments == docsFound)
            {
                break;
            }
            for (MockSolrClient mock : mocks)
            {
                if (!mock.writeRequests.isEmpty())
                {
                    docsFound += mock.writeRequests.poll().size();
                    mock.writePermits.release();
                }
            }
            Thread.yield();
        }
        Assert.assertEquals(docsFound, numDocuments);
    }

    public void expectReadsAndDrainPlusRelease(int numQueries)
    {
        long start = System.nanoTime();
        int numQueriesFound = 0;
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5))
        {
            if (numQueries == numQueriesFound)
            {
                break;
            }
            for (MockSolrClient mock : mocks)
            {
                if (!mock.readRequests.isEmpty())
                {
                    numQueriesFound++;
                    mock.readRequests.poll().hashCode();
                    mock.readPermits.release();
                }
            }
            Thread.yield();
        }
        Assert.assertEquals(numQueriesFound, numQueries);
    }

    public int pendingWrites()
    {
        int docsFound = 0;
        for (MockSolrClient mock : mocks)
        {
            for (Collection<SolrInputDocument> docs : mock.writeRequests)
            {
                docsFound += docs.size();
            }
        }
        return docsFound;
    }

    public int writeWaitersCount()
    {
        int waiters = 0;
        for (MockSolrClient mock : mocks)
        {
            waiters += mock.writePermits.getQueueLength();
        }
        return waiters;
    }

    public int readWaitersCount()
    {
        int waiters = 0;
        for (MockSolrClient mock : mocks)
        {
            waiters += mock.readPermits.getQueueLength();
        }
        return waiters;
    }

    /*
     * Assert the expected number of writers are waiting
     */
    public void assertWriteWaitersCount(int targetWaiters)
    {
        long start = System.nanoTime();
        int waiters = 0;
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5))
        {
            waiters = writeWaitersCount();
            if (waiters == targetWaiters)
            {
                break;
            }
            Thread.yield();
        }
        Assert.assertEquals(waiters, targetWaiters);
    }

    /*
     * Assert the expected number of readers are waiting
     */
    public void assertReadWaitersCount(int targetWaiters)
    {
        long start = System.nanoTime();
        int waiters = 0;
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5))
        {
            waiters = readWaitersCount();
            if (waiters == targetWaiters)
            {
                break;
            }
            Thread.yield();
        }
        Assert.assertEquals(waiters, targetWaiters);
    }


    /*
     * Drain the queues of pending writes and return the writes but don't release the writing threads
     */
    public List<Collection<SolrInputDocument>> assertAndDrainWrites(int targetWrites)
    {

        List<Collection<SolrInputDocument>> writes = new ArrayList<>();
        long start = System.nanoTime();
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5))
        {
            for (MockSolrClient mock : mocks)
            {
                Collection<SolrInputDocument> write = mock.writeRequests.poll();
                if (write != null)
                {
                    writes.add(write);
                }
            }
            if (writes.size() == targetWrites)
            {
                break;
            }
            Thread.yield();
        }
        Assert.assertEquals(writes.size(), targetWrites);
        return writes;
    }

    /*
     * Make permits permits available at each MockSolrServer
     */
    public void releaseReadPermits(int permits) {
        for (MockSolrClient mockSolr : mocks)
        {
            mockSolr.readPermits.release(permits);
        }
    }

    public static class MockSolrClient extends SolrClient
    {
        public final Semaphore shutdownCount = new Semaphore(0);

        public final Semaphore writePermits = new Semaphore(0);

        public final Semaphore readPermits = new Semaphore(0);

        public final BlockingQueue<SolrRequest> readRequests = new LinkedBlockingQueue<>();

        public final BlockingQueue<Collection<SolrInputDocument>> writeRequests = new LinkedBlockingQueue<>();

        public final String endpoint;

        public final HttpClient httpClient;

        public MockSolrClient(String endpoint, HttpClient httpClient)
        {
            this.endpoint = endpoint;
            this.httpClient = httpClient;
        }

        @Override
        public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException
        {
            writeRequests.offer(docs);
            try
            {
                writePermits.acquire();
            }
            catch (InterruptedException e)
            {
                Throwables.propagate(e);
            }
            return new UpdateResponse();
        }

        @Override
        public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException
        {
            return this.add(Collections.singleton(doc));
        }

        @Override
        public NamedList<Object> request(SolrRequest solrRequest, String collection) throws SolrServerException, IOException
        {
            if (solrRequest instanceof SolrPing)
            {
                return new NamedList<Object>();
            }
            readRequests.offer(solrRequest);
            try
            {
                readPermits.acquire();
            }
            catch (InterruptedException e)
            {
                Throwables.propagate(e);
            }
            return new NamedList<Object>();
        }

        @Override
        public void close()
        {
            shutdownCount.release();
        }
    }
}
