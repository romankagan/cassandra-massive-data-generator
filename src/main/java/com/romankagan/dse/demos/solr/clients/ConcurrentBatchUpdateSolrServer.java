package com.romankagan.dse.demos.solr.clients;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.romankagan.driver.core.ConsistencyLevel;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.romankagan.dse.demos.solr.SolrStress;

/*
 * An attempt at an alternative to ConcurrentUpdateSolrServer that has some answers WRT
 * to when requests fail or generate errors instead of silently asynchronously doing ???
 * Does similar write coalescing goodness.
 */
public class ConcurrentBatchUpdateSolrServer
{
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentBatchUpdateSolrServer.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition queueNotEmpty = lock.newCondition();
    private final Condition queueNotFull = lock.newCondition();
    //A runner should wake up and start polling the queue
    private final Condition needMoreRunner = lock.newCondition();

    //Tasks can be a batch of documents to insert/update or a collection of a single document
    private final ArrayDeque<Pair<SettableFuture<Object>, Collection<SolrInputDocument>>> taskQueue = new ArrayDeque<>();

    private volatile boolean keepRunning = true;
    private final Thread[] threads;
    private final List<SolrClient> solrs = new ArrayList<>();
    private final AtomicIntegerArray outstandingRequests;
    private final Semaphore concurrencyLimit;
    private final int batchSize;
    private final int maxQueueWeight;
    private final int halfMaxQueueWeight;
    private int queueWeight = 0;
    private int runners = 0;

    public ConcurrentBatchUpdateSolrServer(Collection<String> solrHttpEndpoints, int threadCount, int batchSize, int concurrency, ConsistencyLevel consistencyLevel)
    {
        for (String endpoint : solrHttpEndpoints)
        {
            SolrClient server = Clients.getHttpSolrClient(endpoint, consistencyLevel);
            //Check if the server is reachable so it can fail immediately
            try
            {
                server.ping();
            }
            catch (HttpSolrClient.RemoteSolrException e)
            {
                //DSP-9897 discovered that you can get an exception from ping with a valid connection
                //because it assumes a default field when generating a query to ping with.
                //If you got a remote exception it means you connected.
                //If the connection works for requests afterwards then great.
                //If not the error will be logged later by threads submitting requests.
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
            solrs.add(server);
        }
        this.batchSize = batchSize;
        //Submit batches of batchSize tasks at a time
        //Leave space for an extra batch for each thread
        maxQueueWeight = (batchSize * threadCount) * 2;
        halfMaxQueueWeight = Math.max(1, maxQueueWeight / 2);
        outstandingRequests = new AtomicIntegerArray(solrHttpEndpoints.size());
        threads = new Thread[threadCount];
        runners = threadCount;
        ThreadFactory tfb = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("solr-http-%d").build();
        for (int ii = 0; ii < threadCount; ii++)
        {
            threads[ii] = tfb.newThread(new Runner());
            threads[ii].start();
        }
        this.concurrencyLimit = new Semaphore(concurrency);
    }

    public QueryResponse query(SolrQuery query) throws Exception
    {
        shutdownCheck();

        int solrIndex = -1;
        try
        {
            concurrencyLimit.acquire();

            solrIndex = selectSolr();
            outstandingRequests.incrementAndGet(solrIndex);

            return solrs.get(solrIndex).query(query);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            concurrencyLimit.release();
            if (solrIndex > -1)
            {
                outstandingRequests.decrementAndGet(solrIndex);
            }
        }

    }

    public ListenableFuture<Object> add(SolrInputDocument doc)
    {
        return add(ImmutableList.of(doc));
    }

    public ListenableFuture<Object> add(final Collection<SolrInputDocument> docs)
    {
        shutdownCheck();

        SettableFuture<Object> resultFuture = SettableFuture.create();
        Pair<SettableFuture<Object>, Collection<SolrInputDocument>> p = Pair.create(resultFuture, docs);

        try
        {
            concurrencyLimit.acquire();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        lock.lock();
        try
        {
            //If the queue is kinda full or there are no runners request one
            if (queueWeight > halfMaxQueueWeight | runners == 0)
            {
                needMoreRunner.signal();
            }

            //If the queue is completely full wait for it not to be
            while (queueWeight >= maxQueueWeight)
            {
                try
                {
                    queueNotFull.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                shutdownCheck();
            }

            //Make the task available and record the weight change
            queueWeight += docs.size();
            taskQueue.offer(p);

            //If there is another waiter to put things in the queue make sure to signal them.
            if (queueWeight < maxQueueWeight)
            {
                queueNotFull.signal();
            }

            //And of course, the queue is definitely not empty anymore
            queueNotEmpty.signal();
        }
        finally
        {
            lock.unlock();
            concurrencyLimit.release();
        }
        return resultFuture;
    }

    public Throwable shutdownAndBlockUntilFinished(Throwable mergedException)
    {
        lock.lock();
        try
        {
            keepRunning = false;
            needMoreRunner.signalAll();
            queueNotEmpty.signalAll();
            queueNotFull.signalAll();
        }
        finally
        {
            lock.unlock();
        }
        try
        {
            for (Thread t : threads)
            {
                t.join();
            }
        }
        catch (Exception e)
        {
            mergedException = Throwables.merge(mergedException, e);
        }
        return mergedException;
    }

    private void shutdownCheck()
    {
        if (!keepRunning)
        {
            throw new RuntimeException("Already shut down");
        }
    }


    /*
     * Select the next server to send a request to based on outstanding requests
     */
    private int selectSolr()
    {
        //Select the endpoint with the least amount of load
        //The start index is randomized to reduce bias to nodes earlier
        //in the list.
        int minIndex = -1;
        int minOutstanding = Integer.MAX_VALUE;
        int startIndex = ThreadLocalRandom.current().nextInt(outstandingRequests.length());
        int loopEnd = outstandingRequests.length() + startIndex;
        for (int ii = startIndex; ii < loopEnd; ii++)
        {
            int index = ii % outstandingRequests.length();
            int outstanding = outstandingRequests.get(index);
            if (outstanding < minOutstanding)
            {
                minIndex = index;
                minOutstanding = outstanding;
            }
        }

        return minIndex;
    }

    private List<Pair<SettableFuture<Object>, Collection<SolrInputDocument>>> getDocs() throws InterruptedException
    {
        //Goal is to collect batchSize documents either has batches or as individual documents
        List<Pair<SettableFuture<Object>, Collection<SolrInputDocument>>> tasks = new ArrayList<>(batchSize);
        int collectedDocs = 0;

        lock.lock();
        try
        {
            //Wait on the queue not being empty for up to 250 milliseconds
            long waitRemaining = TimeUnit.MILLISECONDS.toNanos(250);
            while (waitRemaining > 0 & taskQueue.isEmpty() & keepRunning)
            {
                waitRemaining = queueNotEmpty.awaitNanos(waitRemaining);
            }

            //Timed out polling so block on needing more runners
            //Rather then add another loop and indentation return an empty list
            //to restart the loop
            if (waitRemaining <= 0 & keepRunning)
            {
                runners--;
                needMoreRunner.await();
                runners++;
                return tasks;
            }

            //Pull stuff out of the queue until there are batchSize docs
            //Tracks # of docs collected as well as the weight impact on the queue
            while (collectedDocs < batchSize & !taskQueue.isEmpty())
            {
                Pair<SettableFuture<Object>, Collection<SolrInputDocument>> p = taskQueue.poll();
                int size = p.right.size();
                collectedDocs += size;
                queueWeight -= size;
                tasks.add(p);
            }

            //If we didn't drain the queue, then signal the next waiter on the queue
            if (!taskQueue.isEmpty())
            {
                queueNotEmpty.signal();
            }

            //If the queue is no longer full wake up anyone waiting to offer
            if (queueWeight < maxQueueWeight)
            {
                queueNotFull.signal();
            }
        }
        finally
        {
            lock.unlock();
        }
        return tasks;
    }

    private class Runner implements Runnable
    {
        //Block all the threads except for one on the more runners signal at startup
        private void initialBlockOnMoarRunners() throws InterruptedException
        {
            lock.lock();
            try
            {
                if (taskQueue.isEmpty() && runners > 1)
                {
                    runners--;
                    needMoreRunner.await();
                    runners++;
                }
            }
            finally
            {
                lock.unlock();
            }
        }

        @Override
        public void run()
        {
            try
            {
                initialBlockOnMoarRunners();
                //Keep running until shutdown or while there are still more tasks
                boolean hadTasks = false;
                while (keepRunning | hadTasks)
                {
                    hadTasks = false;
                    List<Pair<SettableFuture<Object>, Collection<SolrInputDocument>>> futuresAndDocs = getDocs();

                    //Happens when the runner goes to sleep, and is then called back into action
                    //Just wants the loop to restart
                    if (futuresAndDocs.isEmpty())
                        continue;

                    hadTasks = true;
                    //Select a server to contact
                    int solrIndex = selectSolr();
                    SolrClient solr = solrs.get(solrIndex);

                    //Move the docs into a single collection for the solr client
                    List<SolrInputDocument> docs = new ArrayList<>(batchSize);
                    int docCount = 0;
                    for (Pair<SettableFuture<Object>, Collection<SolrInputDocument>> p : futuresAndDocs)
                    {
                        docs.addAll(p.right);
                        docCount += p.right.size();
                    }

                    //Submit the document to solr while tracking the impact on # of outstanding requests
                    outstandingRequests.addAndGet(solrIndex, docCount);
                    try
                    {
                        solr.add(docs);
                        //Signal success to listeners
                        for (Pair<SettableFuture<Object>, Collection<SolrInputDocument>> p : futuresAndDocs)
                        {
                            p.left.set(null);
                        }
                    }
                    catch (Throwable t)
                    {
                        //Signal failure to listeners
                        for (Pair<SettableFuture<Object>, Collection<SolrInputDocument>> p : futuresAndDocs)
                        {
                            p.left.setException(t);
                        }
                    }
                    finally
                    {
                        outstandingRequests.addAndGet(solrIndex, -docCount);
                    }
                }
            }
            catch (Exception e)
            {
                SolrStress.logException(e);
                throw new RuntimeException(e);
            }
        }
    }
}
