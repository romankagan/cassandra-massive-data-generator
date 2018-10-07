package com.romankagan.dse.demos.solr.clients;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.codahale.metrics.Gauge;
import com.romankagan.bdp.shade.com.google.common.collect.Lists;
import com.romankagan.driver.core.BoundStatement;
import com.romankagan.driver.core.CloseFuture;
import com.romankagan.driver.core.Cluster;
import com.romankagan.driver.core.CodecRegistry;
import com.romankagan.driver.core.ColumnDefinitions;
import com.romankagan.driver.core.ConsistencyLevel;
import com.romankagan.driver.core.Metrics;
import com.romankagan.driver.core.PreparedId;
import com.romankagan.driver.core.PreparedStatement;
import com.romankagan.driver.core.RegularStatement;
import com.romankagan.driver.core.ResultSet;
import com.romankagan.driver.core.ResultSetFuture;
import com.romankagan.driver.core.Session;
import com.romankagan.driver.core.Statement;
import com.romankagan.driver.core.policies.RetryPolicy;
import com.romankagan.driver.core.querybuilder.BuiltStatement;
import org.easymock.EasyMock;

public class MockCqlClusterFactory extends Clients.CqlClusterFactory
{
    public final List<MockCluster> clusters = new CopyOnWriteArrayList<MockCluster>();

    @Override
    public com.romankagan.driver.core.Cluster newCqlCluster(Collection<String> endpoints, int clients, String cqlUsername, String cqlPassword,
            boolean cqlSSL, String cqlCipherSuites, String cqlSslProtocol,
            String cqlKeystore, String cqlKeystoreType, String cqlKeystorePassword, String cqlTruststore, String cqlTruststoreType, String cqlTruststorePassword,
            ConsistencyLevel level)
    {
        MockCluster mc = new MockCluster(endpoints, clients);
        clusters.add(mc);
        return mc;
    }

    //Return value of function not used, is there a better interface to use?
    public void applyToSessions(Function<MockSession, Object> function)
    {
        for (MockCluster cluster : clusters)
        {
            for (MockSession session : cluster.sessions)
            {
                function.apply(session);
            }
        }
    }

    public class MockCluster extends Cluster
    {
        public final Collection<String> endpoints;
        public final int clients;
        public final List<MockSession> sessions = Lists.newCopyOnWriteArrayList();

        public MockCluster(Collection<String> endpoints, int clients)
        {
            //contact point is unused
            super(new Cluster.Builder().addContactPoint("0.0.0.0"));
            this.endpoints = endpoints;
            this.clients = clients;
        }

        @Override
        public Session connect()
        {
            MockSession ms = new MockSession(this);
            sessions.add(ms);
            return ms;
        }

        //The number of connections is used to drive the calculated concurrency
        //used so a it will be retrieved and a value must be provided
        @SuppressWarnings("unchecked")
        @Override
        public Metrics getMetrics()
        {
            Gauge<Integer> gauge = EasyMock.createMock(Gauge.class);
            EasyMock.expect(gauge.getValue()).andReturn(1);
            Metrics metrics = EasyMock.createMock(Metrics.class);
            EasyMock.expect(metrics.getConnectedToHosts()).andReturn(gauge);
            EasyMock.replay(gauge, metrics);
            return metrics;
        }
    }

    public class MockSession implements Session
    {
        public final MockCluster mockCluster;
        public final Set<String> queries = Sets.newConcurrentHashSet();
        public final Semaphore queryPermits = new Semaphore(Integer.MAX_VALUE);
        public final List<Object[]> boundQueries = Lists.newCopyOnWriteArrayList();
        public final BlockingQueue<ResultSet> resultSets = new LinkedBlockingQueue<>();
        public volatile boolean closed = false;

        public MockSession(MockCluster mockCluster)
        {
            this.mockCluster = mockCluster;
        }

        @Override
        public String getLoggedKeyspace()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.romankagan.driver.core.Session init()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<com.romankagan.driver.core.Session> initAsync()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResultSet execute(String query)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResultSet execute(String query, Object... values)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResultSet execute(Statement statement)
        {
            if (statement instanceof BuiltStatement)
            {
                queries.add(statement.toString());
            }
            try
            {
                return resultSets.take();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ResultSet execute(String query, Map<String,Object> params)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResultSetFuture executeAsync(String query)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResultSetFuture executeAsync(String query, Object... values)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResultSetFuture executeAsync(Statement statement)
        {
            try
            {
                queryPermits.acquire();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            if (statement instanceof BuiltStatement)
            {
                queries.add(statement.toString());
            }
            return completedResultSetFuture();
        }

        @Override
        public ResultSetFuture executeAsync(String query, Map<String,Object> params)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedStatement prepare(String query)
        {
            queries.add(query);
            return new MockPreparedStatement(query, this);
        }

        @Override
        public PreparedStatement prepare(RegularStatement statement)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<PreparedStatement> prepareAsync(String query)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CloseFuture closeAsync()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public boolean isClosed()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.romankagan.driver.core.Cluster getCluster()
        {
            return mockCluster;
        }

        @Override
        public State getState()
        {
            throw new UnsupportedOperationException();
        }
    }

    public class MockPreparedStatement implements PreparedStatement
    {
        public final String query;
        public final MockSession session;

        public MockPreparedStatement(String query, MockSession session)
        {
            this.query = query;
            this.session = session;
        }

        @Override
        public ColumnDefinitions getVariables()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BoundStatement bind(Object... values)
        {
            session.boundQueries.add(values);
            BoundStatement bs = EasyMock.createMock(BoundStatement.class);
            return bs;
        }

        @Override
        public BoundStatement bind()
        {
            BoundStatement bs = EasyMock.createMock(BoundStatement.class);
            return bs;
        }

        @Override
        public com.romankagan.driver.core.PreparedStatement setRoutingKey(ByteBuffer routingKey)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.romankagan.driver.core.PreparedStatement setRoutingKey(ByteBuffer... routingKeyComponents)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer getRoutingKey()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.romankagan.driver.core.PreparedStatement setConsistencyLevel(ConsistencyLevel consistency)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsistencyLevel getConsistencyLevel()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.romankagan.driver.core.PreparedStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsistencyLevel getSerialConsistencyLevel()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getQueryString()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getQueryKeyspace()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.romankagan.driver.core.PreparedStatement enableTracing()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.romankagan.driver.core.PreparedStatement disableTracing()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean isIdempotent()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTracing()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.romankagan.driver.core.PreparedStatement setRetryPolicy(RetryPolicy policy)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RetryPolicy getRetryPolicy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedId getPreparedId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, ByteBuffer> getIncomingPayload()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedStatement setIdempotent(Boolean idempotent)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CodecRegistry getCodecRegistry()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedStatement setOutgoingPayload(Map<String, ByteBuffer> payload)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, ByteBuffer> getOutgoingPayload()
        {
            throw new UnsupportedOperationException();
        }

    }

    private ResultSetFuture completedResultSetFuture()
    {
        return new ResultSetFuture()
        {

            @Override
            public void addListener(Runnable listener, Executor executor)
            {
                executor.execute(listener);
            }

            @Override
            public ResultSet getUninterruptibly()
            {
                return null;
            }

            @Override
            public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException
            {
                return null;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning)
            {
                return false;
            }

            @Override
            public boolean isCancelled()
            {
                return false;
            }

            @Override
            public boolean isDone()
            {
                return true;
            }

            @Override
            public ResultSet get() throws InterruptedException, ExecutionException
            {
                return null;
            }

            @Override
            public ResultSet get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
            {
                return null;
            }
        };
    }

}
