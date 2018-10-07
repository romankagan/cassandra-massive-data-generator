package com.romankagan.dse.demos.solr.clients;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import com.romankagan.bdp.config.ClientConfigurationFactory;
import com.romankagan.driver.core.ConsistencyLevel;
import com.romankagan.driver.dse.auth.DseGSSAPIAuthProvider;
import com.romankagan.driver.dse.auth.DsePlainTextAuthProvider;
import org.apache.cassandra.utils.Pair;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.romankagan.driver.core.SSLOptions;
import com.romankagan.driver.core.JdkSSLOptions;
import org.apache.cassandra.utils.Throwables;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;

import com.google.common.annotations.VisibleForTesting;

import com.romankagan.bdp.shade.org.apache.http.client.HttpClient;
import com.romankagan.driver.core.Cluster;
import com.romankagan.driver.core.HostDistance;
import com.romankagan.driver.core.PoolingOptions;
import com.romankagan.driver.core.Session;
import com.romankagan.dse.demos.solr.SolrStress;
import com.romankagan.dse.demos.solr.stats.Metrics;

import static org.apache.commons.io.IOUtils.closeQuietly;

public class Clients
{
    public static final int CONCURRENT_UPDATE_BATCH_SIZE = 10;

    public static SolrClientFactory SOLR_SERVER_FACTORY = new SolrClientFactory();
    public static CqlClusterFactory CQL_CLUSTER_FACTORY = new CqlClusterFactory();

    //CQL client information
    private static Cluster cluster;
    private static Session session;
    private static CqlSession wrappedSession;
    private static boolean connectionFailed = false;
    private static Collection<String> cqlEndpoints;

    //Shared HTTP solr servers
    private static final Map<Pair<String, ConsistencyLevel>, SolrClient> solrs = new HashMap<>();

    //Shared HTTP client used for all HTTP access methods
    //Safe publication is ensured by creating this client before starting the other threads
    private static HttpClient httpClient;

    //Shared concurrent update solr servers used for writes and updates
    private static final Map<Collection<String>, ConcurrentBatchUpdateSolrServer> concurrentBatchUpdateSolrs = new HashMap<>();

    @VisibleForTesting
    public static void reset()
    {
        concurrentBatchUpdateSolrs.clear();
        solrs.clear();
        cluster = null;
        session = null;
        wrappedSession = null;
        connectionFailed = false;
    }

    @VisibleForTesting
    public static CqlSession getCqlClient(Collection<String> endpoints, int clients, int concurrency, Metrics metrics, ConsistencyLevel consistencyLevel)
    {
        return getCqlClient(endpoints, clients, concurrency, metrics, null, null, false, null, null, null, null, null,null, null, null, consistencyLevel);
    }

    /**
     * Returns a shared process global CQL client wrapper. On shutdown all in flight requests
     * are waited on by a shutdown task registered with SolrStress which is run after
     * all load generating threads have been quiesced.
     *
     * Only one set of endpoints can be configured. Attempting to configure multiple sets of
     * endpoints will generate a RuntimeException.
     * @param endpoints Endpoints to add as contact points to the CQL client
     * @param clients Number of load generation threads used to set # of connections per server
     * @param metrics Metrics to record results in
     * @param cqlUsername Native protocol authentication username
     * @param cqlPassword Native protocol authentication password
     * @param cqlSSL whether to use SSL to connect via native protocol
     * @param cqlCipherSuites Command separated list of cipher suites
     * @param cqlSslProtocol SSL protocol to use
     * @param cqlKeystore Path to the keystore
     * @param cqlKeystoreType Type of keystore
     * @param cqlKeystorePassword Keystore password
     * @param cqlTruststore Path to truststore
     * @param cqlTruststoreType Type of truststore
     * @param consistencyLevel Consistency level to use for requests
     * @return CQL client wrapper for the specified endpoints recording to the provided metrics
     */
    public static synchronized CqlSession getCqlClient(Collection<String> endpoints, int clients, int concurrency, Metrics metrics,
            String cqlUsername, String cqlPassword,
            boolean cqlSSL, String cqlCipherSuites, String cqlSslProtocol,
            String cqlKeystore, String cqlKeystoreType, String cqlKeystorePassword, String cqlTruststore, String cqlTruststoreType, String cqlTruststorePassword,
            ConsistencyLevel consistencyLevel)
    {
        if (connectionFailed)
        {
            //Have the Runner terminate its activities
            throw new SolrStress.TerminateException();
        }

        if (cqlEndpoints != null && !cqlEndpoints.equals(endpoints))
        {
            throw new RuntimeException("Only support one set of CQL endpoints");
        }

        if (wrappedSession != null && metrics != wrappedSession.metrics)
        {
            throw new RuntimeException("Metrics must match");
        }

        //Already constructed return the existing session
        if (wrappedSession != null)
        {
            return wrappedSession;
        }

        if ((cqlUsername != null || cqlPassword != null) && (cqlUsername == null || cqlPassword == null))
        {
            throw new RuntimeException("Must specify username and password");
        }

        cqlEndpoints = endpoints;

        boolean success = false;
        try
        {
            cluster = CQL_CLUSTER_FACTORY.newCqlCluster(endpoints, clients, cqlUsername, cqlPassword,
                    cqlSSL, cqlCipherSuites, cqlSslProtocol, cqlKeystore, cqlKeystoreType, cqlKeystorePassword, cqlTruststore, cqlTruststoreType, cqlTruststorePassword,
                    consistencyLevel);
            session = cluster.connect();
            wrappedSession = new CqlSession(session, metrics, concurrency);
            SolrStress.shutdownTasks.offer(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    wrappedSession.blockOnInflightCommands();

                    Throwable mergedException = null;
                    try
                    {
                        session.close();
                    }
                    catch (Throwable t)
                    {
                        mergedException = Throwables.merge(mergedException, t);
                    }

                    try
                    {
                        cluster.close();
                    }
                    catch (Throwable t)
                    {
                        mergedException = Throwables.merge(mergedException, t);
                    }

                    Throwables.maybeFail(mergedException);
                    return null;
                }
            });
            success = true;
        }
        finally
        {
            if (!success)
            {
                connectionFailed = true;
                cluster = null;
                session = null;
                wrappedSession = null;
            }
        }
        return wrappedSession;
    }

    /**
     * Returns a shared process global HttpSolrClient for the given endpoiont.
     *
     * All SolrServer instances returned will share the same HTTP connection pool
     * configured via initHttpClient()
     * @param endpoint Endpoint to get a SolrServer for
     * @param consistencyLevel ConsistencyLevel to be used
     * @return SolrServer for the specified endpoint
     */
    public static synchronized SolrClient getHttpSolrClient(String endpoint, ConsistencyLevel consistencyLevel)
    {
        Pair<String, ConsistencyLevel> key = Pair.create(endpoint, consistencyLevel);
        SolrClient solrServer = solrs.get(key);
        if (solrServer != null)
        {
            return solrServer;
        }

        solrServer = SOLR_SERVER_FACTORY.newSolrServer(endpoint, httpClient, consistencyLevel);
        final SolrClient solrServerFinal = solrServer;
        solrs.put(key, solrServer);
        SolrStress.shutdownTasks.offer(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                solrServerFinal.close();
                return null;
            }
        });

        return solrServer;
    }

    /**
     * Returns a shared process global HTTP Solr client. On shutdown all in flight requests
     * are waited on by a shutdown task registered with SolrStress which is run after
     * all load generating threads have been quiesced.
     * @param endpoints Endpoints the SolrServer will connect to
     * @param threads Number of concurrenct requests the SolrServer will issue backed by a thread pool
     * @param concurrency Maximum number of concurrent requests that can be issued through the client
     * @param consistencyLevel Consistency level to perform writes and reads at
     * @return SolrServer connected to the specified endpoints
     */
    public static synchronized ConcurrentBatchUpdateSolrServer getConcurrentBatchUpdateSolrServer(Collection<String> endpoints, int threads, int concurrency, ConsistencyLevel consistencyLevel)
    {
        ConcurrentBatchUpdateSolrServer solrServer = concurrentBatchUpdateSolrs.get(endpoints);
        if (solrServer != null)
        {
            return solrServer;
        }

        solrServer = new ConcurrentBatchUpdateSolrServer(endpoints, threads, CONCURRENT_UPDATE_BATCH_SIZE, concurrency, consistencyLevel);
        final ConcurrentBatchUpdateSolrServer forShutdown = solrServer;
        SolrStress.shutdownTasks.offer(new Callable<Object>()
        {

            @Override
            public Object call() throws Exception
            {
                Throwable mergedException = null;
                mergedException = forShutdown.shutdownAndBlockUntilFinished(mergedException);
                Throwables.maybeFail(mergedException);
                return null;
            }
        });
        concurrentBatchUpdateSolrs.put(endpoints, solrServer);

        return solrServer;
    }

    public static synchronized void initHTTPClient(int endpoints, int clients)
    {
        if (httpClient == null)
        {
            ModifiableSolrParams params = new ModifiableSolrParams();
            //HttpSolrClient hard codes this to 128 max, 32 max per host
            //Going to allow clients connections to each server.
            //For a load generation tool this avoids any confusing step functions.
            params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, endpoints * clients);
            params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, clients);
            params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
            httpClient = HttpClientUtil.createClient(params);

            final HttpClient forShutdown = httpClient;
            SolrStress.shutdownTasks.offer(new Callable<Object>()
            {
               @Override
                public Object call() throws Exception
               {
                   forShutdown.getConnectionManager().shutdown();
                   return null;
               }
            });
        }
        else
        {
            throw new RuntimeException("Already initialized HTTP client");
        }
    }

    /**
     * Creates trust manager factory and loads trust store. Null value is ok for password if it is
     * not used.
     */
    public static TrustManagerFactory initTrustManagerFactory(
            String keystorePath,
            String keystoreType,
            String keystorePassword
    ) throws IOException, GeneralSecurityException
    {
        FileInputStream tsf = null;
        try
        {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tsf = new FileInputStream(keystorePath);
            KeyStore ts = KeyStore.getInstance(keystoreType);
            ts.load(tsf, keystorePassword != null ? keystorePassword.toCharArray() : null);
            tmf.init(ts);
            return tmf;
        }
        finally
        {
            closeQuietly(tsf);
        }
    }

    /**
     * Creates key manager factory and loads the key store. Null values are ok for passwords if they are
     * not used.
     */
    public static KeyManagerFactory initKeystoreFactory(
            String keystorePath,
            String keystoreType,
            String keystorePassword,
            String keyPassword
    ) throws IOException, GeneralSecurityException
    {
        FileInputStream ksf = null;
        try
        {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            ksf = new FileInputStream(keystorePath);
            KeyStore ks = KeyStore.getInstance(keystoreType);
            ks.load(ksf, keystorePassword != null ? keystorePassword.toCharArray() : null);
            kmf.init(ks, keyPassword != null ? keyPassword.toCharArray() : null);
            return kmf;
        }
        finally
        {
            closeQuietly(ksf);
        }
    }

    public static class SolrClientFactory
    {
        public SolrClient newSolrServer(String endpoint, HttpClient httpClient, ConsistencyLevel cl)
        {
            HttpSolrClient solrServer = new HttpSolrClient(endpoint, httpClient);
            if (cl != null)
            {
                solrServer.getInvariantParams().add("cl", cl.toString());
            }
            return solrServer;
        }
    }

    public static class CqlClusterFactory
    {
        public Cluster newCqlCluster(Collection<String> endpoints, int clients, String cqlUsername, String cqlPassword,
                boolean cqlSSL, String cqlCipherSuites, String cqlSslProtocol,
                String cqlKeystore, String cqlKeystoreType, String cqlKeystorePassword, String cqlTruststore, String cqlTruststoreType, String cqlTruststorePassword,
                ConsistencyLevel consistencyLevel)
        {
            Cluster.Builder builder = Cluster.builder()
                    .withPoolingOptions(
                            new PoolingOptions()
                            .setCoreConnectionsPerHost(HostDistance.LOCAL, clients)
                            .setMaxConnectionsPerHost(HostDistance.LOCAL, clients));
            if (consistencyLevel != null)
            {
                builder.getConfiguration().getQueryOptions().setConsistencyLevel(consistencyLevel);
            }
            
            if (cqlUsername != null && cqlPassword != null)
            {
                builder.withAuthProvider(new DsePlainTextAuthProvider(cqlUsername, cqlPassword));
            }
            else
            {
                builder.withAuthProvider(new DseGSSAPIAuthProvider(ClientConfigurationFactory.getYamlClientConfiguration().getSaslProtocolName()));
            }
            
            if (cqlSSL)
            {
                try
                {
                    String[] cipherSuites = null;
                    if (cqlCipherSuites != null)
                        cipherSuites = cqlCipherSuites.split(",");
                    SSLContext context = SSLContext.getInstance(cqlSslProtocol);
                    KeyManager[] keyManagers = null;
                    if (cqlKeystore != null)
                        keyManagers = initKeystoreFactory(cqlKeystore, cqlKeystoreType, cqlKeystorePassword, cqlKeystorePassword).getKeyManagers();
                    TrustManager[] trustManagers = null;
                    if (cqlTruststore != null)
                        trustManagers = initTrustManagerFactory(cqlTruststore, cqlTruststoreType, cqlTruststorePassword).getTrustManagers();
                    SecureRandom secureRandom = new SecureRandom();
                    context.init(keyManagers, trustManagers, secureRandom);
                    SSLOptions sslOptions = JdkSSLOptions.builder().withSSLContext(context).withCipherSuites(cipherSuites).build();
                    builder.withSSL(sslOptions);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

            for (String host : endpoints)
            {
                try
                {
                    builder.addContactPoint(new URI(host).getHost());
                }
                catch (URISyntaxException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return builder.build();
        }
    }
}
