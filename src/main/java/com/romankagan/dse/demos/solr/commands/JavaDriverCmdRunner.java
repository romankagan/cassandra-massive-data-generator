package com.romankagan.dse.demos.solr.commands;

import java.util.List;

import com.romankagan.driver.core.SimpleStatement;
import com.romankagan.driver.core.Statement;
import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.Utils;
import com.romankagan.dse.demos.solr.clients.Clients;
import com.romankagan.dse.demos.solr.clients.CqlSession;
import com.romankagan.dse.demos.solr.probes.IndexingLatencyProbe;
import com.romankagan.dse.demos.solr.stats.Metrics;

public class JavaDriverCmdRunner extends CmdRunner
{
    public final static String CMD_STRING_ID = "CQL";

    protected final CqlSession session;

    protected final CommandLine.Params params;

    protected final IndexingLatencyProbe indexingLatencyProbe;

    JavaDriverCmdRunner(CommandLine.Params params, Metrics metrics, IndexingLatencyProbe indexingLatencyProbe) throws Exception
    {
        super(metrics);
        this.params = params;
        this.session = Clients.getCqlClient(params.urls, params.clients, params.concurrency, metrics,
                params.cqlUsername, params.cqlPassword, params.cqlSSL, params.cqlCipherSuites, params.cqlSslProtocol,
                params.cqlKeystore, params.cqlKeystoreType, params.cqlKeystorePassword, params.cqlTruststore, params.cqlTruststoreType, params.cqlTruststorePassword,
                params.consistencyLevel);
        this.indexingLatencyProbe = indexingLatencyProbe;
    }

    @Override
    public void runCommand(List<String> cmd2Run) throws Throwable
    {
        StatementAndProbeId statementAndProbeId = getStatement(cmd2Run);
        start = session.executeAsyncAndPageResults(statementAndProbeId.statement, statementAndProbeId.probeId, indexingLatencyProbe);
    }

    /**
     * Retrieve the statement to run and if the statement is update or insert an ID string for the pkey
     * to be used to probe indexing latency.
     */
    protected StatementAndProbeId getStatement(List<String> cmd2Run)
    {
        String cqlCmd = Utils.runCmdNotationSubstitutions(cmd2Run.get(1), params.random.get());
        return new StatementAndProbeId(new SimpleStatement(cqlCmd).setFetchSize(params.fetchSize), null);
    }

    boolean isUpdateOrInsert(String cql)
    {
        String command = cql.trim().substring(0, 6);
        return command.equalsIgnoreCase("INSERT") | command.equalsIgnoreCase("UPDATE");
    }

    static class StatementAndProbeId
    {
        public final Statement statement;
        public final String probeId;

        public StatementAndProbeId(Statement statement, String probeId)
        {
            this.statement = statement;
            this.probeId = probeId;
        }
    }
}
