package com.romankagan.dse.demos.solr.commands;

import java.util.Arrays;
import java.util.List;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocumentList;
import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.Utils;
import com.romankagan.dse.demos.solr.clients.ConcurrentBatchUpdateSolrServer;
import com.romankagan.dse.demos.solr.clients.Clients;
import com.romankagan.dse.demos.solr.stats.Metrics;

public class HttpReadCmdRunner extends CmdRunner
{
    public final static String CMD_STRING_ID = "HTTPREAD";
    private final ConcurrentBatchUpdateSolrServer readSolrClient;
    private final CommandLine.Params params;

    public HttpReadCmdRunner(CommandLine.Params params, Metrics metrics)
    {
        super(metrics);
        this.params = params;
        this.readSolrClient = Clients.getConcurrentBatchUpdateSolrServer(params.urls, params.clients, params.concurrency, params.consistencyLevel);
    }

    @Override
    public void runCommand(List<String> cmd2Run) throws Throwable
    {
        SolrQuery query = new SolrQuery();

        // Take escaping into account
        for (String nameValuePair : cmd2Run.get(1).split("(?<!&)&(?!&)"))
        {
            nameValuePair = nameValuePair.replace("&&", "&");
            String[] nameValuePairArray = nameValuePair.split("(?<!=)=(?!=)");
            query.add(nameValuePairArray[0], Utils.runCmdNotationSubstitutions(nameValuePairArray[1].replace("==", "="), params.random.get()));
        }

        start = System.nanoTime();
        SolrDocumentList results = readSolrClient.query(query).getResults();
        metrics.update(Metrics.StatsType.OP_LATENCY, start, System.nanoTime(), true);
        metrics.update(Metrics.StatsType.DOC_COUNT, results == null ? 0 : results.getNumFound(), true);
        // Do we have a number of expected results to be parsed and verified?
        Long expectedNumResults = cmd2Run.size() == 3 ? Long.parseLong(cmd2Run.get(2)) : null;
        if (expectedNumResults != null && results != null && results.getNumFound() != expectedNumResults)
        {
            throw new Exception("Number of results not matched for command: " + Arrays.asList(cmd2Run) + " expected "
                    + expectedNumResults + " but found: " + results.getNumFound() + " instead.");
        }
    }
}
