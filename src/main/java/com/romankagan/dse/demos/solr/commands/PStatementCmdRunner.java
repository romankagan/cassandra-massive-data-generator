package com.romankagan.dse.demos.solr.commands;

import java.util.List;

import com.romankagan.dse.demos.solr.probes.IndexingLatencyProbe;
import org.jctools.maps.NonBlockingHashMap;

import com.romankagan.driver.core.PreparedStatement;
import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.Utils;
import com.romankagan.dse.demos.solr.stats.Metrics;

public class PStatementCmdRunner extends JavaDriverCmdRunner
{
    public final static String CMD_STRING_ID = "P_STMNT";
    private static final NonBlockingHashMap<String, PreparedStatement> pstmntCache = new NonBlockingHashMap<>();

    PStatementCmdRunner(CommandLine.Params params, Metrics metrics, IndexingLatencyProbe indexingLatencyProbe) throws Exception
    {
        super(params, metrics, indexingLatencyProbe);
    }

    @Override
    protected StatementAndProbeId getStatement(List<String> cmd2Run)
    {
        String cql = cmd2Run.get(1);
        PreparedStatement pstmnt = pstmntCache.get(cql);
        if (pstmnt == null)
        {
            synchronized(pstmntCache)
            {
                pstmnt = pstmntCache.get(cql);
                if (pstmnt == null)
                {
                    pstmnt = session.prepare(cql);
                    pstmntCache.put(cql, pstmnt);
                }
            }
        }

        Object[] values = cmd2Run.subList(2, cmd2Run.size()).toArray();
        for (int i = 0; i < values.length; i++)
        {
            values[i] = Utils.runCmdNotationSubstitutions((String) values[i], params.random.get());
        }

        String probeId = null;
        if (isUpdateOrInsert(cmd2Run.get(1)))
        {
            try
            {
                //Column may not exist for some schemas if it wasn't specified correctly as a parameter
                int index = pstmnt.getVariables().getIndexOf(params.indexingLatencyProbeColumn);
                if (index > -1)
                {
                    probeId = (String) values[index];
                }
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                System.exit(0);
            }
        }

        return new StatementAndProbeId(pstmnt.bind(values).setFetchSize(params.fetchSize), probeId);
    }
}
