package com.romankagan.dse.demos.solr.commands;

import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.probes.IndexingLatencyProbe;
import com.romankagan.dse.demos.solr.stats.Metrics;

import java.util.List;

public class CmdRunnerFactory
{
    public static CmdRunner getInstance(List<String> cmd2Run,
                                        CommandLine.Params params,
                                        Metrics metrics,
                                        IndexingLatencyProbe indexingLatencyProbe) throws Exception
    {
        String commandType = getCmdType(cmd2Run);

        if (commandType.equals(HttpReadCmdRunner.class.getName()))
        {
            return new HttpReadCmdRunner(params, metrics);
        }
        else if (commandType.equals(JavaDriverCmdRunner.class.getName()))
        {
            return new JavaDriverCmdRunner(params, metrics, indexingLatencyProbe);
        }
        else if (commandType.equals(LucenePerfCmdRunner.class.getName()))
        {
            return new LucenePerfCmdRunner(params, metrics);
        }
        else if (GenerateQueriesCmdRunner.class.getName().equals(commandType))
        {
            return new GenerateQueriesCmdRunner(params, metrics);
        }
        else if (PStatementCmdRunner.class.getName().equals(commandType))
        {
            return new PStatementCmdRunner(params, metrics, indexingLatencyProbe);
        }
        else if (LoadCmdRunner.class.getName().equals(commandType))
        {
            return new LoadCmdRunner(params, metrics, indexingLatencyProbe);
        }
        else
        {
            throw new Exception("Sorry I could not understand the command: " + cmd2Run.get(0));
        }
    }

    public static String getCmdType(List<String> cmd2Run) throws Exception
    {
        if (cmd2Run.get(0).toUpperCase().startsWith(HttpReadCmdRunner.CMD_STRING_ID))
        {
            return HttpReadCmdRunner.class.getName();
        }
        else if (cmd2Run.get(0).toUpperCase().startsWith(JavaDriverCmdRunner.CMD_STRING_ID))
        {
            return JavaDriverCmdRunner.class.getName();
        }
        else if (cmd2Run.get(0).toUpperCase().startsWith(LucenePerfCmdRunner.CMD_STRING_ID))
        {
            return LucenePerfCmdRunner.class.getName();
        }
        else if (cmd2Run.get(0).toUpperCase().startsWith(GenerateQueriesCmdRunner.CMD_STRING_ID))
        {
            return GenerateQueriesCmdRunner.class.getName();
        }
        else if (cmd2Run.get(0).toUpperCase().startsWith(PStatementCmdRunner.CMD_STRING_ID))
        {
            return PStatementCmdRunner.class.getName();
        }
        else if (cmd2Run.get(0).toUpperCase().startsWith(LoadCmdRunner.CMD_STRING_ID))
        {
            return LoadCmdRunner.class.getName();
        }
        else
        {
            throw new Exception("Sorry I could not understand the command: " + cmd2Run.get(0));
        }
    }
}
