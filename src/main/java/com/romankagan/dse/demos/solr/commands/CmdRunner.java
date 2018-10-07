package com.romankagan.dse.demos.solr.commands;

import java.util.List;

import com.romankagan.dse.demos.solr.SolrStress;
import com.romankagan.dse.demos.solr.stats.Metrics;

public abstract class CmdRunner
{
    protected final Metrics metrics;
    protected long start;

    protected CmdRunner(Metrics metrics)
    {
        this.metrics = metrics;
    }

    public void executeCommand(List<String> cmd2Run)
    {
        try
        {
            runCommand(cmd2Run);
        }
        catch (Throwable t)
        {
            //Exceptions thrown when running a command are not fatal, and are logged and counted
            //in stats
            SolrStress.logException(t);
            metrics.update(Metrics.StatsType.OP_LATENCY, start, System.nanoTime(), false);
        }
    }

    public abstract void runCommand(List<String> cmd2Run) throws Throwable;
}
