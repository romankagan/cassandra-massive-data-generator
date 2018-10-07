package com.romankagan.dse.demos.solr.load;

import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.probes.IndexingLatencyProbe;
import com.romankagan.dse.demos.solr.stats.Metrics;

public abstract class LoadStrategy
{
    protected final CommandLine.Params params;
    protected final Metrics metrics;
    protected final InputLoader reader;
    protected final IndexingLatencyProbe indexingLatencyProbe;


    protected LoadStrategy(CommandLine.Params params, Metrics metrics, InputLoader reader, IndexingLatencyProbe indexingLatencyProbe)
    {
        this.params = params;
        this.metrics = metrics;
        this.reader = reader;
        this.indexingLatencyProbe = indexingLatencyProbe;
    }

    public abstract void execute();
}
