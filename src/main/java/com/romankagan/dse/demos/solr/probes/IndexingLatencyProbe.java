package com.romankagan.dse.demos.solr.probes;

public interface IndexingLatencyProbe
{
    void maybeProbe(String id, long startNanos);
}
