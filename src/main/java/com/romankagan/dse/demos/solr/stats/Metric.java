package com.romankagan.dse.demos.solr.stats;

import org.HdrHistogram.AtomicHistogram;

public class Metric
{
    //Configuration for histograms
    private static final long HIGHEST = Integer.MAX_VALUE;
    private static final long LOWEST = 1;
    private static final int VALUE_DIGITS = 3;

    private volatile AtomicHistogram current = new AtomicHistogram(LOWEST, HIGHEST, VALUE_DIGITS);
    private final AtomicHistogram total = current.copy();
    final AtomicHistogram failures = current.copy();

    public void update(long startNanos, long endNanos, boolean success)
    {
    }

    public void update(long value, boolean success)
    {
        AtomicHistogram targetHistogram = success ? current : failures;
        if (value > HIGHEST)
        {
            //Record up to value / HIGHEST instances of the highest value
            //hides slightly less information (we are lying either way).
            //Something of a deck chairs on the titanic situation, but
            //this is all worst case stuff anyways and we are usually
            //beyond caring about the exact magnitude of epic fail
            while (value > HIGHEST)
            {
                targetHistogram.recordValue(Math.min(HIGHEST, value));
                value -= HIGHEST;
            }
        }
        else
        {
            targetHistogram.recordValue(value);
        }
    }

    public boolean hasEvents()
    {
        return current.getTotalCount() > 0 || total.getTotalCount() > 0;
    }

    public AtomicHistogram swapAndCopy()
    {
        //Swap the histogram and hackily wait for anyone using it to finish
        AtomicHistogram histogram = current;
        current = new AtomicHistogram(LOWEST, HIGHEST, VALUE_DIGITS);
        try
        {
            Thread.sleep(100);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        total.add(histogram);
        return histogram;
    }

    public AtomicHistogram getTotal()
    {
        swapAndCopy();
        return total;
    }

    public long getTotalCount()
    {
        return total.getTotalCount();
    }
}
