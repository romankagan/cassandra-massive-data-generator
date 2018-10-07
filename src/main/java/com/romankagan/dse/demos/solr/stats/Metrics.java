package com.romankagan.dse.demos.solr.stats;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.AtomicHistogram;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.HdrHistogram.Histogram;

import com.romankagan.dse.demos.solr.stats.JMXMetrics.Type;

public class Metrics
{
    public enum StatsType
    {
        DOC_COUNT,
        OP_LATENCY,
        INDEX_LATENCY,
    }

    EnumMap<StatsType, Metric> metrics = new EnumMap<>(StatsType.class);

    //Count of # of reports used to drop column headers occasionally
    private long reportCount = 0;

    //Track the last time the reporter ran
    private long lastReportTimeNanos = System.nanoTime();

    //Thread to run the reporter in
    private final ScheduledExecutorService ses =
            Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Reporter").build());

    //Task to cancel and wait on reporter
    private final ScheduledFuture<?> reporter;

    //Number of expected commands used to drive completed % indicator
    private volatile long expectedCommands;
    private final AtomicLong commandsExecuted = new AtomicLong();

    //Start time of measurement, used to track total duration
    private final long startNanos;

    private final List<PrintStream> targets;

    private final DateFormat df = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss.SSS");

    private final long startTimeNanos = System.nanoTime();

    private final Long endTimeNanos;

    private final JMXMetrics jmxMetrics;

    private final Runnable reportRunnable = new Runnable() {
        @Override
        public void run() {
            try
            {
                //Fetching metrics can take a bit of time since it is multiple RPC calls
                Map<Type, Histogram> jmxValues = jmxMetrics.getLatest();

                //Track time
                long nowNanos = System.nanoTime();
                Date now = new Date();
                long timeDelta = nowNanos - lastReportTimeNanos;
                lastReportTimeNanos = nowNanos;

                Metric latencyMetric = metrics.get(StatsType.OP_LATENCY);
                long failures = latencyMetric.failures.getTotalCount();
                AtomicHistogram histogram = latencyMetric.swapAndCopy();
                //Operations per second
                int rate = (int) (histogram.getTotalCount() / (timeDelta / 1000000000.0));

                //Drop a column header occasionally
                if (reportCount % 40 == 0)
                {
                    println(String.format("%23s, %13s, %13s, %10s, %10s, %10s, %12s, %12s, %14s, %14s, %12s, %12s",
                            "Date & Time GMT",
                            "Success total",
                            "Failure total",
                            "% complete",
                            "ops/second",
                            "99%tile ms",
                            "99.9%tile ms",
                            "QUEUE P99 ms",
                            "PREPARE P99 ms",
                            "EXECUTE P99 ms",
                            "FLUSH P99 ms",
                            "MERGE P99 ms"),
                            reportCount != 0);
                }
                reportCount++;

                //Log interval statistics
                if (jmxValues.isEmpty())
                {
                    println(String.format("%23s, %13d, %13d, %10.2f, %10d, %10.3f, %12.3f, %12s, %14s, %14s, %12s, %12s",
                            df.format(now),
                            latencyMetric.getTotalCount(),
                            failures,
                            getPercentCompletion(),
                            rate,
                            getValueAtPercentile(histogram, 99.0) / 1000.0,
                            getValueAtPercentile(histogram, 99.9) / 1000.0,
                            "n/a",
                            "n/a",
                            "n/a",
                            "n/a",
                            "n/a"));
                }
                else
                {
                    println(String.format("%23s, %13d, %13d, %10.2f, %10d, %10.3f, %12.3f, %12.3f, %14.3f, %14.3f",
                            df.format(now),
                            latencyMetric.getTotalCount(),
                            failures,
                            getPercentCompletion(),
                            rate,
                            getValueAtPercentile(histogram, 99.0) / 1000.0,
                            getValueAtPercentile(histogram, 99.9) / 1000.0,
                            getValueAtPercentile(jmxValues.get(Type.UPDATE_QUEUE), 99.0) / 1000.0,
                            getValueAtPercentile(jmxValues.get(Type.UPDATE_PREPARE), 99.0) / 1000.0,
                            getValueAtPercentile(jmxValues.get(Type.UPDATE_EXECUTE), 99.0) / 1000.0));
                }
            }
            catch (Throwable t)
            {
                //Log the error here, since the scheduled future will likely swallow it.
                t.printStackTrace();
                throw new RuntimeException(t);
            }

        }
    };

    public Metrics(long reportInterval,
                   TimeUnit reportIntervalUnit,
                   long expectedCommands,
                   List<PrintStream> targets,
                   Long endTimeNanos,
                   JMXMetrics jmxMetrics)
    {
        this.expectedCommands = expectedCommands;
        this.targets = targets;
        for (StatsType type : StatsType.values())
        {
            metrics.put(type, new Metric());
        }
        startNanos = System.nanoTime();
        long reportIntervalNanos = reportIntervalUnit.toNanos(reportInterval) - TimeUnit.MILLISECONDS.toNanos(100);
        reportIntervalNanos = Math.max(0, reportIntervalNanos);
        reporter = ses.scheduleWithFixedDelay(reportRunnable, reportIntervalNanos, reportIntervalNanos, TimeUnit.NANOSECONDS);
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        this.endTimeNanos = endTimeNanos;
        this.jmxMetrics = jmxMetrics;
    }

    public void update(StatsType metric, long startNanos, long endNanos, boolean success)
    {
        Preconditions.checkArgument(endNanos >= startNanos);
        long delta = endNanos - startNanos;
        long micros = TimeUnit.NANOSECONDS.toMicros(delta);
        update(metric, micros, success);
    }

    public void update(StatsType metric, long value, boolean success)
    {
        metrics.get(metric).update(value, success);
    }


    public void finalReport()
    {
        long delta = System.nanoTime() - startNanos;
        Date now = new Date();
        double seconds = delta / 1000000000.0;
        double minutes = seconds / 60.0;
        reporter.cancel(false);
        ses.shutdown();
        try
        {
            ses.awaitTermination(365, TimeUnit.DAYS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        Map<Type, Histogram> finalValues = jmxMetrics.getTotals();
        if (!finalValues.isEmpty())
        {
            printHistogramIfNotEmpty("Update time QUEUE histogram in milliseconds", finalValues.get(Type.UPDATE_QUEUE), 1000.0);
            printHistogramIfNotEmpty("Update time PREPARE histogram in milliseconds", finalValues.get(Type.UPDATE_PREPARE), 1000.0);
            printHistogramIfNotEmpty("Update time EXECUTE histogram in milliseconds", finalValues.get(Type.UPDATE_EXECUTE), 1000.0);
            printHistogramIfNotEmpty("IndexWriter#getReader time histogram in milliseconds", finalValues.get(Type.IR_GET), 1000.0);
            printHistogramIfNotEmpty("DirectoryReader#open time histogram in milliseconds", finalValues.get(Type.IR_REOPEN), 1000.0);
            printHistogramIfNotEmpty("IndexWriter#maybeApplyDeletes time histogram in milliseconds", finalValues.get(Type.IR_APPLY_DELETES), 1000.0);
        }

        Metric latencyMetrics = metrics.get(StatsType.OP_LATENCY);
        if (latencyMetrics.hasEvents())
        {
            AtomicHistogram totalLatencyHistogram = metrics.get(StatsType.OP_LATENCY).getTotal();

            println(String.format("Finished at %s GMT, Final duration %.2f minutes rate %.2f ops/second with %d errors",
                    df.format(now),
                    minutes,
                    totalLatencyHistogram.getTotalCount() / seconds,
                    latencyMetrics.failures.getTotalCount()));
            println("Latency histogram in milliseconds");
            printHistogram(totalLatencyHistogram, 1000.0);
            if (latencyMetrics.failures.getTotalCount() > 0)
            {
                println("Failures latency histogram in milliseconds");
                printHistogram(latencyMetrics.failures, 1000.0);
            }
        }
        else
        {
            println(String.format("Final duration %.2f minutes", minutes));
        }

        Metric docsMetric = metrics.get(StatsType.DOC_COUNT);
        if (docsMetric.hasEvents())
        {
            println("Doc read count histogram");
            printHistogram(docsMetric.getTotal(), 1.0);
        }

        Metric indexLatencyMetric = metrics.get(StatsType.INDEX_LATENCY);
        if (indexLatencyMetric.hasEvents())
        {
            AtomicHistogram indexLatencyHistogram = indexLatencyMetric.getTotal();
            println(String.format("Finished at %s GMT, Final duration %.2f minutes", df.format(now), minutes));
            println("Index latency histogram in milliseconds");
            printHistogram(indexLatencyHistogram, 1000.0);
        }
    }

    public boolean hasEvents()
    {
        boolean hasEvents = false;
        for (Metric m : metrics.values())
        {
            hasEvents |= m.hasEvents();
        }
        return hasEvents;
    }

    public synchronized void addToExpectedCommands(long amount)
    {
        expectedCommands += amount;
    }

    public void logExecutedCommands(long executedCount)
    {
        commandsExecuted.addAndGet(executedCount);
    }

    public Metric getMetric(StatsType metric)
    {
        return metrics.get(metric);
    }

    private double getPercentCompletion()
    {
        if (endTimeNanos.equals(Long.MAX_VALUE))
        {
            return 100.0 * (commandsExecuted.get() / (double) expectedCommands);
        }
        else
        {
            long durationNanos = endTimeNanos - startTimeNanos;
            long elapsed = System.nanoTime() - startTimeNanos;
            return 100.0 * (elapsed / (double)durationNanos);
        }
    }

    private void printHistogram(AbstractHistogram h, double scaling)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        h.outputPercentileDistribution(ps, 1, scaling);
        ps.flush();
        print(new String(baos.toByteArray()));
    }

    private void print(String str)
    {
        for (PrintStream target : targets)
        {
            target.print(str);
        }
    }

    private void println(String str)
    {
        println(str, false);
    }

    private void println(String str, boolean isHeader)
    {
        for (PrintStream target : targets)
        {
            //Only repeat headers to stdout/stderr for human consumers
            if (isHeader && (target != System.out && target != System.err))
                continue;
            target.print(str);
            target.print(System.lineSeparator());
        }
    }

    private long getValueAtPercentile(AbstractHistogram histogram, double percentile)
    {
        if (histogram.getTotalCount() == 0)
        {
            return 0;
        }
        return histogram.getValueAtPercentile(percentile);
    }

    private void printHistogramIfNotEmpty(String intro, AbstractHistogram histogram, double scaling)
    {
        if (histogram.getTotalCount() > 0)
        {
            println(intro);
            printHistogram(histogram, scaling);
        }
    }

}
