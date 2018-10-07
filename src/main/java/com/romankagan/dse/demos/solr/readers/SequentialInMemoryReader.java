package com.romankagan.dse.demos.solr.readers;

import com.google.common.base.Optional;

import java.io.BufferedReader;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SequentialInMemoryReader extends InputReaderStrategy
{
    private final List<String> lines;
    private final ThreadLocal<AtomicLong> indexCounter = new ThreadLocal<AtomicLong>()
    {
        @Override
        protected AtomicLong initialValue()
        {
            return new AtomicLong(0);
        }
    };
    private final ThreadLocal<AtomicLong> repeatsCounter;
    private final long loopCount;

    protected SequentialInMemoryReader(BufferedReader input, final long loopCount, int lineCount)
    {
        super(input, lineCount);
        this.lines = readAllRemainingLines();
        this.repeatsCounter = new ThreadLocal<AtomicLong>()
        {
            @Override
            protected AtomicLong initialValue()
            {
                return new AtomicLong(loopCount);
            }
        };
        this.loopCount = loopCount;
    }

    @Override
    public Optional<String> getNextLine()
    {
        long index = indexCounter.get().getAndIncrement();
        if (index < lines.size())
        {
            return Optional.of(lines.get((int)index));
        }
        else if (repeatsCounter.get().decrementAndGet() > 0)
        {
            indexCounter.get().set(0);
            return getNextLine();
        }
        return Optional.absent();
    }

    @Override
    public long getExpectedEvents(int clients)
    {
        return loopCount * clients * (long)lineCount;
    }
}
