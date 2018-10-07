package com.romankagan.dse.demos.solr.readers;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import java.io.BufferedReader;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RandomInMemoryReader extends InputReaderStrategy
{
    private final List<String> lines;
    private final AtomicLong repeatCount;
    private final Supplier<Random> random;

    protected RandomInMemoryReader(BufferedReader input, Supplier<Random> random, long loopCount, int lineCount)
    {
        super(input, lineCount);
        this.random = random;
        this.lines = readAllRemainingLines();
        this.repeatCount = new AtomicLong(loopCount);
    }

    @Override
    public Optional<String> getNextLine()
    {
        if (repeatCount.getAndDecrement() > 0)
        {
            int index = random.get().nextInt(lines.size());
            return Optional.of(lines.get(index));
        }
        return Optional.absent();
    }

    @Override
    public long getExpectedEvents(int clients)
    {
        return repeatCount.get();
    }
}
