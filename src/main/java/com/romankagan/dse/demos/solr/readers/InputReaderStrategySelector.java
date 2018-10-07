package com.romankagan.dse.demos.solr.readers;

import com.romankagan.bdp.shade.com.google.common.collect.ImmutableMap;
import com.romankagan.dse.demos.solr.Utils;
import com.romankagan.dse.demos.solr.commands.Constants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Supplier;

import static com.romankagan.dse.demos.solr.commands.Constants.TestDataHeader.EXCLUSIVE_RANDOM;
import static com.romankagan.dse.demos.solr.commands.Constants.TestDataHeader.EXCLUSIVE_SEQUENTIAL;
import static com.romankagan.dse.demos.solr.commands.Constants.TestDataHeader.SHARED_SEQUENTIAL;

public class InputReaderStrategySelector
{
    private final Map<Constants.TestDataHeader, InputReaderStrategySupplier> strategies;

    public InputReaderStrategySelector(Supplier<Random> random, long loopCount)
    {
        this.strategies = getAvailableStrategies(random, loopCount);
    }

    public InputReaderStrategy createInputReader(File input)
    {
        try
        {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(input));
            Constants.TestDataHeader header = peekDataHeader(bufferedReader);
            return strategies.get(header).get(bufferedReader, Utils.countLinesApproximate(input) - 1);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private Map<Constants.TestDataHeader, InputReaderStrategySupplier> getAvailableStrategies(final Supplier<Random> random, final long loopCount)
    {
        return ImmutableMap.of(
                EXCLUSIVE_SEQUENTIAL, new InputReaderStrategySupplier()
                {
                    @Override
                    public InputReaderStrategy get(BufferedReader input, int lineCount)
                    {
                        return new SequentialInMemoryReader(input, loopCount, lineCount);
                    }
                },
                EXCLUSIVE_RANDOM, new InputReaderStrategySupplier()
                {
                    @Override
                    protected InputReaderStrategy get(BufferedReader input, int lineCount)
                    {
                        return new RandomInMemoryReader( input, random, loopCount, lineCount);
                    }
                },
                SHARED_SEQUENTIAL, new InputReaderStrategySupplier()
                {
                    @Override
                    protected InputReaderStrategy get(BufferedReader input, int lineCount)
                    {
                        return new SequentialFileReader(input, lineCount);
                    }
                });
    }

    private Constants.TestDataHeader peekDataHeader(BufferedReader reader) throws IOException
    {
        String headerLine = reader.readLine();
        return Constants.TestDataHeader.byName(headerLine);
    }

    private abstract static class InputReaderStrategySupplier
    {
        protected abstract InputReaderStrategy get(BufferedReader input, int lineCount);
    }
}
