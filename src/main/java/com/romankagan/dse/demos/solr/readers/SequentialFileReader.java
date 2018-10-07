package com.romankagan.dse.demos.solr.readers;

import com.google.common.base.Optional;

import java.io.BufferedReader;
import java.io.IOException;

public class SequentialFileReader extends InputReaderStrategy
{

    protected SequentialFileReader(BufferedReader input, int lineCount)
    {
        super(input, lineCount);
    }

    @Override
    public Optional<String> getNextLine()
    {
        try
        {
            return Optional.fromNullable(reader.readLine());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getExpectedEvents(int clients)
    {
        return lineCount;
    }
}
