package com.romankagan.dse.demos.solr.readers;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.romankagan.dse.demos.solr.commands.Constants.TestDataHeader.EXCLUSIVE_SEQUENTIAL;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.asBufferedReader;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.createTestInput;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.hasItems;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.readAllLines;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.readAllLinesConcurrently;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SequentialFileReaderTest
{
    @Test
    public void shouldReadAllLinesWithSingleThread()
    {
        // given:
        List<String> lines = createTestInput(1000);
        SequentialFileReader reader = new SequentialFileReader(asBufferedReader(lines), lines.size());

        // when:
        List<String> readLines = readAllLines(reader);

        // then:
        assertEquals(1000, Sets.newHashSet(readLines).size());
        assertThat(readLines, hasItems(lines));
    }

    @Test
    public void shouldReadAllLinesWithMultipleThreads() throws InterruptedException, ExecutionException
    {
        // given:
        List<String> lines = createTestInput(30_000);
        SequentialFileReader reader = new SequentialFileReader(asBufferedReader(lines), lines.size());

        // when:
        List<String> readLines = readAllLinesConcurrently(8, reader);

        // then:
        assertEquals(30_000, Sets.newHashSet(readLines).size());
        assertThat(readLines, hasItems(lines));
    }
}
