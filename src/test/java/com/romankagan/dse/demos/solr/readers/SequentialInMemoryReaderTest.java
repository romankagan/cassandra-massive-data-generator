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

public class SequentialInMemoryReaderTest
{
    @Test
    public void shouldReadLinesUntilSatisfyingLoopCount()
    {
        // given:
        List<String> lines = createTestInput(1000);
        SequentialInMemoryReader reader = new SequentialInMemoryReader(
                asBufferedReader(lines),
                10,
                lines.size());

        // when:
        List<String> readLines = readAllLines(reader);

        // then:
        assertEquals(10_000, readLines.size());
        assertEquals(1000, Sets.newHashSet(readLines).size());
        assertThat(lines, hasItems(readLines));
    }

    @Test
    public void shouldReadLinesUntilSatisfyingLoopCountUsingMultipleThreads() throws ExecutionException, InterruptedException
    {
        // given:
        List<String> lines = createTestInput(1000);
        SequentialInMemoryReader reader = new SequentialInMemoryReader(
                asBufferedReader(lines),
                10,
                lines.size());

        // when:
        List<String> readLines = readAllLinesConcurrently(8, reader);

        // then:
        assertEquals(80_000, readLines.size());
        assertEquals(1000, Sets.newHashSet(readLines).size());
        assertThat(lines, hasItems(readLines));
    }

    @Test
    public void shouldNotLoop()
    {
        // given:
        List<String> lines = createTestInput(1000);
        SequentialInMemoryReader reader = new SequentialInMemoryReader(
                asBufferedReader(lines),
                0,
                lines.size());

        // when:
        List<String> readLines = readAllLines(reader);

        // then:
        assertEquals(1000, Sets.newHashSet(readLines).size());
        assertThat(lines, hasItems(readLines));
    }

    @Test
    public void shouldReadZeroUsingMultipleThreads() throws ExecutionException, InterruptedException
    {
        // given:
        List<String> lines = createTestInput(1000);
        SequentialInMemoryReader reader = new SequentialInMemoryReader(
                asBufferedReader(lines),
                0,
                lines.size());

        // when:
        List<String> readLines = readAllLinesConcurrently(8, reader);

        // then:
        assertEquals(8_000, readLines.size());
        assertEquals(1000, Sets.newHashSet(readLines).size());
        assertThat(lines, hasItems(readLines));
    }
}
