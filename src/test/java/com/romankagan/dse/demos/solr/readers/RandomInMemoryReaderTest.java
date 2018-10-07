package com.romankagan.dse.demos.solr.readers;

import com.romankagan.dse.demos.solr.commands.Constants;

import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.asBufferedReader;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.createTestInput;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.hasItems;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.readAllLines;
import static com.romankagan.dse.demos.solr.readers.InputReaderStrategyTestUtils.readAllLinesConcurrently;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RandomInMemoryReaderTest
{
    @Test
    public void shouldReadExpectedLinesCount()
    {
        // given:
        List<String> lines = createTestInput(1000);
        RandomInMemoryReader reader = new RandomInMemoryReader(
                asBufferedReader(lines),
                Suppliers.ofInstance(new Random()),
                100,
                lines.size());

        // when:
        List<String> readLines = readAllLines(reader);

        // then:
        assertEquals(100, readLines.size());
        assertThat(lines, hasItems(readLines));
    }

    @Test
    public void shouldReadExpectedNumberOfLinesUsingMultipleThreads() throws InterruptedException, ExecutionException
    {
        // given:
        List<String> lines = createTestInput(16_000);
        RandomInMemoryReader reader = new RandomInMemoryReader(
                asBufferedReader(lines),
                Suppliers.ofInstance(new Random()),
                5_300,
                lines.size());

        // when:
        List<String> readLines = readAllLinesConcurrently(8, reader);

        // then:
        assertEquals(5_300, readLines.size());
    }

    @Test
    public void shouldAllowDuplicatedLinesToSatisfyExpectedLinesCount()
    {
        // given:
        List<String> lines = createTestInput(10);
        RandomInMemoryReader reader = new RandomInMemoryReader(
                asBufferedReader(lines),
                Suppliers.ofInstance(new Random()),
                100,
                lines.size());

        // when:
        List<String> readLines = readAllLines(reader);

        // then:
        assertEquals(100, readLines.size());
        assertEquals(10, Sets.newHashSet(readLines).size());
        assertThat(lines, hasItems(readLines));
    }

    @Test
    public void shouldReadZeroLines()
    {
        // given:
        List<String> lines = createTestInput(1000);
        RandomInMemoryReader reader = new RandomInMemoryReader(
                asBufferedReader(lines),
                Suppliers.ofInstance(new Random()),
                0,
                lines.size());

        // when:
        List<String> readLines = readAllLines(reader);

        // then:
        assertTrue(readLines.isEmpty());
    }

    @Test
    public void shouldReadZeroLinesUsingMultipleThreads() throws InterruptedException, ExecutionException
    {
        // given:
        List<String> lines = createTestInput(16_000);
        RandomInMemoryReader reader = new RandomInMemoryReader(
                asBufferedReader(lines),
                Suppliers.ofInstance(new Random()),
                0,
                lines.size());

        // when:
        List<String> readLines = readAllLinesConcurrently(8, reader);

        // then:
        assertTrue(readLines.isEmpty());
    }
}
