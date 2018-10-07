package com.romankagan.dse.demos.solr.readers;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class InputReaderStrategyTestUtils
{
    public static List<String> createTestInput(int lines)
    {
        List<String> result = Lists.newArrayList();
        for (int i = 0; i < lines; ++i)
        {
            result.add(String.valueOf(i));
        }
        return ImmutableList.copyOf(result);
    }

    public static BufferedReader asBufferedReader(List<String> lines)
    {
        return new BufferedReader(new StringReader(Joiner.on("\n").join(lines)));
    }

    public static List<String> readAllLines(InputReaderStrategy reader)
    {
        List<String> readLines = Lists.newArrayList();
        Optional<String> nextLine;
        while ((nextLine = reader.getNextLine()).isPresent())
        {
            readLines.add(nextLine.get());
        }
        return readLines;
    }

    public static List<String> readAllLinesConcurrently(int threadCount, InputReaderStrategy reader) throws ExecutionException, InterruptedException
    {
        List<ReaderTask> readerTasks = createReaderTasks(threadCount, reader);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CompletionService<List<String>> completionService = new ExecutorCompletionService<>(executorService);

        for (ReaderTask readerTask : readerTasks)
        {
            completionService.submit(readerTask);
        }

        List<String> lines = readAllLines(completionService, threadCount);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
        return lines;
    }

    public static Matcher<Iterable<String>> hasItems(List<String> lines)
    {
        return CoreMatchers.hasItems(lines.toArray(new String[lines.size()]));
    }

    private static List<ReaderTask> createReaderTasks(int count, InputReaderStrategy reader)
    {
        List<ReaderTask> readers = Lists.newArrayList();
        for (int i = 0; i < count; ++i)
        {
            readers.add(new ReaderTask(reader));
        }
        return ImmutableList.copyOf(readers);
    }

    private static List<String> readAllLines(CompletionService<List<String>> completionService, int expectedResponses) throws InterruptedException, ExecutionException
    {
        List<String> lines = Lists.newArrayList();

        while (expectedResponses > 0)
        {
            Future<List<String>> completedFuture = completionService.take();
            expectedResponses--;
            lines.addAll(completedFuture.get());
        }
        return ImmutableList.copyOf(lines);
    }

    private static class ReaderTask implements Callable<List<String>>
    {
        private final InputReaderStrategy reader;

        private ReaderTask(InputReaderStrategy reader)
        {
            this.reader = reader;
        }

        @Override
        public List<String> call() throws Exception
        {
            return readAllLines(reader);
        }
    }
}
