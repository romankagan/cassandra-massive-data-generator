package com.romankagan.dse.demos.solr.readers;

import com.romankagan.dse.demos.solr.commands.LucenePerfCmdRunner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.jctools.maps.NonBlockingHashMap;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.romankagan.dse.demos.solr.commands.Constants.TEST_DATA_VALUES_DELIMITER;

public abstract class InputReaderStrategy implements Closeable
{
    //How many lines are feasible to cache instead of processing directly
    private static final int CACHE_THRESHOLD = 10000;
    private final NonBlockingHashMap<String, List<String>> analyzedLines = new NonBlockingHashMap<>();
    protected final BufferedReader reader;
    protected final int lineCount;

    protected InputReaderStrategy(BufferedReader reader, int lineCount)
    {
        this.reader = reader;
        this.lineCount = lineCount;
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeQuietly(reader);
    }

    public List<String> getNextCommand()
    {
        while (true)
        {
            Optional<String> line = getNextLine();
            if (line.isPresent())
            {
                List<String> analyzedLine;
                if (lineCount > CACHE_THRESHOLD)
                {
                    analyzedLine = analyzeSingleLine(line.get(), false);
                }
                else
                {
                    analyzedLine = cachedAnalyzedLine(line.get());
                }
                if (analyzedLine.isEmpty())
                {
                    // nothing here, try another line
                    continue;
                }
                return analyzedLine;
            }
            return Collections.emptyList();
        }
    }

    abstract public long getExpectedEvents(int clients);

    protected abstract Optional<String> getNextLine();

    protected List<String> readAllRemainingLines()
    {
        try
        {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (String s : IOUtils.readLines(reader))
            {
                builder.add(s.intern());
            }
            return builder.build();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private List<String> analyzeSingleLine(String line, boolean intern)
    {
        if (line.startsWith("#"))
        {
            return Collections.emptyList();
        }
        else if (!line.startsWith(LucenePerfCmdRunner.CMD_STRING_ID))
        {
            line = line.replaceAll("#.*", "");
        }

        if (!line.isEmpty())
        {
            // Take escaping (character doubling) into account
            List<String> cmdSplitArray = Arrays.asList(
                    line.split(
                            "(?<![" + TEST_DATA_VALUES_DELIMITER + "])[" + TEST_DATA_VALUES_DELIMITER + "](?![" + TEST_DATA_VALUES_DELIMITER + "])"));
            for (int i = 0; i < cmdSplitArray.size(); i++)
            {
                String cmd = cmdSplitArray.get(i).replaceAll("[|][|]", "|");
                if (intern)
                {
                    cmd = cmd.intern();
                }
                cmdSplitArray.set(i, cmd);
            }
            return ImmutableList.copyOf(cmdSplitArray);
        }

        return Collections.emptyList();
    }

    private List<String> cachedAnalyzedLine(String line)
    {
        List<String> analyzedLine = analyzedLines.get(line);
        if (analyzedLine == null)
        {
            analyzedLine = analyzeSingleLine(line, true);
            List<String> existingAnalyzedLine = analyzedLines.putIfAbsent(line, analyzedLine);
            if (existingAnalyzedLine != null)
                return existingAnalyzedLine;
        }
        return analyzedLine;
    }
}
