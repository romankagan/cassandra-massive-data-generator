package com.romankagan.dse.demos.solr.commands;

import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.generators.QueryDecoratorType;
import com.romankagan.dse.demos.solr.generators.QueryGenerator;
import com.romankagan.dse.demos.solr.stats.Metrics;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.romankagan.dse.demos.solr.commands.parsers.CommandArgumentParsers.checkArgument;
import static com.romankagan.dse.demos.solr.commands.parsers.CommandArgumentParsers.readArgumentAsLong;
import static com.romankagan.dse.demos.solr.commands.parsers.CommandArgumentParsers.readArgumentValue;
import static com.google.common.collect.ImmutableList.builder;

/**
 * Generates boolean queries from terms fetched from TermsComponent.
 */
public class GenerateQueriesCmdRunner extends CmdRunner
{
    public static final String CMD_STRING_ID = "GENERATE_QUERIES";

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateQueriesCmdRunner.class);
    private static final long DEFAULT_NUM_BOOLEAN_QUERIES = 25;
    private static final long DEFAULT_NUM_TERM_QUERIES = 25;
    private static final long DEFAULT_TERMS_LIMIT_QUERIES = 500;
    private static final String BOOLEAN_QUERIES_COUNT_ARG_NAME = "boolean-queries";
    private static final String TERM_QUERIES_COUNT_ARG_NAME = "term-queries";
    private static final String TERMS_LIMIT_ARG_NAME = "terms-limit";
    private static final String FIELDS_ARG_NAME = "fields";
    private static final String OUTPUT_FILE_ARG_NAME = "output";
    private static final String API_TYPE_ARG_NAME = "api";
    private final String indexName;
    private final String hostname;

    public GenerateQueriesCmdRunner(CommandLine.Params params, Metrics metrics)
    {
        super(metrics);
        this.indexName = params.indexName;
        this.hostname = Iterables.getFirst(params.urls, "http://localhost:8983");
    }

    @Override
    public void runCommand(List<String> commandArguments) throws Throwable
    {
        start = System.nanoTime();
        Stopwatch stopwatch = Stopwatch.createStarted();
        QueryGenerator queryGenerator = new QueryGenerator(hostname, indexName);
        Iterable<String> fields = getFields(commandArguments);
        List<String> queries = queryGenerator.generateQueries(
                fields,
                getTermsLimit(commandArguments),
                getTermQueriesCount(commandArguments),
                getBooleanQueriesCount(commandArguments),
                getQueryDecoratorType(commandArguments));
        LOGGER.info("Generated {} queries in {} ms", queries.size(), stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));

        stopwatch = Stopwatch.createStarted();
        String outputFileLocation = getOutputFileName(commandArguments);
        IOUtils.writeLines(
                builder().add(Constants.TestDataHeader.EXCLUSIVE_SEQUENTIAL.getName()).addAll(queries).build(),
                "\n",
                new FileOutputStream(outputFileLocation));
        LOGGER.info("Saved queries to a file '{}' in {} ms",
                outputFileLocation,
                stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));

    }

    private long getBooleanQueriesCount(Iterable<String> commandArguments)
    {
        return readArgumentAsLong(commandArguments, BOOLEAN_QUERIES_COUNT_ARG_NAME, DEFAULT_NUM_BOOLEAN_QUERIES);
    }

    private long getTermQueriesCount(Iterable<String> commandArguments)
    {
        return readArgumentAsLong(commandArguments, TERM_QUERIES_COUNT_ARG_NAME, DEFAULT_NUM_TERM_QUERIES);
    }

    private int getTermsLimit(Iterable<String> commandArguments)
    {
        return (int) readArgumentAsLong(commandArguments, TERMS_LIMIT_ARG_NAME, DEFAULT_TERMS_LIMIT_QUERIES);
    }

    private String getOutputFileName(Iterable<String> commandArguments)
    {
        Optional<String> output = readArgumentValue(commandArguments, OUTPUT_FILE_ARG_NAME);
        checkArgument(output, OUTPUT_FILE_ARG_NAME);
        return output.get();
    }

    private Iterable<String> getFields(Iterable<String> commandArguments)
    {
        Optional<String> fields = readArgumentValue(commandArguments, FIELDS_ARG_NAME);
        checkArgument(fields, FIELDS_ARG_NAME);
        return Splitter.on(",").omitEmptyStrings().split(fields.get());
    }

    private QueryDecoratorType getQueryDecoratorType(Iterable<String> commandArguments)
    {
        Optional<String> apiType = readArgumentValue(commandArguments, API_TYPE_ARG_NAME);
        checkArgument(apiType, API_TYPE_ARG_NAME);
        return QueryDecoratorType.valueOf(apiType.get());
    }
}
