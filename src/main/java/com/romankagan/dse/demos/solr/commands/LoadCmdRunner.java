package com.romankagan.dse.demos.solr.commands;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.romankagan.dse.demos.solr.probes.IndexingLatencyProbe;
import com.google.common.base.Supplier;

import org.yaml.snakeyaml.Yaml;

import com.romankagan.dse.demos.solr.CommandLine;
import com.romankagan.dse.demos.solr.load.CQLLoadStrategy;
import com.romankagan.dse.demos.solr.load.InputLoader;
import com.romankagan.dse.demos.solr.load.LoadStrategy;
import com.romankagan.dse.demos.solr.stats.Metrics;
import static com.romankagan.dse.demos.solr.commands.parsers.CommandArgumentParsers.checkArgument;
import static com.romankagan.dse.demos.solr.commands.parsers.CommandArgumentParsers.readArgumentValue;

public class LoadCmdRunner extends CmdRunner
{
    public final static String CMD_STRING_ID = "LOAD";

    private static final String LOADER_CLASS_ARG_NAME = "loader-class";
    private static final String LOADER_YAML_ARG_NAME = "config-yaml";
    private static final String LOADER_INPUT_FILE_ARG_NAME = "input-file";
    private static final String LOADER_LIMIT_ARG_NAME = "limit";
    private static final String LOADER_PROTOCOL_ARG_NAME = "protocol";

    protected final CommandLine.Params params;
    private final IndexingLatencyProbe indexingLatencyProbe;

    LoadCmdRunner(CommandLine.Params params, Metrics metrics, IndexingLatencyProbe indexingLatencyProbe) throws URISyntaxException
    {
        super(metrics);
        this.params = params;
        this.indexingLatencyProbe = indexingLatencyProbe;
    }

    @Override
    public void runCommand(List<String> cmd2Run) throws Throwable
    {
        try
        {
            runCommandImpl(cmd2Run);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            throw t;
        }
    }

    private void runCommandImpl(List<String> cmd2Run) throws Throwable
    {
        String inputReaderClassName = getLoaderClass(cmd2Run);
        if (inputReaderClassName.equalsIgnoreCase("geo"))
        {
            inputReaderClassName = "com.romankagan.dse.demos.solr.load.GeonamesLoader";
        }
        @SuppressWarnings("unchecked")
        Class<? extends InputLoader> inputReaderClass = (Class<? extends InputLoader>) Class.forName(inputReaderClassName);
        Constructor<? extends InputLoader> constructor = inputReaderClass.getConstructor(Path.class, long.class, Map.class, Supplier.class);
        Map options = getLoaderYaml(cmd2Run);

        File inputFile = getInputFile(cmd2Run);
        long limit = getLimit(cmd2Run);

        try (InputLoader inputReader = constructor.newInstance(inputFile.toPath(), limit, options, params.random))
        {
            long expectedEvents = inputReader.expectedLines();
            metrics.addToExpectedCommands(expectedEvents);

            String loadType = getLoaderProtocol(cmd2Run);
            LoadStrategy loadStrategy;
            switch (loadType.toUpperCase())
            {
                case "CQL":
                    loadStrategy = new CQLLoadStrategy(params, metrics, inputReader, indexingLatencyProbe);
                    break;
                default:
                    throw new RuntimeException("Supported load types are CQL and HTTP, found " + loadType);
            }

            loadStrategy.execute();
        }
    }

    private String getLoaderClass(Iterable<String> commandArguments)
    {
        Optional<String> output = readArgumentValue(commandArguments, LOADER_CLASS_ARG_NAME);
        checkArgument(output, LOADER_CLASS_ARG_NAME);
        String inputReaderClassName = output.get();
        if (inputReaderClassName.equalsIgnoreCase("geo"))
        {
            inputReaderClassName = "com.romankagan.dse.demos.solr.load.GeonamesLoader";
        }
        return inputReaderClassName;
    }

    private Map getLoaderYaml(Iterable<String> commandArguments)
    {
        Optional<String> output = readArgumentValue(commandArguments, LOADER_YAML_ARG_NAME);
        if (!output.isPresent())
        {
            return Collections.EMPTY_MAP;
        }
        Yaml yaml = new Yaml();
        Map options = (Map)yaml.load(output.get());
        return options;
    }

    private File getInputFile(Iterable<String> commandArguments)
    {
        Optional<String> output = readArgumentValue(commandArguments, LOADER_INPUT_FILE_ARG_NAME);
        checkArgument(output, LOADER_INPUT_FILE_ARG_NAME);
        File inputFile = new File(output.get());
        if (!inputFile.exists())
        {
            throw new RuntimeException("Input file " + inputFile + " does not exist");
        }
        if (!inputFile.canRead())
        {
            throw new RuntimeException("Input file " + inputFile + " is not readable");
        }
        return inputFile;
    }

    private long getLimit(Iterable<String> commandArguments)
    {
        Optional<String> output = readArgumentValue(commandArguments, LOADER_LIMIT_ARG_NAME);
        if (!output.isPresent())
        {
            return Long.MAX_VALUE;
        }
        long limit = Long.valueOf(output.get());
        if (limit < 0)
        {
            throw new RuntimeException("Limit must be >= 0. Configured limit  " + limit + ".");
        }
        if (limit == 0)
        {
            limit = Long.MAX_VALUE;
        }
        return limit;
    }

    private String getLoaderProtocol(Iterable<String> commandArguments)
    {
        Optional<String> output = readArgumentValue(commandArguments, LOADER_PROTOCOL_ARG_NAME);
        if (!output.isPresent())
        {
            return "CQL";
        }
        return output.get();
    }
}
