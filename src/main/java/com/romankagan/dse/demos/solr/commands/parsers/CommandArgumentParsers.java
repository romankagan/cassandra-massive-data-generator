package com.romankagan.dse.demos.solr.commands.parsers;

import com.romankagan.dse.demos.solr.Utils;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Random;

public class CommandArgumentParsers
{
    public static long readArgumentAsLong(Iterable<String> commandArguments, String argumentName, long defaultValue)
    {
        return readArgumentValue(commandArguments, argumentName)
                .transform(new ParseLongFunction())
                .or(defaultValue);
    }

    public static Map<String, String> readArgumentsAsMap(Iterable<String> commandArguments, Random random)
    {
        Map<String, String> result = Maps.newHashMap();
        for (String argument : commandArguments)
        {
            String[] explodedField = argument.replace("&&", "&").split("(?<!=)=(?!=)");
            String fieldName = explodedField[0];
            String fieldValue = Utils.runCmdNotationSubstitutions(explodedField[1].replace("==", "="), random);
            result.put(fieldName, fieldValue);
        }
        return result;
    }

    public static Optional<String> readArgumentValue(Iterable<String> commandArguments, String argumentName)
    {
        return Iterables.tryFind(commandArguments, Predicates.containsPattern(argumentName))
                .transform(new SubstringAfterFunction("="));
    }

    public static void checkArgument(Optional<?> optional, String argName)
    {
        Preconditions.checkArgument(optional.isPresent(), String.format("Missing required argument '%s'", argName));
    }

    public static class SubstringAfterFunction implements Function<String, String>
    {
        private final String separator;

        private SubstringAfterFunction(String separator)
        {
            this.separator = separator;
        }

        @Nullable
        @Override
        public String apply(@Nullable String input)
        {
            return StringUtils.substringAfter(input, separator);
        }
    }

    public static class ParseLongFunction implements Function<String, Long>
    {
        @Nullable
        @Override
        public Long apply(@Nullable String input)
        {
            return Long.parseLong(input);
        }
    }
}
