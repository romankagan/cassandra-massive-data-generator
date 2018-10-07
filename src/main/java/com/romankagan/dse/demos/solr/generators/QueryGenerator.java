package com.romankagan.dse.demos.solr.generators;

import com.clearspring.analytics.util.Lists;
import com.romankagan.dse.demos.solr.commands.Constants;
import com.romankagan.dse.demos.solr.commands.HttpReadCmdRunner;
import com.romankagan.dse.demos.solr.commands.JavaDriverCmdRunner;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.client.solrj.response.TermsResponse.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class QueryGenerator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryGenerator.class);
    private static final int MAX_FIELDS_PER_BOOLEAN_QUERY = 5;
    private static final String[] UNARY_OPERATORS = {"+", "-"};
    private static final Joiner BOOLEAN_QUERY_JOINER = Joiner.on(" ").skipNulls();

    private final Map<QueryDecoratorType, Function<String, String>> queryDecorators;
    private final String solrUrl;

    public QueryGenerator(String contactHost, String indexName)
    {
        this.solrUrl = contactHost;
        this.queryDecorators = ImmutableMap.of(
                QueryDecoratorType.CQL, new SolrStressCQLQueryDecorator(indexName),
                QueryDecoratorType.HTTP, new SolrStressHttpReadQueryDecorator());
    }

    public List<String> generateQueries(Iterable<String> fields, int termsLimit, long termQueriesCount, long booleanQueriesCount, QueryDecoratorType queryDecoratorType)
    {
        try (TermsGenerator termsGenerator = new TermsGenerator(solrUrl))
        {
            Map<String, List<TermsResponse.Term>> fieldTerms = termsGenerator.loadTermsForFields(fields, termsLimit);
            Preconditions.checkArgument(hasAtLeastOneTerm(fieldTerms), "Non of the defined fields contains any term");
            return generateQueriesFromTerms(fieldTerms, termQueriesCount, booleanQueriesCount, queryDecoratorType);
        }
        catch (IOException e)
        {
            LOGGER.error("Could not close terms generator.");
        }
        return Collections.emptyList();
    }

    private List<String> generateQueriesFromTerms(Map<String, List<TermsResponse.Term>> fieldTerms, long termQueriesCount, long booleanQueriesCount, QueryDecoratorType type)
    {
        LOGGER.info("Generating queries...");
        List<String> solrQueries = Lists.newArrayList();

        solrQueries.addAll(generateTermQueries(fieldTerms, termQueriesCount));
        solrQueries.addAll(generateBooleanQueries(fieldTerms, booleanQueriesCount));

        return ImmutableList.copyOf(
                Iterables.transform(solrQueries, queryDecorators.get(type)));
    }

    private boolean hasAtLeastOneTerm(Map<String, List<Term>> fieldTerms)
    {
        return Iterables.any(fieldTerms.values(), new Predicate<List<Term>>()
        {
            @Override
            public boolean apply(List<Term> input)
            {
                return !input.isEmpty();
            }
        });
    }

    private List<String> generateTermQueries(Map<String, List<TermsResponse.Term>> fieldTerms, long queriesCount)
    {
        List<String> result = Lists.newArrayList();
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < queriesCount; ++i)
        {
            result.add(getRandomTermQuery(fieldTerms));
        }

        LOGGER.info("Generated {} term queries in {} ms", queriesCount, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
        return result;
    }

    private List<String> generateBooleanQueries(Map<String, List<TermsResponse.Term>> fieldTerms, long queriesCount)
    {
        List<String> result = Lists.newArrayList();
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < queriesCount; ++i)
        {
            result.add(getRandomBooleanQuery(fieldTerms, randomInt(1, MAX_FIELDS_PER_BOOLEAN_QUERY)));
        }

        LOGGER.info("Generated {} boolean queries in {} ms", queriesCount, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
        return result;
    }

    private String getRandomBooleanQuery(Map<String, List<TermsResponse.Term>> fieldTerms, int fieldCount)
    {
        List<String> booleanQueries = Lists.newArrayList();
        for (int i = 0; i < fieldCount; ++i)
        {
            booleanQueries.add(UNARY_OPERATORS[randomInt(UNARY_OPERATORS.length)] + getRandomTermQuery(fieldTerms));
        }
        return BOOLEAN_QUERY_JOINER.join(booleanQueries);
    }

    private String getRandomTermQuery(Map<String, List<TermsResponse.Term>> fieldTerms)
    {
        String field = getRandomField(fieldTerms);
        return field + ":" + getRandomTermForField(fieldTerms, field).getTerm();
    }

    private String getRandomField(Map<String, List<TermsResponse.Term>> fieldTerms)
    {
        return Iterables.get(fieldTerms.keySet(), randomInt(fieldTerms.size()));
    }

    private Term getRandomTermForField(Map<String, List<TermsResponse.Term>> fieldTerms, String field)
    {
        List<Term> terms = fieldTerms.get(field);
        return terms.get(randomInt(terms.size()));
    }

    private int randomInt(int upperBound)
    {
        return ThreadLocalRandom.current().nextInt(upperBound);
    }

    private int randomInt(int lowerBound, int upperBound)
    {
        return ThreadLocalRandom.current().nextInt(lowerBound, upperBound);
    }

    private static class SolrStressHttpReadQueryDecorator implements Function<String, String>
    {
        private static final String HTTP_READ_QUERY_FORMAT = "q=%s&rows=1";

        @Nullable
        @Override
        public String apply(String input)
        {
            return HttpReadCmdRunner.CMD_STRING_ID +
                    Constants.TEST_DATA_VALUES_DELIMITER +
                    String.format(HTTP_READ_QUERY_FORMAT, input);
        }
    }

    private static class SolrStressCQLQueryDecorator implements Function<String, String>
    {
        private static final String CQL_QUERY_FORMAT = "SELECT * FROM %s WHERE solr_query='%s' LIMIT 1";

        private final String indexName;

        private SolrStressCQLQueryDecorator(String indexName)
        {
            this.indexName = indexName;
        }

        @Nullable
        @Override
        public String apply(String input)
        {
            return JavaDriverCmdRunner.CMD_STRING_ID +
                    Constants.TEST_DATA_VALUES_DELIMITER +
                    String.format(CQL_QUERY_FORMAT, indexName, StringUtils.replace(input, "'", "''"));
        }
    }
}
