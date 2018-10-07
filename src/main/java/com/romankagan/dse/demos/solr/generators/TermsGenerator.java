package com.romankagan.dse.demos.solr.generators;

import com.romankagan.bdp.shade.com.google.common.collect.ImmutableList;
import com.romankagan.bdp.shade.com.google.common.collect.ImmutableMap;
import com.romankagan.driver.core.ConsistencyLevel;
import com.romankagan.dse.demos.solr.clients.Clients;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.client.solrj.response.TermsResponse.Term;
import org.apache.solr.common.params.ShardParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TermsGenerator implements Closeable
{
    private static final int DEFAULT_TERMS_PER_FIELD_LIMIT = 250;
    private static final Logger LOGGER = LoggerFactory.getLogger(TermsGenerator.class);
    private final SolrClient sorlClient;

    public TermsGenerator(String solrUrl)
    {
        this.sorlClient = Clients.getHttpSolrClient(solrUrl, ConsistencyLevel.LOCAL_ONE);
    }

    public Map<String, List<Term>> loadTermsForFields(Iterable<String> fields, int termPerFieldLimit) throws IOException
    {
        ImmutableMap.Builder<String, List<Term>> fieldTermsBuilder = ImmutableMap.builder();
        for (String field : fields)
        {
            List<Term> terms = loadTerms(field, termPerFieldLimit);
            if (!terms.isEmpty())
            {
                fieldTermsBuilder.put(field, terms);
            }
            else
            {
                LOGGER.info("Skipping field '{}' - no terms were returned");
            }
        }
        return fieldTermsBuilder.build();

    }

    public Map<String, List<Term>> loadTermsForFields(Iterable<String> fields) throws IOException
    {
        return loadTermsForFields(fields, DEFAULT_TERMS_PER_FIELD_LIMIT);
    }

    private List<Term> loadTerms(String field, int limit) throws IOException
    {
        SolrQuery query = new SolrQuery();
        query.set(ShardParams.SHARDS_QT, "/terms");
        query.add("terms", "true");
        query.add("terms.fl", field);
        query.add("terms.limit", limit + "");
        query.setRequestHandler("/terms");

        try
        {
            Stopwatch stopwatch = Stopwatch.createStarted();
            QueryResponse queryResponse = sorlClient.query(query);
            List<Term> terms = getTermsFromResponse(queryResponse, field);
            LOGGER.info("Retrieved {} (limit = {}) terms for a field '{}' in {} ms",
                    terms.size(), limit, field, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
            return terms;
        }
        catch (SolrServerException e)
        {
            LOGGER.error("Could not retrieve terms for field {}", field);
            // fail fast
            Throwables.propagate(e);
        }
        return ImmutableList.of();
    }

    private List<Term> getTermsFromResponse(QueryResponse queryResponse, String field)
    {
        TermsResponse termsResponse = queryResponse.getTermsResponse();
        if (termsResponse == null)
        {
            return ImmutableList.of();
        }

        List<Term> terms = termsResponse.getTerms(field);
        if (terms == null)
        {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(terms);
    }

    @Override
    public void close() throws IOException
    {
        sorlClient.close();
    }
}
