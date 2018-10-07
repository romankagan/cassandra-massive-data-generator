package com.romankagan.dse.demos.solr.load;

import com.romankagan.bdp.shade.com.google.common.collect.Iterables;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class GeonamesReaderTest
{
    @Test
    public void shouldNotReadMoreLinesThanLimit() throws IOException
    {
        // given:
        File geonames = getClasspathResource("geonames.txt");

        // when:
        try (GeonamesLoader reader = new GeonamesLoader(geonames.toPath(), 10, ImmutableMap.of(), Suppliers.ofInstance(new Random(42))))
        {
            // then:
            assertEquals(10, reader.expectedLines());
            assertEquals(10, Iterables.size(reader));
        }
    }

    @Test
    public void shouldNotReadAnyLines() throws IOException
    {
        // given:
        File geonames = getClasspathResource("geonames.txt");

        // when:
        try (GeonamesLoader reader = new GeonamesLoader(geonames.toPath(), 0, ImmutableMap.of(), Suppliers.ofInstance(new Random(42))))
        {
            // then:
            assertEquals(0, reader.expectedLines());
            assertEquals(0, Iterables.size(reader));
        }
    }

    @Test
    public void shouldNotTryToReadMoreLinesThanFileContains() throws IOException
    {
        // given:
        File geonames = getClasspathResource("geonames.txt");

        // when:
        try (GeonamesLoader reader = new GeonamesLoader(geonames.toPath(), 100, ImmutableMap.of(), Suppliers.ofInstance(new Random(42))))
        {
            // then:
            assertEquals(50, reader.expectedLines());
            assertEquals(50, Iterables.size(reader));
        }
    }

    @Test
    public void shouldParseFirstLine() throws IOException
    {
        // given:
        File geonames = getClasspathResource("geonames.txt");

        // when:
        try (GeonamesLoader reader = new GeonamesLoader(geonames.toPath(), 1, ImmutableMap.of(), Suppliers.ofInstance(new Random(42))))
        {
            assertEquals(1, reader.expectedLines());
            Map<String, String> fields = Iterables.getFirst(reader.mapAdapter(), new HashMap<String, String>());

            // then:
            assertEquals("2986043", fields.get("id"));
            assertEquals("Pic de Font Blanca", fields.get("name"));
            assertEquals("42.64991", fields.get("latitude"));
            assertEquals("1.53335", fields.get("longitude"));
            assertEquals("AD", fields.get("country"));
            assertEquals("Europe/Andorra", fields.get("timezone"));
            assertEquals("2014-11-05", fields.get("published"));
        }
    }

    @Test
    public void shouldSynthesizePrimaryKey() throws Throwable
    {
        // given:
        File geonames = getClasspathResource("geonames.txt");

        Map options =
                ImmutableMap.of(GeonamesLoader.SYNTHESIZE_KEY_KEY, true,
                                GeonamesLoader.STARTING_KEY_KEY, 42);
        // when:
        try (GeonamesLoader reader = new GeonamesLoader(geonames.toPath(), 5, options, Suppliers.ofInstance(new Random(42))))
        {
            // then:
            assertEquals(5, reader.expectedLines());
            int counter = 43;
            for (Map<String, String> fields : reader.mapAdapter())
            {
                assertEquals(Integer.toString(counter++), fields.get("id"));
            }
        }
    }

    public static File getClasspathResource(String name)
    {
        try
        {
            return new File(Thread.currentThread().getContextClassLoader().getResource(name).toURI());
        }
        catch (URISyntaxException ex)
        {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
