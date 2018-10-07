package com.romankagan.dse.demos.solr.load;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.romankagan.dse.demos.solr.Utils;

public class GeonamesLoader implements InputLoader
{
    static final String SYNTHESIZE_KEY_KEY = "synthesize_primary_key";
    static final String STARTING_KEY_KEY = "starting_primary_key";
    static final String SYNTHESIZE_RANDOM_KEY_KEY = "synthesize_random_key";

    public static final AtomicLong primaryKeyCounter = new AtomicLong();
    private final Path path;
    private BufferedReader reader;
    private final long limit;
    private final boolean synthesizePrimaryKey;
    private final boolean synthesizeRandomKey;
    private final GeonamesLineParser parser;
    private final Supplier<Random> r;

    public GeonamesLoader(Path geonames, long limit, Map options, Supplier<Random> r)
    {
        this.path = geonames;
        synthesizePrimaryKey = (Boolean)getWithDefault(options, SYNTHESIZE_KEY_KEY, false);
        synthesizeRandomKey = (Boolean)getWithDefault(options, SYNTHESIZE_RANDOM_KEY_KEY, true);
        parser = new GeonamesLineParser(synthesizePrimaryKey, synthesizeRandomKey, r);
        primaryKeyCounter.set(((Number)getWithDefault(options, STARTING_KEY_KEY, 0L)).longValue());
        this.r = r;
        try
        {
            buildReader();
            this.limit = limit;
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Iterator<Object[]> iterator()
    {
        return new GeonamesIterator(limit);
    }

    @Override
    public void close()
    {
        IOUtils.closeQuietly(reader);
    }

    public Iterable<Map<String, String>> mapAdapter()
    {
        return new Iterable<Map<String, String>>() {
            @Override
            public Iterator<Map<String, String>> iterator()
            {
                return new GeonamesMapIterator();
            }
        };
    }

    public List<String> columns()
    {
        return ImmutableList.of("id", "name", "latitude", "longitude", "country", "timezone", "published");
    }

    @Override
    public long expectedLines() throws IOException
    {
        if (synthesizePrimaryKey)
        {
            return limit;
        }
        else
        {
            return Math.min(limit, Utils.countLinesApproximate(path.toFile()));
        }
    }

    @Override
    public String pkeyColumnName()
    {
        return "id";
    }

    @Override
    public int pkeyColumnIndex()
    {
        return 0;
    }

    private final class GeonamesMapIterator implements Iterator<Map<String, String>>
    {
        private final Iterator<Object[]> i = iterator();

        @Override
        public boolean hasNext()
        {
            return i.hasNext();
        }

        @Override
        public Map<String, String> next()
        {
            Object fields[] = i.next();
            return parser.toMap(fields);
        }

        @Override
        public void remove()
        {
            // no-op
        }
    }

    private final class GeonamesIterator implements Iterator<Object[]>
    {
        private long iteratorLimit;

        private GeonamesIterator(long limit)
        {
            iteratorLimit = limit;
        }

        @Override
        public boolean hasNext()
        {
            try
            {
                if (synthesizePrimaryKey)
                {
                    return iteratorLimit > 0;
                }
                else
                {
                    return iteratorLimit > 0 && reader.ready();
                }
            }
            catch (IOException e)
            {
                return false;
            }
        }

        @Override
        public Object[] next()
        {
            try
            {
                String line = reader.readLine();
                if (line == null)
                {
                    if (!synthesizePrimaryKey)
                    {
                        throw new AssertionError("Shouldn't attempt to read more lines than available");
                    }
                    buildReader();
                    line = reader.readLine();
                }
                Object[] fields = parser.parseLine(line);
                iteratorLimit--;
                return fields;
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void remove()
        {
            // no-op
        }
    }

    private static Object getWithDefault(Map map, Object key, Object defaultValue)
    {
        Object value = map.get(key);
        if (value == null)
        {
            value = defaultValue;
        }
        return value;
    }

    private void buildReader() throws IOException
    {
        if (reader != null)
        {
            reader.close();
        }
        reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    public static class GeonamesLineParser
    {
        private final boolean synthesizePrimaryKey;
        private final boolean synthesizeRandomKey;
        private final Supplier<Random> r;

        public GeonamesLineParser(boolean synthesizePrimaryKey, boolean synthesizeRandomKey, Supplier<Random> r)
        {
            this.synthesizePrimaryKey = synthesizePrimaryKey;
            this.synthesizeRandomKey = synthesizeRandomKey;
            this.r = r;
        }

        public Object[] parseLine(String line)
        {
            Object[] parsedLine = new Object[7];
            int objectIndex = 0;
            int delimeterIndex = 0;
            int delimeterCount = -1;
            while (objectIndex < 7)
            {
                int index = line.indexOf('\t', delimeterIndex + 1);
                delimeterCount++;
                switch (delimeterCount)
                {
                    case 0:
                        if (synthesizePrimaryKey)
                        {
                            if (synthesizeRandomKey)
                            {
                                parsedLine[objectIndex] = String.valueOf(Math.abs(r.get().nextLong()));
                            }
                            else
                            {
                                parsedLine[objectIndex] = String.valueOf(primaryKeyCounter.incrementAndGet());
                            }
                        }
                        else
                        {
                            parsedLine[objectIndex] = line.substring(delimeterIndex, index);
                        }
                        objectIndex++;
                        break;
                    case 1:
                    case 4:
                    case 5:
                    case 8:
                    case 17:
                        parsedLine[objectIndex] = line.substring(delimeterIndex + 1, index);
                        objectIndex++;
                        break;
                    case 18:
                        parsedLine[objectIndex] = line.substring(delimeterIndex + 1, line.length());
                        objectIndex++;
                        break;
                    default:
                }
                delimeterIndex = index;
            }
            return parsedLine;
        }

        public Map<String, String> toMap(Object[] fields)
        {
            Map<String, String> parsedLine = new HashMap<String, String>(7);
            parsedLine.put("id", (String)fields[0]);
            parsedLine.put("name", (String)fields[1]);
            parsedLine.put("latitude", (String)fields[2]);
            parsedLine.put("longitude", (String)fields[3]);
            parsedLine.put("country", (String)fields[4]);
            parsedLine.put("timezone", (String)fields[5]);
            parsedLine.put("published", (String)fields[6]);
            return parsedLine;
        }
    }
}
