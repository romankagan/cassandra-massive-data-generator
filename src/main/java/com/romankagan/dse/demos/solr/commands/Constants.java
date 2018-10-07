package com.romankagan.dse.demos.solr.commands;

public interface Constants
{
    public static final String TEST_DATA_VALUES_DELIMITER = "|";

    public enum TestDataHeader
    {
        EXCLUSIVE_RANDOM("RANDOM"), EXCLUSIVE_SEQUENTIAL("SEQUENTIAL"), SHARED_SEQUENTIAL("SHARED_SEQUENTIAL");

        private final String name;

        TestDataHeader(String name)
        {
            this.name = name;
        }

        public static TestDataHeader byName(String name)
        {
            for (TestDataHeader testDataHeader : values())
            {
                if (testDataHeader.getName().equalsIgnoreCase(name))
                {
                    return testDataHeader;
                }
            }
            throw new IllegalStateException("Unknown test file header: " + name);
        }

        public String getName()
        {
            return name;
        }
    }
}
