package com.romankagan.dse.demos.solr.load;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public interface InputLoader extends Iterable<Object[]>, Closeable
{
    Iterable<Map<String, String>> mapAdapter();
    List<String> columns();
    long expectedLines() throws Exception;
    String pkeyColumnName();
    int pkeyColumnIndex();
}
