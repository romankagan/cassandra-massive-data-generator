package com.romankagan.dse.demos.solr;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.util.concurrent.ThreadLocalRandom;

public class ZipfGenerator {

    //private final Random random;
    private final int size;
    private final double skew;
    private double bottom;
    private static final ConcurrentMap<Integer, ZipfGenerator> INSTANCES = new ConcurrentHashMap<>();

    private ZipfGenerator(int size, double skew)
    {
        //this.random = random;
        this.size = size;
        this.skew = skew;
        for (int i = 1; i < size; i++)
        {
            this.bottom += (1 / Math.pow(i, skew));
        }
    }

    public static ZipfGenerator getInstance(int value)
    {

        ZipfGenerator gen = new ZipfGenerator(value, 1.0); // 1.0 == high skew
        gen = INSTANCES.putIfAbsent(value, gen);
        return gen != null ? gen : INSTANCES.get(value);
    }

    public static void clear()
    {
        INSTANCES.clear();
    }

    public int next()
    {
        int rank;
        double frequency = 0;
        double dice;
        Random random = ThreadLocalRandom.current();
        do
        {
            rank = random.nextInt(size);
            frequency = (1.0d / Math.pow(rank, skew)) / bottom;
            dice = random.nextDouble();
        }
        while (!(dice < frequency));

        return rank + 1; // avoids zero as the best ranked
    }
}
