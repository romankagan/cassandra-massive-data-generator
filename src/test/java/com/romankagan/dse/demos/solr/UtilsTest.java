package com.romankagan.dse.demos.solr;

import java.io.File;
import java.io.FileOutputStream;

import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest
{

    @Test
    public void testCountLinesApproximate() throws Throwable
    {
        Assert.assertEquals(0, count(""));
        Assert.assertEquals(1, count("foo"));
        Assert.assertEquals(1, count("foo\n"));
        Assert.assertEquals(1, count("fooo\n#bar"));
        Assert.assertEquals(1, count("foo\n    #bar"));
        Assert.assertEquals(2, count("foo\nf#bar"));
        Assert.assertEquals(2, count("foo\n#bar\nfoo"));

        StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < 10000; ii++)
        {
            sb.append("foo\n");
        }

        long original = Utils.LINE_COUNT_ESTIMATION_THRESHOLD;
        Utils.LINE_COUNT_ESTIMATION_THRESHOLD = 1024;
        try
        {
            Assert.assertEquals(10000, count(sb.toString()));
        }
        finally
        {
            Utils.LINE_COUNT_ESTIMATION_THRESHOLD = original;
        }
    }

    private int count(String string) throws Throwable
    {
        File f = File.createTempFile("foo", "bar");
        f.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(string.getBytes(Charsets.UTF_8));
        return Utils.countLinesApproximate(f);
    }
}
