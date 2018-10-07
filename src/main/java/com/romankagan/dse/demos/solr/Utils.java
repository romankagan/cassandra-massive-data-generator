package com.romankagan.dse.demos.solr;

import java.nio.ByteBuffer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class Utils
{
    private static final Pattern randomPattern = Pattern.compile("\\$RANDOM_\\d+(:\\d+)?");
    private static final Pattern zipfPattern = Pattern.compile("\\$ZIPF_\\d+(:\\d+)?");
    private static final Pattern ipsumPattern = Pattern.compile("\\$IPSUM_\\d+(:\\d+)?");

    public static final Executor DIRECT_EXECUTOR = new Executor()
    {
        @Override
        public void execute(Runnable command)
        {
            command.run();
        }
    };

    public static long LINE_COUNT_ESTIMATION_THRESHOLD = 512 * 1024 * 1024;

    public static String runCmdNotationSubstitutions(String cmd, final Random random)
    {
        if (!cmd.contains("$"))
        {
            return cmd;
        }

        // First replace UUIDs
        byte[] randomBytes = new byte[16];
        random.nextBytes(randomBytes);
        randomBytes[6]  &= 0x0f;
        randomBytes[6]  |= 0x40;
        randomBytes[8]  &= 0x3f;
        randomBytes[8]  |= 0x80;
        ByteBuffer buf = ByteBuffer.wrap(randomBytes);
        long bits1 = buf.getLong();
        long bits2 = buf.getLong();
        cmd = cmd.replaceAll("\\$RANDOM_UUID", new UUID(bits1, bits2).toString());

        cmd = runVariableSubstitutions(cmd, randomPattern,
            new Randomizable() {
                @Override
                public String nextValue(int randValue)
                {
                    return Integer.toString(random.nextInt(randValue + 1));
                }
            });

        cmd = runVariableSubstitutions(cmd, zipfPattern,
            new Randomizable() {
                @Override
                public String nextValue(int randValue)
                {
                    ZipfGenerator zipfGenerator = ZipfGenerator.getInstance(randValue);
                    return Integer.toString(zipfGenerator.next());
                }
            });

        cmd = runVariableSubstitutions(cmd, ipsumPattern,
             new Randomizable(){
                 @Override
                 public String nextValue(int randValue)
                 {
                     return IpsumGenerator.generate(randValue, random);
                 }
             });

        return cmd;
    }

    private interface Randomizable {
        public String nextValue(int value);
    }

    private static String runVariableSubstitutions(String cmd, Pattern pattern, Randomizable randomizer)
    {
        String varName = pattern.toString().replace("\\$", "").replace("_\\d+(:\\d+)?", "");
        String varPrefix = "$" + varName + "_";

        // Now replace the varPrefix notation ('RANDOM', 'ZIPF', 'IPSUM).
        // RegExp are expensive so check first it is needed
        if (cmd.indexOf(varPrefix) != -1)
        {
            StringBuffer newCmd = new StringBuffer();
            Matcher matcher = pattern.matcher(cmd);
            while (matcher.find())
            {
                String parsedRandValue = matcher.group().toString().replaceAll("[$_]", "").replace(varName, "");

                // Parse repetition generation
                String[] values = parsedRandValue.split(":");
                int num = values.length == 1 ? 1 : Integer.parseInt(values[1]);
                String generatedRandValue = "";
                for (int i = 0; i < num; i++)
                {
                    generatedRandValue += randomizer.nextValue(Integer.parseInt(values[0])) + " ";
                }
                matcher.appendReplacement(newCmd, generatedRandValue.trim());
            }
            matcher.appendTail(newCmd);
            return newCmd.toString();
        }
        return cmd;
    }

    public static ExecutorService buildExecutor(String name, int threads, BlockingQueue<Runnable> queue)
    {
        //Block on submission if necessary
        RejectedExecutionHandler reh = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
            {
                try
                {
                    executor.getQueue().put(r);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
        return new ThreadPoolExecutor(threads,
                threads,
                Long.MAX_VALUE,
                TimeUnit.NANOSECONDS,
                queue,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "%d").build(),
                reh);
    }

    //Thanks fhucho http://stackoverflow.com/a/16937102/5914266
    /**
     * Approximate line count for a file. Doesn't count empty lines or comment only lines.
     */
    public static int countLinesApproximate(File file) throws IOException
    {
        int lines = 0;

        try (FileInputStream fis = new FileInputStream(file))
        {
            byte[] buffer = new byte[1024 * 8];
            int read;
            long processedBytes = 0;
            boolean inComment = false;
            boolean hadChars = false;
            while ((read = fis.read(buffer)) != -1)
            {
                for (int i = 0; i < read; i++)
                {
                    if (buffer[i] == '#')
                    {
                        inComment = true;
                    }

                    //If this is a newline and there was real character data
                    if (buffer[i] == '\n')
                    {
                        if (hadChars)
                        {
                            lines++;
                        }
                        inComment = false;
                        hadChars = false;
                    }
                    else if (!inComment & !Character.isWhitespace(buffer[i]))
                    {
                        hadChars = true;
                    }
                }
                processedBytes += read > 0 ? read : 0;
                if (processedBytes > LINE_COUNT_ESTIMATION_THRESHOLD)
                    break;
            }

            if (hadChars)
            {
                lines++;
            }

            /*
             * For a large file don't read the whole thing just estimate the number of lines
             */
            if (processedBytes < file.length())
            {
                lines = (int) (lines * (file.length() / (double) processedBytes));
            }

            fis.close();
        }

        return lines;
    }

}
