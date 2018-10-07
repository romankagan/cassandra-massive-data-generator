package com.romankagan.dse.demos.solr;

import java.util.Random;

public final class IpsumGenerator
{

    private static final String[] words = new String[]{"lorem", "ipsum", "dolor", "sit", "amet", "consectetuer",
                                                       "adipiscing", "elit", "sed", "diam", "nonummy", "nibh",
                                                       "euismod", "tincidunt", "ut", "laoreet", "dolore",
                                                       "aliquam", "erat", "commodo", "ligula", "eget", "et",
                                                       "dolor", "massa", "sociis", "natoque", "penatibus",
                                                       "magnis", "dis", "parturient", "montes", "nascetur",
                                                       "ridiculus", "mus", "donec", "quam", "felis", "sem",
                                                       "nec", "pellentesque", "eu", "pretium", "quis",
                                                       "enim", "justo", "pede", "fringilla", "semper", "libero",
                                                       "vel", "aliquet", "vulputate", "arcu", "in",
                                                       "rhoncus", "imperdiet", "a", "venenatis",  "varius",
                                                       "nullam", "dictum", "mollis", "pretorium", "integer",
                                                       "cras", "dapibus", "vivamus", "elementum", "nisi",
                                                       "aenean", "eleifend", "tellus", "leo", "viverra", "metus",
                                                       "porttitor", "consequat", "vitae", "ac", "hendrerit",
                                                       "ante", "viverra_quis", "feugiat", "phasellus",
                                                       "quisque", "rutrum", "aenean", "etiam", "ultricies",
                                                       "augue", "curabitur", "ullacorper", "nam",
                                                       "dui", "maecenas", "tempus", "condimentum",
                                                       "neque", "blandit", "vel", "luctus", "pulvinar",
                                                       "id", "odio", "faucibus", "bibendum", "magna",
                                                       "sodales", "sagittis", "cursus", "nunc"};

   public static String generate(int numWords, Random random)
   {
      StringBuilder text = new StringBuilder();
      for (int i = 0; i < numWords; i++)
      {
          text.append(words[random.nextInt(words.length)]);
          text.append(" ");
      }
      return text.toString();
   }
}