package org.infinispan.demo.mapreduce;

import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;

public class WordCountMapper implements Mapper<String, String, String, Integer> {
   private static final long serialVersionUID = -5943370243108735560L;
   private static int chunks = 0, words = 0;

   @Override
   public void map(String key, String value, Collector<String, Integer> c) {
      chunks++;
      /*
       * Split on punctuation or whitespace, except for ' and - to catch contractions and hyphenated
       * words
       */
      for (String word : value.split("[\\p{Punct}\\s&&[^'-]]+")) {
         if (word != null) {
            String w = word.trim();
            if (w.length() > 0) {
               c.emit(word.toLowerCase(), 1);
               words++;
            }
         }
      }

      if (chunks % 1000 == 0)
         System.out.printf("Analyzed %s words in %s lines%n", words, chunks);
   }
}