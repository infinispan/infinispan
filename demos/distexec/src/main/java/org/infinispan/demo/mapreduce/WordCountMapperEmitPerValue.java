package org.infinispan.demo.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;

public class WordCountMapperEmitPerValue implements Mapper<String, String, String, Integer> {
   private static final long serialVersionUID = -5943370243108735560L;
   private static int values = 0, words = 0;

   @Override
   public void map(String key, String value, Collector<String, Integer> c) {
      HashMap<String, Integer> results = new HashMap<String, Integer>();
      values++;
      /*
       * Split on punctuation or whitespace, except for ' and - to catch contractions and hyphenated
       * words
       */
      for (String word : value.split("[\\p{Punct}\\s&&[^'-]]+")) {
         if (word.length() > 0) {
            if (results.containsKey(word)) {
               results.put(word, results.get(word) + 1);
            } else {
               results.put(word, 1);
               words++;
            }
         }
      }

      for (Map.Entry<String, Integer> entry : results.entrySet()) {
         c.emit(entry.getKey().toLowerCase(), entry.getValue());
      }

      if (values % 1000 == 0)
         System.out.printf("Analyzed %s words in %s lines%n", words, values);
   }
}
