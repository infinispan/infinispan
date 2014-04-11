package org.infinispan.demo.mapreduce;

import org.infinispan.distexec.mapreduce.Collator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Collates reduced results by grouping them into the top K most frequent words.
 *
 * @author Vladimir Blagojevic
 */
public class WordCountCollator implements Collator<String, Integer, List<Map.Entry<String, Integer>>> {

   private final int kthFrequentWord;

   public WordCountCollator() {
      this.kthFrequentWord = 10;
   }

   public WordCountCollator(int kthFrequentWord) {
      if (kthFrequentWord < 0)
         throw new IllegalArgumentException("kth FrequentWord can not be less than 0");
      this.kthFrequentWord = kthFrequentWord;
   }

   @Override
   public List<Map.Entry<String, Integer>> collate(Map<String, Integer> reducedResults) {
      Set<Map.Entry<String, Integer>> entrySet = reducedResults.entrySet();
      ArrayList<Map.Entry<String, Integer>> l = new ArrayList<Map.Entry<String, Integer>>(entrySet);
      Collections.sort(l, new Comparator<Map.Entry<String, Integer>>() {

         @Override
         public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return o1.getValue() < o2.getValue() ? 1 : o1.getValue() > o2.getValue() ? -1 : 0;
         }
      });

      List<Map.Entry<String, Integer>> results = new LinkedList<Map.Entry<String, Integer>>();
      for (int i=0; i<kthFrequentWord; i++) results.add(l.get(i));

      return results;
   }
}