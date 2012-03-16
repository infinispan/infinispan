/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

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