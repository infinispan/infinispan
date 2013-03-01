package org.infinispan.scripting;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

public class EntryComparator implements Comparator<Map.Entry<String, Double>> {

   @Override
   public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
      return o1.getValue() < o2.getValue() ? 1 : o1.getValue() > o2.getValue() ? -1 : 0;
   }

}
