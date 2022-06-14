package org.infinispan.stats.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.util.logging.Log;

/**
 * An abstract class to expose statistics of the local JVM. Concrete classes must override {@link #statistics()}
 * to return the complete list of statistics.
 *
 * @author José Bolina
 * @since 14.0
 */
public abstract class AbstractContainerStats extends AbstractClusterStats {

   // Memory
   protected static final String MEMORY_AVAILABLE = "memoryAvailable";
   protected static final String MEMORY_MAX = "memoryMax";
   protected static final String MEMORY_TOTAL = "memoryTotal";
   protected static final String MEMORY_USED = "memoryUsed";

   private static final String[] LONG_ATTRIBUTES = {MEMORY_AVAILABLE, MEMORY_MAX, MEMORY_TOTAL, MEMORY_USED};

   AbstractContainerStats(Log log) {
      super(log);
   }

   protected static Map<String, Number> getLocalStatMaps() {
      Map<String, Number> map = new HashMap<>();
      long available = Runtime.getRuntime().freeMemory();
      long total = Runtime.getRuntime().totalMemory();
      long max = Runtime.getRuntime().maxMemory();
      map.put(MEMORY_AVAILABLE, available);
      map.put(MEMORY_MAX, max);
      map.put(MEMORY_TOTAL, total);
      map.put(MEMORY_USED, total - available);
      return map;
   }

   protected abstract List<Map<String, Number>> statistics() throws Exception;

   @Override
   void updateStats() throws Exception {
      List<Map<String, Number>> memoryMap = statistics();
      for (String attr: LONG_ATTRIBUTES) {
         putLongAttributes(memoryMap, attr);
      }
   }
}
