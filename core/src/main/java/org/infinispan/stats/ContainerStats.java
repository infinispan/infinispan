package org.infinispan.stats;

import java.util.HashMap;
import java.util.Map;

public interface ContainerStats {

   String MEMORY_AVAILABLE = "memoryAvailable";
   String MEMORY_MAX = "memoryMax";
   String MEMORY_TOTAL = "memoryTotal";
   String MEMORY_USED = "memoryUsed";

   String[] LONG_ATTRIBUTES = {MEMORY_AVAILABLE, MEMORY_MAX, MEMORY_TOTAL, MEMORY_USED};

   static Map<String, Number> getLocalStatMaps() {
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

   /**
    * @return the maximum amount of free memory in bytes across the cluster JVMs.
    */
   long getMemoryAvailable();

   /**
    * @return the maximum amount of memory that JVMs across the cluster will attempt to utilise in bytes.
    */
   long getMemoryMax();

   /**
    * @return the total amount of memory in the JVMs across the cluster in bytes.
    */
   long getMemoryTotal();

   /**
    * @return the amount of memory used by JVMs across the cluster in bytes.
    */
   long getMemoryUsed();
}
