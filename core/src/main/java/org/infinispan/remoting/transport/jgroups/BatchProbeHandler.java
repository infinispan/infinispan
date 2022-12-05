package org.infinispan.remoting.transport.jgroups;

import java.util.HashMap;
import java.util.Map;

import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.util.AverageMinMax;

import net.jcip.annotations.GuardedBy;

/**
 * TODO! document this
 *
 * @since 15.0
 */
public class BatchProbeHandler implements DiagnosticsHandler.ProbeHandler {

   private static final int DEFAULT_MIN_BATCH;

   static {
      DEFAULT_MIN_BATCH = Integer.parseInt(SecurityActions.getSystemProperty("org.infinispan.jgroups-transport.min_batch_size", "10"));
   }

   private volatile int minBatchSize;
   @GuardedBy("sequentialBatches")
   private final AverageMinMax sequentialBatches = new AverageMinMax();
   @GuardedBy("parallelBatches")
   private final AverageMinMax parallelBatches = new AverageMinMax();
   @GuardedBy("orderedBatches")
   private final AverageMinMax orderedBatches = new AverageMinMax();

   public BatchProbeHandler() {
      minBatchSize = DEFAULT_MIN_BATCH;
   }

   public int getMinBatchSize() {
      return minBatchSize;
   }

   public void addParallelBatchSize(int size) {
      synchronized (parallelBatches) {
         parallelBatches.add(size);
      }
   }

   public void addSequentialBatchSize(int size) {
      synchronized (sequentialBatches) {
         sequentialBatches.add(size);
      }
   }

   public void addOrderedBatchSize(int size) {
      synchronized (orderedBatches) {
         orderedBatches.add(size);
      }
   }

   @Override
   public Map<String, String> handleProbe(String... keys) {
      Map<String, String> map = new HashMap<>();
      for (String key : keys) {
         if ("batch".equals(key)) {
            map.put("min_batch_size", String.format("%,d", minBatchSize));
            addOrderedBatchesToMap(map);
            addParallelBatchesToMap(map);
            addSequentialBatchesToMap(map);
         } else if (key.startsWith("batch-size")) {
            int index = key.indexOf("=");
            if (index > 0) {
               minBatchSize = Integer.parseInt(key.substring(index + 1));
            }
         } else if ("batch-reset".equals(key)) {
            synchronized (sequentialBatches) {
               sequentialBatches.clear();
            }
            synchronized (parallelBatches) {
               parallelBatches.clear();
            }
            synchronized (orderedBatches) {
               orderedBatches.clear();
            }
         }
      }
      return map;
   }

   @Override
   public String[] supportedKeys() {
      return new String[]{"batch", "batch-size", "batch-reset"};
   }

   private void addParallelBatchesToMap(Map<String, String> map) {
      synchronized (parallelBatches) {
         addAverageMinMaxBatchSizeToMap("parallel", parallelBatches, map);
      }
   }

   private void addSequentialBatchesToMap(Map<String, String> map) {
      synchronized (sequentialBatches) {
         addAverageMinMaxBatchSizeToMap("sequential", sequentialBatches, map);
      }
   }

   private void addOrderedBatchesToMap(Map<String, String> map) {
      synchronized (orderedBatches) {
         addAverageMinMaxBatchSizeToMap("ordered", orderedBatches, map);
      }
   }

   private static void addAverageMinMaxBatchSizeToMap(String type, AverageMinMax averageMinMax, Map<String, String> map) {
      String result = averageMinMax.count() == 0 ?
            "n/a" :
            String.format("%s (count=%,d)", averageMinMax, averageMinMax.count());
      map.put(type + "_batch_size", result);
   }
}
