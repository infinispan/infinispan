package org.infinispan.metrics.impl;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.List;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.lang.NonNullApi;
import io.micrometer.core.lang.NonNullFields;

@NonNullApi
@NonNullFields
class VendorAdditionalMetrics implements MeterBinder {

   static final String PREFIX = "vendor.";

   @Override
   public void bindTo(MeterRegistry registry) {
      List<BufferPoolMXBean> bufferPoolBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
      for (BufferPoolMXBean bufferPoolBean : bufferPoolBeans) {
         String name = bufferPoolBean.getName();
         // avoid illegal characters due to beans named for example: "mapped - 'non-volatile memory'"
         String metricName = normalizeMetricName(name);
         Gauge.builder(PREFIX + "BufferPool.used.memory." + metricName, bufferPoolBean, BufferPoolMXBean::getMemoryUsed)
               .baseUnit(BaseUnits.BYTES)
               .description("The memory used by the NIO pool:" + name)
               .register(registry);
      }

      List<MemoryPoolMXBean> memoryPoolBeans = ManagementFactory.getMemoryPoolMXBeans();
      for (MemoryPoolMXBean memoryPoolBean : memoryPoolBeans) {
         String name = memoryPoolBean.getName();
         // avoid illegal characters due to beans named for example: "CodeHeap 'non-nmethods'"
         String metricName = normalizeMetricName(name);

         Gauge.builder(PREFIX + "memoryPool." + metricName + ".usage", memoryPoolBean, (mem) -> mem.getUsage().getUsed())
               .baseUnit(BaseUnits.BYTES)
               .description("Current usage of the " + name + " memory pool")
               .register(registry);

         Gauge.builder(PREFIX + "memoryPool." + metricName + ".usage.max", memoryPoolBean, (mem) -> mem.getPeakUsage().getUsed())
               .baseUnit(BaseUnits.BYTES)
               .description("Peak usage of the " + name + " memory pool")
               .register(registry);
      }
   }

   private static String normalizeMetricName(String name) {
      return NameUtils
              // map all illegal characters to "_"
              .filterIllegalChars(name)
              // remove underscores at the start and end of the name
              .replaceAll("^_", "").replaceAll("_$", "");
   }
}
