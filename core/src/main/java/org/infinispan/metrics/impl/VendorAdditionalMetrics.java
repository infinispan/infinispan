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
         Gauge.builder(PREFIX + "BufferPool.used.memory." + name, bufferPoolBean, BufferPoolMXBean::getMemoryUsed)
               .baseUnit(BaseUnits.BYTES)
               .description("The memory used by the NIO pool:" + name)
               .register(registry);
      }

      List<MemoryPoolMXBean> memoryPoolBeans = ManagementFactory.getMemoryPoolMXBeans();
      for (MemoryPoolMXBean memoryPoolBean : memoryPoolBeans) {
         String name = memoryPoolBean.getName();
         Gauge.builder(PREFIX + "memoryPool." + name + ".usage", memoryPoolBean, (mem) -> mem.getUsage().getUsed())
               .baseUnit(BaseUnits.BYTES)
               .description("Current usage of the " + name + " memory pool")
               .register(registry);

         Gauge.builder(PREFIX + "memoryPool." + name + ".usage.max", memoryPoolBean, (mem) -> mem.getPeakUsage().getUsed())
               .baseUnit(BaseUnits.BYTES)
               .description("Peak usage of the " + name + " memory pool")
               .register(registry);
      }
   }
}
