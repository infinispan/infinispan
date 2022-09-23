package org.infinispan.metrics.impl;

import static org.infinispan.metrics.impl.BaseAdditionalMetrics.PREFIX;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.MeterBinder;

public class BaseMemoryAdditionalMetrics implements MeterBinder {

   @Override
   public void bindTo(MeterRegistry registry) {
      MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

      Gauge.builder(PREFIX + "memory.committedHeap", memoryBean, (mem) -> mem.getHeapMemoryUsage().getCommitted())
            .baseUnit(BaseUnits.BYTES)
            .description("Displays the amount of memory that is committed for the Java virtual machine to use.")
            .register(registry);

      Gauge.builder(PREFIX + "memory.maxHeap", memoryBean, (mem) -> mem.getHeapMemoryUsage().getMax())
            .baseUnit(BaseUnits.BYTES)
            .description("Displays the maximum amount of memory, in bytes, that can be used for memory management.")
            .register(registry);

      Gauge.builder(PREFIX + "memory.usedHeap", memoryBean, (mem) -> mem.getHeapMemoryUsage().getUsed())
            .baseUnit(BaseUnits.BYTES)
            .description("Displays the amount of used memory.")
            .register(registry);

      Gauge.builder(PREFIX + "memory.initHeap", memoryBean, (mem) -> mem.getHeapMemoryUsage().getInit())
            .baseUnit(BaseUnits.BYTES)
            .description("Displays the initial amount of allocated heap memory in bytes.")
            .register(registry);

      Gauge.builder(PREFIX + "memory.committedNonHeap", memoryBean, (mem) -> mem.getNonHeapMemoryUsage().getCommitted())
            .baseUnit(BaseUnits.BYTES)
            .description("Displays the amount of memory that is committed for the Java virtual machine to use.")
            .register(registry);

      Gauge.builder(PREFIX + "memory.maxNonHeap", memoryBean, (mem) -> mem.getNonHeapMemoryUsage().getMax())
            .baseUnit(BaseUnits.BYTES)
            .description("Displays the maximum amount of memory in bytes that can be used for memory management.")
            .register(registry);

      Gauge.builder(PREFIX + "memory.usedNonHeap", memoryBean, (mem) -> mem.getNonHeapMemoryUsage().getUsed())
            .baseUnit(BaseUnits.BYTES)
            .description("Displays the amount of used memory.")
            .register(registry);

      Gauge.builder(PREFIX + "memory.initNonHeap", memoryBean, (mem) -> mem.getNonHeapMemoryUsage().getInit())
            .baseUnit(BaseUnits.BYTES)
            .description("Displays the initial amount of allocated memory, in bytes, for off-heap storage.")
            .register(registry);
   }
}
