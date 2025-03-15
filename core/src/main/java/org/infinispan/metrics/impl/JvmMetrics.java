package org.infinispan.metrics.impl;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmCompilationMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadDeadlockMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

class JvmMetrics implements MeterBinder {

   private JvmGcMetrics gcMetrics = new JvmGcMetrics();
   private JvmHeapPressureMetrics heapPressureMetrics = new JvmHeapPressureMetrics();

   @Override
   public void bindTo(MeterRegistry registry) {
      new ClassLoaderMetrics().bindTo(registry);
      new FileDescriptorMetrics().bindTo(registry);
      new JvmCompilationMetrics().bindTo(registry);
      gcMetrics.bindTo(registry);
      heapPressureMetrics.bindTo(registry);
      new JvmInfoMetrics().bindTo(registry);
      new JvmMemoryMetrics().bindTo(registry);
      new JvmThreadDeadlockMetrics().bindTo(registry);
      new JvmThreadMetrics().bindTo(registry);
      new ProcessorMetrics().bindTo(registry);
      new UptimeMetrics().bindTo(registry);
   }

   public void close() {
      gcMetrics.close();
      heapPressureMetrics.close();
   }
}
