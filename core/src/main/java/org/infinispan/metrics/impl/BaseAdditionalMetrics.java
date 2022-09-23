package org.infinispan.metrics.impl;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.lang.NonNullApi;
import io.micrometer.core.lang.NonNullFields;

@NonNullApi
@NonNullFields
class BaseAdditionalMetrics implements MeterBinder {

   static final String PREFIX = "base.";

   @Override
   public void bindTo(MeterRegistry registry) {
      bindClassLoaderMetrics(registry);
      new BaseOperatingSystemAdditionalMetrics().bindTo(registry);
      bindGarbageCollectionMetrics(registry);
      bindRuntimeMetrics(registry);
      new BaseMemoryAdditionalMetrics().bindTo(registry);
      bindThreadingMetrics(registry);
   }

   private void bindClassLoaderMetrics(MeterRegistry registry) {
      ClassLoadingMXBean classLoadingBean = ManagementFactory.getClassLoadingMXBean();

      Gauge.builder(PREFIX + "classloader.loadedClasses.count", classLoadingBean, ClassLoadingMXBean::getLoadedClassCount)
            .description("Displays the number of classes that are currently loaded in the Java virtual machine.")
            .register(registry);

      FunctionCounter.builder(PREFIX + "classloader.loadedClasses.total", classLoadingBean, ClassLoadingMXBean::getTotalLoadedClassCount)
            .description("Displays the total number of classes that have been loaded since the Java virtual machine has started execution.")
            .register(registry);

      FunctionCounter.builder(PREFIX + "classloader.unloadedClasses.total", classLoadingBean, ClassLoadingMXBean::getUnloadedClassCount)
            .description("Displays the total number of classes unloaded since the Java virtual machine has started execution.")
            .register(registry);
   }

   private void bindGarbageCollectionMetrics(MeterRegistry registry) {
      for (GarbageCollectorMXBean garbageCollectorBean : ManagementFactory.getGarbageCollectorMXBeans()) {
         FunctionCounter.builder(PREFIX + "gc.total", garbageCollectorBean, GarbageCollectorMXBean::getCollectionCount)
               .tags(Tags.of("name", garbageCollectorBean.getName()))
               .description("Displays the total number of collections that have occurred. This attribute lists -1 if the collection count is undefined for this collector.")
               .register(registry);

         FunctionCounter.builder(PREFIX + "gc.time", garbageCollectorBean, GarbageCollectorMXBean::getCollectionTime)
               .tags(Tags.of("name", garbageCollectorBean.getName()))
               .description("Displays the approximate accumulated collection elapsed time in milliseconds. This attribute displays -1 if the collection elapsed time is undefined for this collector. The Java virtual machine implementation may use a high resolution timer to measure the elapsed time. This attribute might display the same value even if the collection count has been incremented if the collection elapsed time is very short.")
               .register(registry);
      }
   }

   private void bindRuntimeMetrics(MeterRegistry registry) {
      RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

      Gauge.builder(PREFIX + "jvm.uptime", runtimeBean, RuntimeMXBean::getUptime)
            .description("Displays the uptime of the Java virtual machine.")
            .register(registry);
   }

   private void bindThreadingMetrics(MeterRegistry registry) {
      ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

      Gauge.builder(PREFIX + "thread.count", threadBean, ThreadMXBean::getThreadCount)
            .description("Displays the current thread count.")
            .register(registry);

      Gauge.builder(PREFIX + "thread.daemon.count", threadBean, ThreadMXBean::getDaemonThreadCount)
            .description("Displays the current number of live daemon threads.")
            .register(registry);

      Gauge.builder(PREFIX + "thread.max.count", threadBean, ThreadMXBean::getPeakThreadCount)
            .description("Displays the peak live thread count since the Java virtual machine started or peak was reset. This includes daemon and non-daemon threads.")
            .register(registry);

      Gauge.builder(PREFIX + "thread.totalStarted", threadBean, ThreadMXBean::getTotalStartedThreadCount)
            .description("Displays the total number of started threads.")
            .register(registry);
   }
}
