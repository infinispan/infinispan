package org.infinispan.metrics.impl;

import static org.infinispan.metrics.impl.BaseAdditionalMetrics.PREFIX;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.lang.NonNullApi;
import io.micrometer.core.lang.NonNullFields;

@NonNullApi
@NonNullFields
/**
 * Provides process metrics.
 * <p>
 * Inspired by io.micrometer.core.instrument.binder.system.ProcessorMetrics
 */
public class BaseOperatingSystemAdditionalMetrics implements MeterBinder {

   private static final Log log = LogFactory.getLog(BaseOperatingSystemAdditionalMetrics.class);

   @Override
   public void bindTo(MeterRegistry registry) {
      OperatingSystemMXBean operatingSystemBean = ManagementFactory.getOperatingSystemMXBean();
      Gauge.builder(PREFIX + "cpu.availableProcessors", operatingSystemBean, OperatingSystemMXBean::getAvailableProcessors)
            .description("Displays the number of processors available to the Java virtual machine. This value may change during a particular invocation of the virtual machine.")
            .register(registry);

      Gauge.builder(PREFIX + "cpu.systemLoadAverage", operatingSystemBean, OperatingSystemMXBean::getSystemLoadAverage)
            .description("Displays the system load average for the last minute. The system load average is the sum of the number of runnable entities queued to the available processors and the number of runnable entities running on the available processors averaged over a period of time. The way in which the load average is calculated is operating system specific but is typically a damped time-dependent average. If the load average is not available, a negative value is displayed. This attribute is designed to provide a hint about the system load and may be queried frequently. The load average might be unavailable on some platforms where it is expensive to implement this method.")
            .register(registry);

      Method processCpuLoadMethod = getProcessCpuLoad(operatingSystemBean);
      if (processCpuLoadMethod != null) {
         Gauge.builder(PREFIX + "cpu.processCpuLoad", () -> invoke(operatingSystemBean, processCpuLoadMethod))
               .description("Displays the \"recent cpu usage\" for the Java virtual machine process.")
               .register(registry);
      }

      Method processCpuTimeMethod = getProcessCpuTime(operatingSystemBean);
      if (processCpuTimeMethod != null) {
         Gauge.builder(PREFIX + "cpu.processCpuTime", () -> invoke(operatingSystemBean, processCpuTimeMethod))
               .description("Displays the CPU time, in nanoseconds, used by the process on which the Java virtual machine is running.")
               .register(registry);
      }
   }

   public Json cpuReport() {
      OperatingSystemMXBean operatingSystemBean = ManagementFactory.getOperatingSystemMXBean();
      Json report = Json.object("system-load-average", operatingSystemBean.getSystemLoadAverage());

      Method processCpuLoadMethod = getProcessCpuLoad(operatingSystemBean);
      if (processCpuLoadMethod != null) {
         report.set("process-cpu-load", invoke(operatingSystemBean, processCpuLoadMethod));
      }

      Method processCpuTimeMethod = getProcessCpuTime(operatingSystemBean);
      if (processCpuTimeMethod != null) {
         report.set("process-cpu-time", invoke(operatingSystemBean, processCpuTimeMethod));
      }

      return report;
   }

   private static Method getProcessCpuLoad(OperatingSystemMXBean operatingSystemBean) {
      return detectMethod(operatingSystemBean, getOperatingSystemMXBeanImpl(), "getProcessCpuLoad");
   }

   private static Method getProcessCpuTime(OperatingSystemMXBean operatingSystemBean) {
      return detectMethod(operatingSystemBean, getOperatingSystemMXBeanImpl(), "getProcessCpuTime");
   }

   private static Class<?> getOperatingSystemMXBeanImpl() {
      List<String> classNames = Arrays.asList(
            "com.ibm.lang.management.OperatingSystemMXBean", // J9
            "com.sun.management.OperatingSystemMXBean" // HotSpot
      );

      for (String className : classNames) {
         try {
            return Class.forName(className);
         } catch (ClassNotFoundException ignore) {
         }
      }
      return null;
   }

   private static Method detectMethod(OperatingSystemMXBean operatingSystemBean, Class<?> operatingSystemBeanClass, String name) {
      if (operatingSystemBeanClass == null) {
         return null;
      }
      try {
         // ensure the Bean we have is actually an instance of the interface
         operatingSystemBeanClass.cast(operatingSystemBean);
         return operatingSystemBeanClass.getDeclaredMethod(name);
      } catch (ClassCastException | NoSuchMethodException | SecurityException e) {
         return null;
      }
   }

   private double invoke(OperatingSystemMXBean operatingSystemBean, Method method) {
      try {
         Object returnedValue = method.invoke(operatingSystemBean);
         if (returnedValue instanceof Long) {
            return ((Long) returnedValue).doubleValue();
         }
         return (double) returnedValue;
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
         log.warn("An error occurred while invoking method %1$1.", method, e);
         return -1;
      }
   }
}
