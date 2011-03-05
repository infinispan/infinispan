package org.infinispan.factories;

import java.util.HashMap;
import java.util.Map;

/**
 * Holder for known named component names.  To be used with {@link org.infinispan.factories.annotations.ComponentName}
 * annotation.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class KnownComponentNames {
   public static final String ASYNC_TRANSPORT_EXECUTOR = "org.infinispan.executors.transport";
   public static final String ASYNC_NOTIFICATION_EXECUTOR = "org.infinispan.executors.notification";
   public static final String EVICTION_SCHEDULED_EXECUTOR = "org.infinispan.executors.eviction";
   public static final String ASYNC_REPLICATION_QUEUE_EXECUTOR = "org.infinispan.executors.replicationQueue";
   public static final String MODULE_COMMAND_INITIALIZERS ="org.infinispan.modules.command.initializers";
   public static final String MODULE_COMMAND_FACTORIES ="org.infinispan.modules.command.factories";

   private static final Map<String, Integer> DEFAULT_THREADCOUNTS = new HashMap<String, Integer>(2);
   private static final Map<String, Integer> DEFAULT_THREADPRIO = new HashMap<String, Integer>(4);

   static {
      DEFAULT_THREADCOUNTS.put(ASYNC_NOTIFICATION_EXECUTOR, 1);
      DEFAULT_THREADCOUNTS.put(ASYNC_TRANSPORT_EXECUTOR, 25);

      DEFAULT_THREADPRIO.put(ASYNC_NOTIFICATION_EXECUTOR, Thread.MIN_PRIORITY);
      DEFAULT_THREADPRIO.put(ASYNC_TRANSPORT_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREADPRIO.put(EVICTION_SCHEDULED_EXECUTOR, Thread.MIN_PRIORITY);
      DEFAULT_THREADPRIO.put(ASYNC_REPLICATION_QUEUE_EXECUTOR, Thread.NORM_PRIORITY);
   }

   public static int getDefaultThreads(String componentName) {
      return DEFAULT_THREADCOUNTS.get(componentName);
   }

   public static int getDefaultThreadPrio(String componentName) {
      return DEFAULT_THREADPRIO.get(componentName);
   }
}
