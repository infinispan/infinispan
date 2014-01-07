package org.infinispan.factories;

import java.util.Arrays;
import java.util.Collection;
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
   public static final String REMOTE_COMMAND_EXECUTOR = "org.infinispan.executors.remote";
   public static final String ASYNC_NOTIFICATION_EXECUTOR = "org.infinispan.executors.notification";
   public static final String PERSISTENCE_EXECUTOR = "org.infinispan.executors.persistence";
   public static final String EVICTION_SCHEDULED_EXECUTOR = "org.infinispan.executors.eviction";
   public static final String ASYNC_REPLICATION_QUEUE_EXECUTOR = "org.infinispan.executors.replicationQueue";
   public static final String MODULE_COMMAND_INITIALIZERS ="org.infinispan.modules.command.initializers";
   public static final String MODULE_COMMAND_FACTORIES ="org.infinispan.modules.command.factories";
   public static final String GLOBAL_MARSHALLER = "org.infinispan.marshaller.global";
   public static final String CACHE_MARSHALLER = "org.infinispan.marshaller.cache";
   public static final String CLASS_LOADER = "java.lang.ClassLoader";
   public static final String TOTAL_ORDER_EXECUTOR = "org.infinispan.executors.totalOrderExecutor";

   // Please make sure this is kept up to date
   public static final Collection<String> ALL_KNOWN_COMPONENT_NAMES = Arrays.asList(
      ASYNC_TRANSPORT_EXECUTOR, ASYNC_NOTIFICATION_EXECUTOR, PERSISTENCE_EXECUTOR, EVICTION_SCHEDULED_EXECUTOR, ASYNC_REPLICATION_QUEUE_EXECUTOR,
      MODULE_COMMAND_INITIALIZERS, MODULE_COMMAND_FACTORIES, GLOBAL_MARSHALLER, CACHE_MARSHALLER, CLASS_LOADER,
      REMOTE_COMMAND_EXECUTOR, TOTAL_ORDER_EXECUTOR
   );

   public static final Collection<String> PER_CACHE_COMPONENT_NAMES = Arrays.asList(CACHE_MARSHALLER);

   private static final Map<String, Integer> DEFAULT_THREADCOUNTS = new HashMap<String, Integer>(4);
   private static final Map<String, Integer> DEFAULT_QUEUE_SIZE = new HashMap<String, Integer>(4);
   private static final Map<String, Integer> DEFAULT_THREADPRIO = new HashMap<String, Integer>(6);

   static {
      DEFAULT_THREADCOUNTS.put(ASYNC_NOTIFICATION_EXECUTOR, 1);
      DEFAULT_THREADCOUNTS.put(ASYNC_TRANSPORT_EXECUTOR, 25);
      DEFAULT_THREADCOUNTS.put(ASYNC_REPLICATION_QUEUE_EXECUTOR, 1);
      DEFAULT_THREADCOUNTS.put(EVICTION_SCHEDULED_EXECUTOR, 1);
      DEFAULT_THREADCOUNTS.put(PERSISTENCE_EXECUTOR, 4);
      DEFAULT_THREADCOUNTS.put(REMOTE_COMMAND_EXECUTOR, 32);
      DEFAULT_THREADCOUNTS.put(TOTAL_ORDER_EXECUTOR, 32);

      DEFAULT_QUEUE_SIZE.put(ASYNC_NOTIFICATION_EXECUTOR, 100000);
      DEFAULT_QUEUE_SIZE.put(ASYNC_TRANSPORT_EXECUTOR, 100000);
      DEFAULT_QUEUE_SIZE.put(ASYNC_REPLICATION_QUEUE_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(EVICTION_SCHEDULED_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(PERSISTENCE_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(REMOTE_COMMAND_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(TOTAL_ORDER_EXECUTOR, 0);

      DEFAULT_THREADPRIO.put(ASYNC_NOTIFICATION_EXECUTOR, Thread.MIN_PRIORITY);
      DEFAULT_THREADPRIO.put(ASYNC_REPLICATION_QUEUE_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREADPRIO.put(ASYNC_TRANSPORT_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREADPRIO.put(EVICTION_SCHEDULED_EXECUTOR, Thread.MIN_PRIORITY);
      DEFAULT_THREADPRIO.put(PERSISTENCE_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREADPRIO.put(REMOTE_COMMAND_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREADPRIO.put(TOTAL_ORDER_EXECUTOR, Thread.NORM_PRIORITY);
   }

   public static int getDefaultThreads(String componentName) {
      return DEFAULT_THREADCOUNTS.get(componentName);
   }

   public static int getDefaultThreadPrio(String componentName) {
      return DEFAULT_THREADPRIO.get(componentName);
   }
   
   public static int getDefaultQueueSize(String componentName) {
      return DEFAULT_QUEUE_SIZE.get(componentName);
   }

   public static String shortened(String cn) {
      int dotIndex = cn.lastIndexOf(".");
      int dotIndexPlusOne = dotIndex + 1;
      String cname = cn;
      if (dotIndexPlusOne == cn.length())
         cname = shortened(cn.substring(0, cn.length() - 1));
      else {
         if (dotIndex > -1 && cn.length() > dotIndexPlusOne) {
            cname = cn.substring(dotIndexPlusOne);
         }
         cname += "-thread";
      }
      return cname;
   }

}
