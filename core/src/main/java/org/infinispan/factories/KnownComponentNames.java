package org.infinispan.factories;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
   public static final String EXPIRATION_SCHEDULED_EXECUTOR = "org.infinispan.executors.expiration";
   public static final String ASYNC_REPLICATION_QUEUE_EXECUTOR = "org.infinispan.executors.replicationQueue";
   public static final String MODULE_COMMAND_INITIALIZERS ="org.infinispan.modules.command.initializers";
   public static final String MODULE_COMMAND_FACTORIES ="org.infinispan.modules.command.factories";
   public static final String GLOBAL_MARSHALLER = "org.infinispan.marshaller.global";
   public static final String CACHE_MARSHALLER = "org.infinispan.marshaller.cache";
   public static final String CLASS_LOADER = "java.lang.ClassLoader";
   public static final String TOTAL_ORDER_EXECUTOR = "org.infinispan.executors.totalOrderExecutor";
   public static final String STATE_TRANSFER_EXECUTOR = "org.infinispan.executors.stateTransferExecutor";
   public static final String TRANSACTION_VERSION_GENERATOR = "org.infinispan.transaction.versionGenerator";
   public static final String ASYNC_OPERATIONS_EXECUTOR = "org.infinispan.executors.async";
   public static final String TIMEOUT_SCHEDULE_EXECUTOR = "org.infinispan.executors.timeout";

   // Please make sure this is kept up to date
   public static final Collection<String> ALL_KNOWN_COMPONENT_NAMES = Arrays.asList(
      ASYNC_TRANSPORT_EXECUTOR, ASYNC_NOTIFICATION_EXECUTOR, PERSISTENCE_EXECUTOR, ASYNC_OPERATIONS_EXECUTOR,
      EXPIRATION_SCHEDULED_EXECUTOR, ASYNC_REPLICATION_QUEUE_EXECUTOR,
      MODULE_COMMAND_INITIALIZERS, MODULE_COMMAND_FACTORIES, GLOBAL_MARSHALLER, CACHE_MARSHALLER, CLASS_LOADER,
      REMOTE_COMMAND_EXECUTOR, TOTAL_ORDER_EXECUTOR, STATE_TRANSFER_EXECUTOR, TRANSACTION_VERSION_GENERATOR,
      TIMEOUT_SCHEDULE_EXECUTOR
   );

   public static final Collection<String> PER_CACHE_COMPONENT_NAMES = Collections.singletonList(CACHE_MARSHALLER);

   private static final Map<String, Integer> DEFAULT_THREAD_COUNT = new HashMap<>(4);
   private static final Map<String, Integer> DEFAULT_QUEUE_SIZE = new HashMap<>(4);
   private static final Map<String, Integer> DEFAULT_THREAD_PRIORITY = new HashMap<>(6);

   static {
      DEFAULT_THREAD_COUNT.put(ASYNC_NOTIFICATION_EXECUTOR, 1);
      DEFAULT_THREAD_COUNT.put(ASYNC_TRANSPORT_EXECUTOR, 25);
      DEFAULT_THREAD_COUNT.put(ASYNC_REPLICATION_QUEUE_EXECUTOR, 1);
      DEFAULT_THREAD_COUNT.put(EXPIRATION_SCHEDULED_EXECUTOR, 1);
      DEFAULT_THREAD_COUNT.put(PERSISTENCE_EXECUTOR, 4);
      DEFAULT_THREAD_COUNT.put(REMOTE_COMMAND_EXECUTOR, 200);
      DEFAULT_THREAD_COUNT.put(TOTAL_ORDER_EXECUTOR, 32);
      DEFAULT_THREAD_COUNT.put(STATE_TRANSFER_EXECUTOR, 60);
      DEFAULT_THREAD_COUNT.put(ASYNC_OPERATIONS_EXECUTOR, 25);

      DEFAULT_QUEUE_SIZE.put(ASYNC_NOTIFICATION_EXECUTOR, 100000);
      DEFAULT_QUEUE_SIZE.put(ASYNC_TRANSPORT_EXECUTOR, 100000);
      DEFAULT_QUEUE_SIZE.put(ASYNC_REPLICATION_QUEUE_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(EXPIRATION_SCHEDULED_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(PERSISTENCE_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(REMOTE_COMMAND_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(TOTAL_ORDER_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(STATE_TRANSFER_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(ASYNC_OPERATIONS_EXECUTOR, 1000);

      DEFAULT_THREAD_PRIORITY.put(ASYNC_NOTIFICATION_EXECUTOR, Thread.MIN_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(ASYNC_REPLICATION_QUEUE_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(ASYNC_TRANSPORT_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(EXPIRATION_SCHEDULED_EXECUTOR, Thread.MIN_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(PERSISTENCE_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(REMOTE_COMMAND_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(TOTAL_ORDER_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(STATE_TRANSFER_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(ASYNC_OPERATIONS_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(TIMEOUT_SCHEDULE_EXECUTOR, Thread.NORM_PRIORITY);
   }

   public static int getDefaultThreads(String componentName) {
      return DEFAULT_THREAD_COUNT.get(componentName);
   }

   public static int getDefaultThreadPrio(String componentName) {
      return DEFAULT_THREAD_PRIORITY.get(componentName);
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
