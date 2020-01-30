package org.infinispan.factories;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.util.ProcessorInfo;

/**
 * Holder for known named component names.  To be used with {@link org.infinispan.factories.annotations.ComponentName}
 * annotation.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class KnownComponentNames {
   public static final String CACHE_NAME = "cacheName";

   // This is to be removed as we isolate parts of commands that make them block
   public static final String REMOTE_COMMAND_EXECUTOR = "org.infinispan.executors.remote";
   // BLOCKING - invokes user code! (designed to be single thread) TODO: should this be on PERSISTENCE thread instead?
   public static final String ASYNC_NOTIFICATION_EXECUTOR = "org.infinispan.executors.notification";
   // BLOCKING - invoke all blocking ops here
   public static final String PERSISTENCE_EXECUTOR = "org.infinispan.executors.persistence";
   // NON BLOCKING
   public static final String ASYNC_OPERATIONS_EXECUTOR = "org.infinispan.executors.async";

   // Consolidate scheduled thread pools together?
   public static final String EXPIRATION_SCHEDULED_EXECUTOR = "org.infinispan.executors.expiration";
   // Consolidate scheduled thread pools together?
   public static final String TIMEOUT_SCHEDULE_EXECUTOR = "org.infinispan.executors.timeout";


   public static final String MODULE_COMMAND_INITIALIZERS ="org.infinispan.modules.command.initializers";
   public static final String MODULE_COMMAND_FACTORIES ="org.infinispan.modules.command.factories";
   public static final String CLASS_LOADER = "java.lang.ClassLoader";
   public static final String TRANSACTION_VERSION_GENERATOR = "org.infinispan.transaction.versionGenerator";
   public static final String CACHE_DEPENDENCY_GRAPH = "org.infinispan.CacheDependencyGraph";
   public static final String INTERNAL_MARSHALLER = "org.infinispan.marshaller.internal";
   public static final String PERSISTENCE_MARSHALLER = "org.infinispan.marshaller.persistence";

   private static final Map<String, Integer> DEFAULT_THREAD_COUNT = new HashMap<>(7);
   private static final Map<String, Integer> DEFAULT_QUEUE_SIZE = new HashMap<>(7);
   private static final Map<String, Integer> DEFAULT_THREAD_PRIORITY = new HashMap<>(8);

   static {
      DEFAULT_THREAD_COUNT.put(ASYNC_NOTIFICATION_EXECUTOR, 1);
      DEFAULT_THREAD_COUNT.put(EXPIRATION_SCHEDULED_EXECUTOR, 1);
      // Persistence Executor default to # of CPUs
      DEFAULT_THREAD_COUNT.put(PERSISTENCE_EXECUTOR, ProcessorInfo.availableProcessors() * 4);
      DEFAULT_THREAD_COUNT.put(REMOTE_COMMAND_EXECUTOR, 200);
      DEFAULT_THREAD_COUNT.put(ASYNC_OPERATIONS_EXECUTOR, 25);

      DEFAULT_QUEUE_SIZE.put(ASYNC_NOTIFICATION_EXECUTOR, 1_000);
      DEFAULT_QUEUE_SIZE.put(EXPIRATION_SCHEDULED_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(PERSISTENCE_EXECUTOR, 5_000);
      DEFAULT_QUEUE_SIZE.put(REMOTE_COMMAND_EXECUTOR, 0);
      DEFAULT_QUEUE_SIZE.put(ASYNC_OPERATIONS_EXECUTOR, 1_000);

      DEFAULT_THREAD_PRIORITY.put(ASYNC_NOTIFICATION_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(EXPIRATION_SCHEDULED_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(PERSISTENCE_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREAD_PRIORITY.put(REMOTE_COMMAND_EXECUTOR, Thread.NORM_PRIORITY);
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

   public static int getDefaultMinThreads(String componentName) {
      if (getDefaultQueueSize(componentName) == 0) {
         return 1;
      } else {
         return getDefaultThreads(componentName);
      }
   }

   public static long getDefaultKeepaliveMillis() {
      return BlockingThreadPoolExecutorFactory.DEFAULT_KEEP_ALIVE_MILLIS;
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
