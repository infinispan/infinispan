package org.infinispan.remoting.transport.jgroups;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.executors.LazyInitializingBlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;
import org.jgroups.stack.DiagnosticsHandler;

/**
 * A probe handler for {@link org.jgroups.tests.Probe} protocol is JGroups.
 * <p>
 * It contains a single key and returns the information about the remote thread pool executor service.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
class ThreadPoolProbeHandler implements DiagnosticsHandler.ProbeHandler {

   private static final String KEY = "ispn-remote";

   private volatile ExecutorService executor;

   @Override
   public Map<String, String> handleProbe(String... keys) {
      if (keys == null || keys.length == 0) {
         return null;
      }
      ThreadPoolExecutor exec = extract(executor);
      if (exec == null || !Arrays.asList(keys).contains(KEY)) {
         return null;
      }
      return Map.of(
            "active-thread", String.valueOf(exec.getActiveCount()),
            "min-thread", String.valueOf(exec.getCorePoolSize()),
            "max-thread", String.valueOf(exec.getMaximumPoolSize()),
            "current-pool-size", String.valueOf(exec.getPoolSize()),
            "largest-pool-size", String.valueOf(exec.getLargestPoolSize()),
            "keep-alive", String.valueOf(exec.getKeepAliveTime(TimeUnit.MILLISECONDS)),
            "queue-size", String.valueOf(exec.getQueue().size()));
   }

   @Override
   public String[] supportedKeys() {
      return new String[]{KEY};
   }

   void updateThreadPool(ExecutorService executorService) {
      if (executorService != null) {
         this.executor = executorService;
      }
   }

   private static ThreadPoolExecutor extract(ExecutorService service) {
      if (service instanceof ThreadPoolExecutor) {
         return (ThreadPoolExecutor) service;
      } else if (service instanceof BlockingTaskAwareExecutorServiceImpl) {
         return extract(((BlockingTaskAwareExecutorServiceImpl) service).getExecutorService());
      } else if (service instanceof LazyInitializingBlockingTaskAwareExecutorService) {
         return extract(((LazyInitializingBlockingTaskAwareExecutorService) service).getExecutorService());
      }
      return null; //we don't know how to handle the remaining cases.
   }

}
