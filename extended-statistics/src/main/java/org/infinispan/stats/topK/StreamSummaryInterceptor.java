package org.infinispan.stats.topK;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.stats.wrappers.TopKeyLockManager;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Intercepts the VisitableCommands to calculate the corresponding top-key values.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@MBean(objectName = "StreamLibStatistics", description = "Show analytics for workload monitor")
public class StreamSummaryInterceptor extends BaseCustomInterceptor {

   private static final Log log = LogFactory.getLog(StreamSummaryInterceptor.class);
   private StreamLibContainer streamLibContainer;
   private DistributionManager distributionManager;

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {

      if (streamLibContainer.isEnabled() && ctx.isOriginLocal()) {
         streamLibContainer.addGet(command.getKey(), isRemote(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         if (streamLibContainer.isEnabled() && ctx.isOriginLocal()) {
            streamLibContainer.addPut(command.getKey(), isRemote(command.getKey()));
         }
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (streamLibContainer.isEnabled() && key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (streamLibContainer.isEnabled() && key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @ManagedOperation(description = "Resets statistics gathered by this component",
                     displayName = "Reset Statistics (Statistics)")
   public void resetStatistics() {
      streamLibContainer.resetAll();
   }

   @ManagedOperation(description = "Set K for the top-K values",
                     displayName = "Set K")
   public void setTopKValue(@Parameter(name = "n", description = "the n-th top key to collect") int n) {
      streamLibContainer.setCapacity(n);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most read remotely by this instance",
                     displayName = "Top Remote Read Keys")
   public Map<String, Long> getRemoteTopGets() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET));
   }

   @ManagedOperation(description = "Show the top n keys most read remotely by this instance",
                     displayName = "Nth Top Remote Read Keys")
   public Map<String, Long> getNRemoteTopGets(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.REMOTE_GET);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most read locally by this instance",
                     displayName = "Top Local Read Keys")
   public Map<String, Long> getLocalTopGets() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET));
   }

   @ManagedOperation(description = "Show the top n keys most read locally by this instance",
                     displayName = "Nth Top Local Read Keys")
   public Map<String, Long> getNLocalTopGets(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.LOCAL_GET);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most write remotely by this instance",
                     displayName = "Top Remote Write Keys")
   public Map<String, Long> getRemoteTopPuts() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_PUT));
   }

   @ManagedOperation(description = "Show the top n keys most write remotely by this instance",
                     displayName = "Nth Top Remote Write Keys")
   public Map<String, Long> getNRemoteTopPuts(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_PUT, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.REMOTE_PUT);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most write locally by this instance",
                     displayName = "Top Local Write Keys")
   public Map<String, Long> getLocalTopPuts() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_PUT));
   }

   @ManagedOperation(description = "Show the top n keys most write locally by this instance",
                     displayName = "Nth Top Local Write Keys")
   public Map<String, Long> getNLocalTopPuts(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_PUT, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.LOCAL_PUT);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most locked",
                     displayName = "Top Locked Keys")
   public Map<String, Long> getTopLockedKeys() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_LOCKED_KEYS));
   }

   @ManagedOperation(description = "Show the top n keys most locked",
                     displayName = "Nth Top Locked Keys")
   public Map<String, Long> getNTopLockedKeys(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_LOCKED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_LOCKED_KEYS);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most contended",
                     displayName = "Top Contended Keys")
   public Map<String, Long> getTopContendedKeys() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_CONTENDED_KEYS));
   }

   @ManagedOperation(description = "Show the top n keys most contended",
                     displayName = "Nth Top Contended Keys")
   public Map<String, Long> getNTopContendedKeys(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_CONTENDED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_CONTENDED_KEYS);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys whose lock acquisition failed by timeout",
                     displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<String, Long> getTopLockFailedKeys() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_FAILED_KEYS));
   }

   @ManagedOperation(description = "Show the top n keys whose lock acquisition failed ",
                     displayName = "Nth Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<String, Long> getNTopLockFailedKeys(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_FAILED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_FAILED_KEYS);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public Map<String, Long> getTopWriteSkewFailedKeys() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS));
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "Nth Top Keys whose Write Skew Check was failed")
   public Map<String, Long> getNTopWriteSkewFailedKeys(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS);
      return convertAndSort(res);
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public void setStatisticsEnabled(@Parameter(name = "enabled", description = "true to enable the top-k collection") boolean enabled) {
      streamLibContainer.setEnabled(enabled);
   }

   @Override
   protected void start() {
      super.start();
      log.info("Starting StreamSummaryInterceptor");
      streamLibContainer = StreamLibContainer.getOrCreateStreamLibContainer(cache);
      streamLibContainer.setEnabled(true);
      distributionManager = cache.getAdvancedCache().getDistributionManager();

      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      LockManager oldLockManager = componentRegistry.getComponent(LockManager.class);
      LockManager newLockManager = new TopKeyLockManager(oldLockManager, streamLibContainer);
      log.infof("Replacing LockManager. old=[%s] new=[%s]", oldLockManager, newLockManager);
      componentRegistry.registerComponent(newLockManager, LockManager.class);
      componentRegistry.rewire();
   }

   @Override
   protected void stop() {
      super.stop();
      log.info("Stopping StreamSummaryInterceptor");
      streamLibContainer.setEnabled(false);
   }

   private boolean isRemote(Object key) {
      return distributionManager != null && !distributionManager.getLocality(key).isLocal();
   }

   private Map<String, Long> convertAndSort(Map<Object, Long> topKeyMap) {
      Map<String, Long> sorted = new LinkedHashMap<String, Long>();
      TopKeyEntry[] array = new TopKeyEntry[topKeyMap.size()];
      int insertPosition = 0;
      for (Map.Entry<Object, Long> entry : topKeyMap.entrySet()) {
         array[insertPosition++] = new TopKeyEntry(entry.getKey(), entry.getValue());
      }
      Arrays.sort(array);
      for (TopKeyEntry topKeyEntry : array) {
         sorted.put(String.valueOf(topKeyEntry.key), topKeyEntry.value);
      }
      return sorted;
   }

   private class TopKeyEntry implements Comparable<TopKeyEntry> {

      private final Object key;
      private final long value;

      private TopKeyEntry(Object key, long value) {
         this.key = key;
         this.value = value;
      }

      @Override
      public int compareTo(TopKeyEntry topKeyEntry) {
         return topKeyEntry == null ? 1 : Long.valueOf(value).compareTo(topKeyEntry.value);
      }
   }


}
