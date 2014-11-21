package org.infinispan.stats.topK;

import java.util.Map;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.GetManyCommand;
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
import org.infinispan.stats.logging.Log;
import org.infinispan.stats.wrappers.TopKeyLockManager;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.LogFactory;

/**
 * Intercepts the VisitableCommands to calculate the corresponding top-key values.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@MBean(objectName = "CacheUsageStatistics", description = "Keeps tracks of the accessed keys")
public class CacheUsageInterceptor extends BaseCustomInterceptor {

   public static final int DEFAULT_TOP_KEY = 10;
   private static final Log log = LogFactory.getLog(CacheUsageInterceptor.class, Log.class);
   private StreamSummaryContainer streamSummaryContainer;
   private DistributionManager distributionManager;

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {

      if (streamSummaryContainer.isEnabled() && ctx.isOriginLocal()) {
         streamSummaryContainer.addGet(command.getKey(), isRemote(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetManyCommand(InvocationContext ctx, GetManyCommand command) throws Throwable {
      if (streamSummaryContainer.isEnabled() && ctx.isOriginLocal()) {
         for (Object key : command.getKeys()) {
            streamSummaryContainer.addGet(key, isRemote(key));
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   // TODO: implement visitPutMapCommand

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         if (streamSummaryContainer.isEnabled() && ctx.isOriginLocal()) {
            streamSummaryContainer.addPut(command.getKey(), isRemote(command.getKey()));
         }
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (streamSummaryContainer.isEnabled() && key != null && ctx.isOriginLocal()) {
            streamSummaryContainer.addWriteSkewFailed(key);
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
         if (streamSummaryContainer.isEnabled() && key != null && ctx.isOriginLocal()) {
            streamSummaryContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @ManagedOperation(description = "Resets statistics gathered by this component",
                     displayName = "Reset Statistics (Statistics)")
   public void resetStatistics() {
      streamSummaryContainer.resetAll();
   }

   @ManagedOperation(description = "Set K for the top-K values",
                     displayName = "Set capacity")
   public void setTopKValue(@Parameter(name = "n", description = "the n-th top key to collect") int n) {
      streamSummaryContainer.setCapacity(n);
   }

   @ManagedAttribute(description = "Shows the current capacity for top-K values",
                     displayName = "getCapacity")
   public int getCapacity() {
      return streamSummaryContainer.getCapacity();
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most read remotely by this instance",
                     displayName = "Top Remote Read Keys")
   public Map<String, Long> getRemoteTopGets() {
      return getNRemoteTopGets(DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most read remotely by this instance",
                     displayName = "Nth Top Remote Read Keys")
   public Map<String, Long> getNRemoteTopGets(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      return streamSummaryContainer.getTopKFromAsKeyString(StreamSummaryContainer.Stat.REMOTE_GET, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most read locally by this instance",
                     displayName = "Top Local Read Keys")
   public Map<String, Long> getLocalTopGets() {
      return getNLocalTopGets(DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most read locally by this instance",
                     displayName = "Nth Top Local Read Keys")
   public Map<String, Long> getNLocalTopGets(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      return streamSummaryContainer.getTopKFromAsKeyString(StreamSummaryContainer.Stat.LOCAL_GET, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most write remotely by this instance",
                     displayName = "Top Remote Write Keys")
   public Map<String, Long> getRemoteTopPuts() {
      return getNRemoteTopPuts(DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most write remotely by this instance",
                     displayName = "Nth Top Remote Write Keys")
   public Map<String, Long> getNRemoteTopPuts(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      return streamSummaryContainer.getTopKFromAsKeyString(StreamSummaryContainer.Stat.REMOTE_PUT, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most write locally by this instance",
                     displayName = "Top Local Write Keys")
   public Map<String, Long> getLocalTopPuts() {
      return getNLocalTopPuts(DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most write locally by this instance",
                     displayName = "Nth Top Local Write Keys")
   public Map<String, Long> getNLocalTopPuts(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      return streamSummaryContainer.getTopKFromAsKeyString(StreamSummaryContainer.Stat.LOCAL_PUT, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most locked",
                     displayName = "Top Locked Keys")
   public Map<String, Long> getTopLockedKeys() {
      return getNTopLockedKeys(DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most locked",
                     displayName = "Nth Top Locked Keys")
   public Map<String, Long> getNTopLockedKeys(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      return streamSummaryContainer.getTopKFromAsKeyString(StreamSummaryContainer.Stat.MOST_LOCKED_KEYS, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most contended",
                     displayName = "Top Contended Keys")
   public Map<String, Long> getTopContendedKeys() {
      return getNTopContendedKeys(DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most contended",
                     displayName = "Nth Top Contended Keys")
   public Map<String, Long> getNTopContendedKeys(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      return streamSummaryContainer.getTopKFromAsKeyString(StreamSummaryContainer.Stat.MOST_CONTENDED_KEYS, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys whose lock acquisition failed by timeout",
                     displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<String, Long> getTopLockFailedKeys() {
      return getNTopLockFailedKeys(DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys whose lock acquisition failed ",
                     displayName = "Nth Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<String, Long> getNTopLockFailedKeys(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      return streamSummaryContainer.getTopKFromAsKeyString(StreamSummaryContainer.Stat.MOST_FAILED_KEYS, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public Map<String, Long> getTopWriteSkewFailedKeys() {
      return getNTopWriteSkewFailedKeys(DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "Nth Top Keys whose Write Skew Check was failed")
   public Map<String, Long> getNTopWriteSkewFailedKeys(@Parameter(name = "n", description = "the n-th top key to return") int n) {
      return streamSummaryContainer.getTopKFromAsKeyString(StreamSummaryContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS, n);
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public void setStatisticsEnabled(@Parameter(name = "enabled", description = "true to enable the top-k collection") boolean enabled) {
      streamSummaryContainer.setEnabled(enabled);
   }

   @Override
   protected void start() {
      super.start();
      log.startStreamSummaryInterceptor();
      streamSummaryContainer = StreamSummaryContainer.getOrCreateStreamLibContainer(cache);
      streamSummaryContainer.setEnabled(true);
      distributionManager = cache.getAdvancedCache().getDistributionManager();

      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      LockManager oldLockManager = componentRegistry.getComponent(LockManager.class);
      LockManager newLockManager = new TopKeyLockManager(oldLockManager, streamSummaryContainer);
      log.replaceComponent("LockManager", oldLockManager, newLockManager);
      componentRegistry.registerComponent(newLockManager, LockManager.class);
      componentRegistry.rewire();
   }

   @Override
   protected void stop() {
      super.stop();
      log.stopStreamSummaryInterceptor();
      streamSummaryContainer.setEnabled(false);
   }

   private boolean isRemote(Object key) {
      return distributionManager != null && !distributionManager.getLocality(key).isLocal();
   }
}
