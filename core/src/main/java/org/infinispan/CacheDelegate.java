/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan;

import static org.infinispan.context.Flag.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stats.Stats;
import org.infinispan.stats.StatsImpl;
import org.infinispan.util.concurrent.AbstractInProcessNotifyingFuture;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
@SurvivesRestarts
@MBean(objectName = CacheDelegate.OBJECT_NAME, description = "Component that acts as a manager, factory and container for caches in the system.")
public class CacheDelegate<K, V> extends CacheSupport<K,V> implements AdvancedCache<K, V> {
   public static final String OBJECT_NAME = "Cache";
   protected InvocationContextContainer icc;
   protected CommandsFactory commandsFactory;
   protected InterceptorChain invoker;
   protected Configuration config;
   protected CacheNotifier notifier;
   protected BatchContainer batchContainer;
   protected ComponentRegistry componentRegistry;
   protected TransactionManager transactionManager;
   protected RpcManager rpcManager;
   protected StreamingMarshaller marshaller;
   private final String name;
   private EvictionManager evictionManager;
   private DataContainer dataContainer;
   private static final Log log = LogFactory.getLog(CacheDelegate.class);
   private EmbeddedCacheManager cacheManager;
   // this is never used here but should be injected - this is a hack to make sure the StateTransferManager is properly constructed if needed.
   private StateTransferManager stateTransferManager;
   // as above for ResponseGenerator
   private ResponseGenerator responseGenerator;
   private DistributionManager distributionManager;
   private final ThreadLocal<PreInvocationContext> flagHolder = new ThreadLocal<PreInvocationContext>();

   public CacheDelegate(String name) {
      this.name = name;
   }

   @Inject
   public void injectDependencies(EvictionManager evictionManager,
                                  InvocationContextContainer icc,
                                  CommandsFactory commandsFactory,
                                  InterceptorChain interceptorChain,
                                  Configuration configuration,
                                  CacheNotifier notifier,
                                  ComponentRegistry componentRegistry,
                                  TransactionManager transactionManager,
                                  BatchContainer batchContainer,
                                  RpcManager rpcManager, DataContainer dataContainer,
                                  StreamingMarshaller marshaller, ResponseGenerator responseGenerator,
                                  DistributionManager distributionManager,
                                  EmbeddedCacheManager cacheManager, StateTransferManager stateTransferManager) {
      this.commandsFactory = commandsFactory;
      this.invoker = interceptorChain;
      this.config = configuration;
      this.notifier = notifier;
      this.componentRegistry = componentRegistry;
      this.transactionManager = transactionManager;
      this.batchContainer = batchContainer;
      this.rpcManager = rpcManager;
      this.evictionManager = evictionManager;
      this.dataContainer = dataContainer;
      this.marshaller = marshaller;
      this.cacheManager = cacheManager;
      this.responseGenerator = responseGenerator;
      this.stateTransferManager = stateTransferManager;
      this.icc = icc;
      this.distributionManager = distributionManager;
   }

   private void assertKeyNotNull(Object key) {
      if (key == null) {
         throw new NullPointerException("Null keys are not supported!");
      }
   }

   private void assertKeysNotNull(Map<?, ?> data) {
      if (data == null) {
         throw new NullPointerException("Expected map cannot be null");
      }
      for (Object key: data.keySet()) {
         if (key == null) {
            throw new NullPointerException("Null keys are not supported!");
         }
      }
   }

   public final boolean remove(Object key, Object value) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value);
      return (Boolean) invoker.invoke(ctx, command);
   }

   public final int size() {
      SizeCommand command = commandsFactory.buildSizeCommand();
      return (Integer) invoker.invoke(getInvocationContext(false), command);
   }

   public final boolean isEmpty() {
      return size() == 0;
   }

   public final boolean containsKey(Object key) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key);
      Object response = invoker.invoke(ctx, command);
      return response != null;
   }

   public final boolean containsValue(Object value) {
      throw new UnsupportedOperationException("Not supported");
   }

   @SuppressWarnings("unchecked")
   public final V get(Object key) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key);
      return (V) invoker.invoke(ctx, command);
   }

   @SuppressWarnings("unchecked")
   public final V remove(Object key) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, null);
      return (V) invoker.invoke(ctx, command);
   }

   public final void clear() {
      InvocationContext ctx = getInvocationContext(false);
      ClearCommand command = commandsFactory.buildClearCommand();
      invoker.invoke(ctx, command);
   }

   @SuppressWarnings("unchecked")
   public Set<K> keySet() {
      KeySetCommand command = commandsFactory.buildKeySetCommand();
      return (Set<K>) invoker.invoke(getInvocationContext(false), command);
   }

   @SuppressWarnings("unchecked")
   public Collection<V> values() {
      ValuesCommand command = commandsFactory.buildValuesCommand();
      return (Collection<V>) invoker.invoke(getInvocationContext(false), command);
   }

   @SuppressWarnings("unchecked")
   public Set<Map.Entry<K, V>> entrySet() {
      EntrySetCommand command = commandsFactory.buildEntrySetCommand();
      return (Set<Map.Entry<K, V>>) invoker.invoke(getInvocationContext(false), command);
   }

   public final void putForExternalRead(K key, V value) {
      Transaction ongoingTransaction = null;
      try {
         if (transactionManager != null && (ongoingTransaction = transactionManager.getTransaction()) != null) {
            transactionManager.suspend();
         }
         // if the entry exists then this should be a no-op.
         withFlags(FAIL_SILENTLY, FORCE_ASYNCHRONOUS, ZERO_LOCK_ACQUISITION_TIMEOUT, PUT_FOR_EXTERNAL_READ).putIfAbsent(key, value);
      }
      catch (Exception e) {
         if (log.isDebugEnabled()) {
            log.debug("Caught exception while doing putForExternalRead()", e);
         }
      }
      finally {
         try {
            if (ongoingTransaction != null) {
               transactionManager.resume(ongoingTransaction);
            }
         }
         catch (Exception e) {
            log.debug("Had problems trying to resume a transaction after putForExternalRead()", e);
         }
      }
   }

   public final void evict(K key) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(true);
      EvictCommand command = commandsFactory.buildEvictCommand(key);
      invoker.invoke(ctx, command);
   }

   public Configuration getConfiguration() {
      return config;
   }

   public void addListener(Object listener) {
      notifier.addListener(listener);
   }

   public void removeListener(Object listener) {
      notifier.removeListener(listener);
   }

   public Set<Object> getListeners() {
      return notifier.getListeners();
   }

   private InvocationContext getInvocationContext(boolean forceNonTransactional) {
      InvocationContext ctx = forceNonTransactional ? icc.createNonTxInvocationContext() : icc.createInvocationContext();
      PreInvocationContext pic = flagHolder.get();
      if (pic != null && !pic.flags.isEmpty()) {
         ctx.setFlags(pic.flags);
      }
      flagHolder.remove();
      return ctx;
   }

   public boolean lock(K... keys) {
      assertKeyNotNull(keys);
      return lock(Arrays.asList(keys));
   }

   public boolean lock(Collection<? extends K> keys) {
      if (keys == null || keys.isEmpty()) {
         throw new IllegalArgumentException("Cannot lock empty list of keys");
      }
      LockControlCommand command = commandsFactory.buildLockControlCommand(keys, false);
      return (Boolean) invoker.invoke(getInvocationContext(false), command);
   }

   @ManagedOperation(description = "Starts the cache.")
   @Operation(displayName = "Starts cache.")
   public void start() {
      componentRegistry.start();
      defaultLifespan = config.getExpirationLifespan();
      defaultMaxIdleTime = config.getExpirationMaxIdle();
   }

   @ManagedOperation(description = "Stops the cache.")
   @Operation(displayName = "Stops cache.")
   public void stop() {
      componentRegistry.stop();
   }

   public List<CommandInterceptor> getInterceptorChain() {
      return invoker.asList();
   }

   public void addInterceptor(CommandInterceptor i, int position) {
      invoker.addInterceptor(i, position);
   }

   public void addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      invoker.addInterceptorAfter(i, afterInterceptor);
   }

   public void addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      invoker.addInterceptorBefore(i, beforeInterceptor);
   }

   public void removeInterceptor(int position) {
      invoker.removeInterceptor(position);
   }

   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      invoker.removeInterceptor(interceptorType);
   }

   public EvictionManager getEvictionManager() {
      return evictionManager;
   }

   public ComponentRegistry getComponentRegistry() {
      return componentRegistry;
   }

   public DistributionManager getDistributionManager() {
      return distributionManager;
   }

   public ComponentStatus getStatus() {
      return componentRegistry.getStatus();
   }

   /**
    * Returns String representation of ComponentStatus enumeration in order to avoid class not found exceptions in JMX
    * tools that don't have access to infinispan classes.
    */
   @ManagedAttribute(description = "Returns the cache status")
   @Metric(displayName = "Cache status", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getCacheStatus() {
      return getStatus().toString();
   }

   public boolean startBatch() {
      if (!config.isInvocationBatchingEnabled()) {
         throw new ConfigurationException("Invocation batching not enabled in current configuration!  Please use the <invocationBatching /> element.");
      }
      return batchContainer.startBatch();
   }

   public void endBatch(boolean successful) {
      if (!config.isInvocationBatchingEnabled()) {
         throw new ConfigurationException("Invocation batching not enabled in current configuration!  Please use the <invocationBatching /> element.");
      }
      batchContainer.endBatch(successful);
   }

   public String getName() {
      return name;
   }

   /**
    * Returns the cache name. If this is the default cache, it returns a more friendly name.
    */
   @ManagedAttribute(description = "Returns the cache name")
   @Metric(displayName = "Cache name", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getCacheName() {
      return getName().equals(CacheContainer.DEFAULT_CACHE_NAME) ? "Default Cache" : getName();
   }

   public String getVersion() {
      return Version.version;
   }

   @Override
   public String toString() {
      return "Cache '" + name + "'@" + (config.getCacheMode().isClustered() ? getCacheManager().getAddress() : System.identityHashCode(this));
   }

   public BatchContainer getBatchContainer() {
      return batchContainer;
   }

   public InvocationContextContainer getInvocationContextContainer() {
      return icc;
   }

   public DataContainer getDataContainer() {
      return dataContainer;
   }

   public TransactionManager getTransactionManager() {
      return transactionManager;
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   public Stats getStats() {
      return new StatsImpl(invoker);
   }

   @SuppressWarnings("unchecked")
   public final V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      return (V) invoker.invoke(ctx, command);
   }

   @SuppressWarnings("unchecked")
   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      assertKeyNotNull(key);
      InvocationContext context = getInvocationContext(false);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      command.setPutIfAbsent(true);
      return (V) invoker.invoke(context, command);
   }

   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      assertKeysNotNull(map);
      PutMapCommand command = commandsFactory.buildPutMapCommand(map, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      invoker.invoke(getInvocationContext(false), command);
   }

   @SuppressWarnings("unchecked")
   public final V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, null, value, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      return (V) invoker.invoke(ctx, command);

   }

   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, oldValue, value, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      return (Boolean) invoker.invoke(ctx, command);
   }

   /**
    * Wraps a return value as a future, if needed.  Typically, if the stack, operation and configuration support
    * handling of futures, this retval is already a future in which case this method does nothing except cast to
    * future.
    * <p/>
    * Otherwise, a future wrapper is created, which has already completed and simply returns the retval.  This is used
    * for API consistency.
    *
    * @param retval return value to wrap
    * @param <X>    contents of the future
    * @return a future
    */
   @SuppressWarnings("unchecked")
   private <X> NotifyingFuture<X> wrapInFuture(final Object retval) {
      if (retval instanceof NotifyingFuture) {
         return (NotifyingFuture<X>) retval;
      } else {
         return new AbstractInProcessNotifyingFuture<X>() {
            @SuppressWarnings("unchecked")
            public X get() throws InterruptedException, ExecutionException {
               return (X) retval;
            }
         };
      }
   }

   public final NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      ctx.setUseFutureReturnType(true);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      assertKeysNotNull(data);
      InvocationContext ctx = getInvocationContext(false);
      ctx.setUseFutureReturnType(true);
      PutMapCommand command = commandsFactory.buildPutMapCommand(data, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final NotifyingFuture<Void> clearAsync() {
      InvocationContext ctx = getInvocationContext(false);
      ctx.setUseFutureReturnType(true);
      ClearCommand command = commandsFactory.buildClearCommand();
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      ctx.setUseFutureReturnType(true);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      command.setPutIfAbsent(true);
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final NotifyingFuture<V> removeAsync(Object key) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      ctx.setUseFutureReturnType(true);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, null);
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      ctx.setUseFutureReturnType(true);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value);
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      ctx.setUseFutureReturnType(true);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, null, value, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContext(false);
      ctx.setUseFutureReturnType(true);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, oldValue, newValue, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public AdvancedCache<K, V> getAdvancedCache() {
      return this;
   }

   public void compact() {
      for (InternalCacheEntry e : dataContainer) {
         if (e.getKey() instanceof MarshalledValue) {
            ((MarshalledValue) e.getKey()).compact(true, true);
         }
         if (e.getValue() instanceof MarshalledValue) {
            ((MarshalledValue) e.getValue()).compact(true, true);
         }
      }
   }

   public RpcManager getRpcManager() {
      return rpcManager;
   }

   public AdvancedCache<K, V> withFlags(Flag... flags) {
      if (flags != null && flags.length > 0) {
         PreInvocationContext pic = flagHolder.get();
         if (pic == null) {
            flagHolder.set(new PreInvocationContext(flags));
         } else {
            flagHolder.set(pic.add(flags));
         }
      }
      return this;
   }

   private static final class PreInvocationContext {
      EnumSet<Flag> flags;

      private PreInvocationContext(Flag[] flags) {
         this.flags = flags != null && flags.length > 0 ? EnumSet.copyOf(Arrays.asList(flags)) : EnumSet.noneOf(Flag.class);
      }

      private PreInvocationContext add(Flag[] newFlags) {
         if (newFlags != null && newFlags.length > 0) {
            flags.addAll(Arrays.asList(newFlags));
         }
         return this;
      }
   }
}
