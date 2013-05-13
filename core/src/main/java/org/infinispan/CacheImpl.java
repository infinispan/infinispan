/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.stats.Stats;
import org.infinispan.stats.StatsImpl;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.AbstractInProcessNotifyingFuture;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.NotifyingFutureAdaptor;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.*;
import static org.infinispan.context.InvocationContextContainer.*;
import static org.infinispan.factories.KnownComponentNames.*;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
@SurvivesRestarts
@MBean(objectName = CacheImpl.OBJECT_NAME, description = "Component that represents an individual cache instance.")
public class CacheImpl<K, V> extends CacheSupport<K, V> implements AdvancedCache<K, V> {
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
   private static final Log log = LogFactory.getLog(CacheImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private EmbeddedCacheManager cacheManager;
   private LockManager lockManager;
   private DistributionManager distributionManager;
   private ExecutorService asyncExecutor;
   private TransactionTable txTable;
   private RecoveryManager recoveryManager;
   private TransactionCoordinator txCoordinator;
   private GlobalConfiguration globalCfg;
   private boolean isClassLoaderInContext;

   public CacheImpl(String name) {
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
                                  @ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller,
                                  DistributionManager distributionManager,
                                  EmbeddedCacheManager cacheManager,
                                  @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncExecutor,
                                  TransactionTable txTable, RecoveryManager recoveryManager, TransactionCoordinator txCoordinator,
                                  LockManager lockManager,
                                  GlobalConfiguration globalCfg) {
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
      this.icc = icc;
      this.distributionManager = distributionManager;
      this.asyncExecutor = asyncExecutor;
      this.txTable = txTable;
      this.recoveryManager = recoveryManager;
      this.txCoordinator = txCoordinator;
      this.lockManager = lockManager;
      this.globalCfg = globalCfg;
   }

   private void assertKeyNotNull(Object key) {
      if (key == null) {
         throw new NullPointerException("Null keys are not supported!");
      }
   }

   private void assertKeyValueNotNull(Object key, Object value) {
      assertKeyNotNull(key);
      if (value == null) {
         throw new NullPointerException("Null values are not supported!");
      }
   }

   private void assertValueNotNull(Object value) {
      if (value == null) {
         throw new NullPointerException("Null values are not supported!");
      }
   }

   private void assertKeysNotNull(Map<?, ?> data) {
      if (data == null) {
         throw new NullPointerException("Expected map cannot be null");
      }
      for (Object key : data.keySet()) {
         if (key == null) {
            throw new NullPointerException("Null keys are not supported!");
         }
      }
   }

   @Override
   public final boolean remove(Object key, Object value) {
      return remove(key, value, null, null);
   }

   final boolean remove(Object key, Object value, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(false, explicitClassLoader, 1);
      return removeInternal(key, value, explicitFlags, ctx);
   }

   private boolean removeInternal(Object key, Object value, EnumSet<Flag> explicitFlags, InvocationContext ctx) {
      assertKeyValueNotNull(key, value);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value, explicitFlags);
      return (Boolean) executeCommandAndCommitIfNeeded(ctx, command);
   }

   @Override
   public final int size() {
      return size(null, null);
   }

   final int size(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      SizeCommand command = commandsFactory.buildSizeCommand();
      return (Integer) invoker.invoke(getInvocationContextForRead(
            null, explicitClassLoader, UNBOUNDED), command);
   }

   @Override
   public final boolean isEmpty() {
      return size() == 0;
   }

   final boolean isEmpty(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      return size(explicitFlags, explicitClassLoader) == 0;
   }

   @Override
   public final boolean containsKey(Object key) {
      return containsKey(key, null, null);
   }

   final boolean containsKey(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContextForRead(null, explicitClassLoader, 1);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, explicitFlags, false);
      Object response = invoker.invoke(ctx, command);
      return response != null;
   }

   @Override
   public final boolean containsValue(Object value) {
      throw new UnsupportedOperationException("Not supported");
   }

   @Override
   public final V get(Object key) {
      return get(key, null, null);
   }

   @SuppressWarnings("unchecked")
   final V get(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContextForRead(null, explicitClassLoader, 1);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, explicitFlags, false);
      return (V) invoker.invoke(ctx, command);
   }

   @Override
   public final CacheEntry getCacheEntry(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContextForRead(null, explicitClassLoader, 1);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, explicitFlags, true);
      Object ret = invoker.invoke(ctx, command);
      return (CacheEntry) ret;
   }

   @Override
   public final CacheEntry getCacheEntry(K key) {
      return getCacheEntry(key, null, null);
   }

   @Override
   public final V remove(Object key) {
      return remove(key, null, null);
   }

   final V remove(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(false, explicitClassLoader, 1);
      return removeInternal(key, explicitFlags, ctx);
   }

   @SuppressWarnings("unchecked")
   private V removeInternal(Object key, EnumSet<Flag> explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, null, explicitFlags);
      return (V) executeCommandAndCommitIfNeeded(ctx, command);
   }


   @ManagedOperation(
         description = "Clears the cache",
         displayName = "Clears the cache", name = "clear"
   )
   public final void clearOperation() {
      //if we have a TM then this cache is transactional.
      // We shouldn't rely on the auto-commit option as it might be disabled, so always start a tm.
      if (transactionManager != null) {
         try {
            transactionManager.begin();
            clear(null, null);
            transactionManager.commit();
         } catch (Throwable e) {
            throw new CacheException(e);
         }
      } else {
         clear(null, null);
      }
   }

   @Override
   public final void clear() {
      clear(null, null);
   }

   final void clear(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(false, explicitClassLoader, UNBOUNDED);
      clearInternal(explicitFlags, ctx);
   }

   private void clearInternal(EnumSet<Flag> explicitFlags, InvocationContext ctx) {
      ClearCommand command = commandsFactory.buildClearCommand(explicitFlags);
      executeCommandAndCommitIfNeeded(ctx, command);
   }

   @Override
   public Set<K> keySet() {
      return keySet(null, null);
   }

   @SuppressWarnings("unchecked")
   Set<K> keySet(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(null, explicitClassLoader, UNBOUNDED);
      KeySetCommand command = commandsFactory.buildKeySetCommand();
      return (Set<K>) invoker.invoke(ctx, command);
   }

   @Override
   public Collection<V> values() {
      return values(null, null);
   }

   @SuppressWarnings("unchecked")
   Collection<V> values(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(null, explicitClassLoader, UNBOUNDED);
      ValuesCommand command = commandsFactory.buildValuesCommand();
      return (Collection<V>) invoker.invoke(ctx, command);
   }

   @Override
   public Set<Map.Entry<K, V>> entrySet() {
      return entrySet(null, null);
   }

   @SuppressWarnings("unchecked")
   Set<Map.Entry<K, V>> entrySet(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(null, explicitClassLoader, UNBOUNDED);
      EntrySetCommand command = commandsFactory.buildEntrySetCommand();
      return (Set<Map.Entry<K, V>>) invoker.invoke(ctx, command);
   }

   @Override
   public final void putForExternalRead(K key, V value) {
      putForExternalRead(key, value, null, null);
   }

   final void putForExternalRead(K key, V value, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      Transaction ongoingTransaction = null;
      try {
         ongoingTransaction = getOngoingTransaction();
         if (ongoingTransaction != null)
            transactionManager.suspend();

         EnumSet<Flag> flags = EnumSet.of(FAIL_SILENTLY, FORCE_ASYNCHRONOUS, ZERO_LOCK_ACQUISITION_TIMEOUT, PUT_FOR_EXTERNAL_READ);
         if (explicitFlags != null && !explicitFlags.isEmpty()) {
            flags.addAll(explicitFlags);
         }

         // if the entry exists then this should be a no-op.
         Metadata metadata = new EmbeddedMetadata.Builder()
               .lifespan(defaultLifespan, TimeUnit.MILLISECONDS)
               .maxIdle(defaultMaxIdleTime, TimeUnit.MILLISECONDS).build();
         putIfAbsent(key, value, metadata, flags, explicitClassLoader);
      } catch (Exception e) {
         if (log.isDebugEnabled()) log.debug("Caught exception while doing putForExternalRead()", e);
      }
      finally {
         try {
            if (ongoingTransaction != null) transactionManager.resume(ongoingTransaction);
         } catch (Exception e) {
            if (log.isDebugEnabled())
               log.debug("Had problems trying to resume a transaction after putForExternalRead()", e);
         }
      }
   }

   @Override
   public final void evict(K key) {
      evict(key, null, null);
   }

   final void evict(K key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      assertKeyNotNull(key);
      InvocationContext ctx = createSingleKeyNonTxInvocationContext(explicitClassLoader);
      EvictCommand command = commandsFactory.buildEvictCommand(key, explicitFlags);
      invoker.invoke(ctx, command);
   }

   private InvocationContext createSingleKeyNonTxInvocationContext(ClassLoader explicitClassLoader) {
      InvocationContext ctx = icc.createSingleKeyNonTxInvocationContext();
      return setInvocationContextClassLoader(ctx, explicitClassLoader);
   }

   @Override
   @Deprecated
   public org.infinispan.config.Configuration getConfiguration() {
      return LegacyConfigurationAdaptor.adapt(config);
   }

   @Override
   public Configuration getCacheConfiguration() {
      return config;
   }

   @Override
   public void addListener(Object listener) {
      notifier.addListener(listener);
   }

   @Override
   public void removeListener(Object listener) {
      notifier.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      return notifier.getListeners();
   }

   private InvocationContext getInvocationContextForWrite(ClassLoader explicitClassLoader, int keyCount, boolean isPutForExternalRead) {
      InvocationContext ctx = isPutForExternalRead
            ? icc.createSingleKeyNonTxInvocationContext()
            : icc.createInvocationContext(true, keyCount);
      return setInvocationContextClassLoader(ctx, explicitClassLoader);
   }

   private InvocationContext getInvocationContextForRead(Transaction tx, ClassLoader explicitClassLoader, int keyCount) {
      if (config.transaction().transactionMode().isTransactional()) {
         Transaction transaction = tx == null ? getOngoingTransaction() : tx;
         //if we are in the scope of a transaction than return a transactional context. This is relevant e.g.
         // FORCE_WRITE_LOCK is used on read operations - in that case, the lock is held for the the transaction's
         // lifespan (when in tx scope) vs. call lifespan (when not in tx scope).
         if (transaction != null)
            return getInvocationContext(transaction, explicitClassLoader);
      }
      InvocationContext result = icc.createInvocationContext(false, keyCount);
      setInvocationContextClassLoader(result, explicitClassLoader);
      return result;
   }

   private InvocationContext getInvocationContextWithImplicitTransactionForAsyncOps(boolean isPutForExternalRead, ClassLoader explicitClassLoader, int keyCount) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(isPutForExternalRead, explicitClassLoader, keyCount);
      //If the transaction was injected then we should not have it associated to caller's thread, but with the async thread
      try {
         if (isTxInjected(ctx))
            transactionManager.suspend();
      } catch (SystemException e) {
         throw new CacheException(e);
      }
      return ctx;
   }

   /**
    * If this is a transactional cache and autoCommit is set to true then starts a transaction if this is
    * not a transactional call.
    */
   private InvocationContext getInvocationContextWithImplicitTransaction(boolean isPutForExternalRead, ClassLoader explicitClassLoader, int keyCount) {
      InvocationContext invocationContext;
      boolean txInjected = false;
      if (config.transaction().transactionMode().isTransactional() && !isPutForExternalRead) {
         Transaction transaction = getOngoingTransaction();
         if (transaction == null && config.transaction().autoCommit()) {
            try {
               transactionManager.begin();
               transaction = transactionManager.getTransaction();
               txInjected = true;
               if (trace) log.trace("Implicit transaction started!");
            } catch (Exception e) {
               throw new CacheException("Could not start transaction", e);
            }
         }
         invocationContext = getInvocationContext(transaction, explicitClassLoader);
      } else {
         invocationContext = getInvocationContextForWrite(explicitClassLoader, keyCount, isPutForExternalRead);
      }
      if (txInjected) {
         ((TxInvocationContext) invocationContext).setImplicitTransaction(true);
         if (trace) log.tracef("Marked tx as implicit.");
      }
      return invocationContext;
   }

   private boolean isPutForExternalRead(EnumSet<Flag> explicitFlags) {
      return explicitFlags != null && explicitFlags.contains(PUT_FOR_EXTERNAL_READ);
   }

   private InvocationContext getInvocationContext(Transaction tx, ClassLoader explicitClassLoader) {
      InvocationContext ctx = icc.createInvocationContext(tx);
      return setInvocationContextClassLoader(ctx, explicitClassLoader);
   }

   private InvocationContext setInvocationContextClassLoader(InvocationContext ctx, ClassLoader explicitClassLoader) {
      if (isClassLoaderInContext) {
         ctx.setClassLoader(explicitClassLoader != null ?
               explicitClassLoader : getClassLoader());
      }
      return ctx;
   }

   @Override
   public boolean lock(K... keys) {
      assertKeyNotNull(keys);
      return lock(Arrays.asList(keys), null, null);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return lock(keys, null, null);
   }

   boolean lock(Collection<? extends K> keys, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      if (!config.transaction().transactionMode().isTransactional())
         throw new UnsupportedOperationException("Calling lock() on non-transactional caches is not allowed");

      if (keys == null || keys.isEmpty()) {
         throw new IllegalArgumentException("Cannot lock empty list of keys");
      }
      InvocationContext ctx = getInvocationContextForWrite(explicitClassLoader, UNBOUNDED, false);
      LockControlCommand command = commandsFactory.buildLockControlCommand((Collection<Object>) keys, explicitFlags);
      return (Boolean) invoker.invoke(ctx, command);
   }

   @Override
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire) {
      if (locksToAcquire == null || locksToAcquire.length == 0) {
         throw new IllegalArgumentException("Cannot lock empty list of keys");
      }
      InvocationContext ctx = getInvocationContextForWrite(null, UNBOUNDED, false);
      ApplyDeltaCommand command = commandsFactory.buildApplyDeltaCommand(deltaAwareValueKey, delta, Arrays.asList(locksToAcquire));
      invoker.invoke(ctx, command);
   }

   @Override
   @ManagedOperation(
         description = "Starts the cache.",
         displayName = "Starts cache."
   )
   public void start() {
      componentRegistry.start();
      defaultLifespan = config.expiration().lifespan();
      defaultMaxIdleTime = config.expiration().maxIdle();
      // Context only needs to ship ClassLoader if marshalling will be required
      isClassLoaderInContext = config.clustering().cacheMode().isClustered()
            || config.loaders().usingCacheLoaders()
            || config.storeAsBinary().enabled();

      if (log.isDebugEnabled()) log.debugf("Started cache %s on %s", getName(), getCacheManager().getAddress());
   }

   @Override
   @ManagedOperation(
         description = "Stops the cache.",
         displayName = "Stops cache."
   )
   public void stop() {
      stop(null);
   }

   void stop(ClassLoader explicitClassLoader) {
      if (log.isDebugEnabled()) log.debugf("Stopping cache %s on %s", getName(), getCacheManager().getAddress());
      componentRegistry.stop();
   }

   @Override
   public List<CommandInterceptor> getInterceptorChain() {
      return invoker.asList();
   }

   @Override
   public void addInterceptor(CommandInterceptor i, int position) {
      invoker.addInterceptor(i, position);
   }

   @Override
   public boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      return invoker.addInterceptorAfter(i, afterInterceptor);
   }

   @Override
   public boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      return invoker.addInterceptorBefore(i, beforeInterceptor);
   }

   @Override
   public void removeInterceptor(int position) {
      invoker.removeInterceptor(position);
   }

   @Override
   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      invoker.removeInterceptor(interceptorType);
   }

   @Override
   public EvictionManager getEvictionManager() {
      return evictionManager;
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      return componentRegistry;
   }

   @Override
   public DistributionManager getDistributionManager() {
      return distributionManager;
   }

   @Override
   public ComponentStatus getStatus() {
      return componentRegistry.getStatus();
   }

   /**
    * Returns String representation of ComponentStatus enumeration in order to avoid class not found exceptions in JMX
    * tools that don't have access to infinispan classes.
    */
   @ManagedAttribute(
         description = "Returns the cache status",
         displayName = "Cache status",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public String getCacheStatus() {
      return getStatus().toString();
   }

   @Override
   public boolean startBatch() {
      if (!config.invocationBatching().enabled()) {
         throw new ConfigurationException("Invocation batching not enabled in current configuration! Please enable it.");
      }
      return batchContainer.startBatch();
   }

   @Override
   public void endBatch(boolean successful) {
      if (!config.invocationBatching().enabled()) {
         throw new ConfigurationException("Invocation batching not enabled in current configuration! Please enable it.");
      }
      batchContainer.endBatch(successful);
   }

   @Override
   public String getName() {
      return name;
   }

   /**
    * Returns the cache name. If this is the default cache, it returns a more friendly name.
    */
   @ManagedAttribute(
         description = "Returns the cache name",
         displayName = "Cache name",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public String getCacheName() {
      String name = getName().equals(CacheContainer.DEFAULT_CACHE_NAME) ? "Default Cache" : getName();
      return name + "(" + getConfiguration().getCacheModeString().toLowerCase() + ")";
   }

   /**
    * Returns the cache configuration as XML string.
    */
   @ManagedAttribute(
         description = "Returns the cache configuration as XML string",
         displayName = "Cache configuration (XML)",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public String getConfigurationAsXmlString() {
      return getConfiguration().toXmlString();
   }

   @Override
   public String getVersion() {
      return Version.VERSION;
   }

   @Override
   public String toString() {
      return "Cache '" + name + "'@" + (config !=null && config.clustering().cacheMode().isClustered() ? getCacheManager().getAddress() : Util.hexIdHashCode(this));
   }

   @Override
   public BatchContainer getBatchContainer() {
      return batchContainer;
   }

   @Override
   public InvocationContextContainer getInvocationContextContainer() {
      return icc;
   }

   @Override
   public DataContainer getDataContainer() {
      return dataContainer;
   }

   @Override
   public TransactionManager getTransactionManager() {
      return transactionManager;
   }

   @Override
   public LockManager getLockManager() {
      return this.lockManager;
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   @Override
   public Stats getStats() {
      return new StatsImpl(invoker);
   }

   @Override
   public XAResource getXAResource() {
      return new TransactionXaAdapter(txTable, recoveryManager, txCoordinator, commandsFactory, rpcManager, null, config, name);
   }

   @Override
   public final V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return put(key, value, metadata, null, null);
   }

   @SuppressWarnings("unchecked")
   final V put(K key, V value, Metadata metadata,
         EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(false, explicitClassLoader, 1);
      return putInternal(key, value, metadata, explicitFlags, ctx);
   }

   @SuppressWarnings("unchecked")
   private V putInternal(K key, V value, Metadata metadata,
         EnumSet<Flag> explicitFlags, InvocationContext ctx) {
      assertKeyValueNotNull(key, value);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, metadata, explicitFlags);
      return (V) executeCommandAndCommitIfNeeded(ctx, command);
   }

   @Override
   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return putIfAbsent(key, value, metadata, null, null);
   }

   @SuppressWarnings("unchecked")
   final V putIfAbsent(K key, V value, Metadata metadata,
         EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(isPutForExternalRead(explicitFlags), explicitClassLoader, 1);
      return putIfAbsentInternal(key, value, metadata, explicitFlags, ctx);
   }

   @SuppressWarnings("unchecked")
   private V putIfAbsentInternal(K key, V value, Metadata metadata,
         EnumSet<Flag> explicitFlags, InvocationContext ctx) {
      assertKeyValueNotNull(key, value);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, metadata, explicitFlags);
      command.setPutIfAbsent(true);
      return (V) executeCommandAndCommitIfNeeded(ctx, command);
   }

   @Override
   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      putAll(map, metadata, null, null);
   }

   final void putAll(Map<? extends K, ? extends V> map, Metadata metadata, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(false, explicitClassLoader, map.size());
      putAllInternal(map, metadata, explicitFlags, ctx);
   }

   private void putAllInternal(Map<? extends K, ? extends V> map, Metadata metadata, EnumSet<Flag> explicitFlags, InvocationContext ctx) {
      assertKeysNotNull(map);
      PutMapCommand command = commandsFactory.buildPutMapCommand(map, metadata, explicitFlags);
      executeCommandAndCommitIfNeeded(ctx, command);
   }

   @Override
   public final V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return replace(key, value, metadata, null, null);
   }

   @SuppressWarnings("unchecked")
   final V replace(K key, V value, Metadata metadata, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(false, explicitClassLoader, 1);
      return replaceInternal(key, value, metadata, explicitFlags, ctx);
   }

   @SuppressWarnings("unchecked")
   private V replaceInternal(K key, V value, Metadata metadata, EnumSet<Flag> explicitFlags, InvocationContext ctx) {
      assertKeyValueNotNull(key, value);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, null, value, metadata, explicitFlags);
      return (V) executeCommandAndCommitIfNeeded(ctx, command);
   }

   @Override
   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return replace(key, oldValue, value, metadata, null, null);
   }

   final boolean replace(K key, V oldValue, V value, Metadata metadata,
         EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextWithImplicitTransaction(false, explicitClassLoader, 1);
      return replaceInternal(key, oldValue, value, metadata, explicitFlags, ctx);
   }

   private boolean replaceInternal(K key, V oldValue, V value, Metadata metadata,
         EnumSet<Flag> explicitFlags, InvocationContext ctx) {
      assertKeyValueNotNull(key, value);
      assertValueNotNull(oldValue);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(
            key, oldValue, value, metadata, explicitFlags);
      return (Boolean) executeCommandAndCommitIfNeeded(ctx, command);
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
            @Override
            @SuppressWarnings("unchecked")
            public X get() throws InterruptedException, ExecutionException {
               return (X) retval;
            }
         };
      }
   }

   @Override
   public final NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return putAsync(key, value, metadata, null, null);
   }

   final NotifyingFuture<V> putAsync(final K key, final V value, final Metadata metadata, final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureAdaptor<V> result = new NotifyingFutureAdaptor<V>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<V> returnValue = asyncExecutor.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               return putInternal(key, value, metadata, explicitFlags, ctx);
            } finally {
               try {
                  result.notifyDone();
                  log.trace("Finished notifying");
               } catch (Throwable e) {
                  log.trace("Exception while notifying the future", e);
               }
            }
         }
      });
      result.setActual(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return putAllAsync(data, metadata, null, null);
   }

   final NotifyingFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final Metadata metadata, final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureAdaptor<Void> result = new NotifyingFutureAdaptor<Void>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, data.size());
      Future<Void> returnValue = asyncExecutor.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               putAllInternal(data, metadata, explicitFlags, ctx);
               return null;
            } finally {
               result.notifyDone();
            }
         }
      });
      result.setActual(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<Void> clearAsync() {
      return clearAsync(null, null);
   }

   final NotifyingFuture<Void> clearAsync(final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureAdaptor<Void> result = new NotifyingFutureAdaptor<Void>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, UNBOUNDED);
      Future<Void> returnValue = asyncExecutor.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               clearInternal(explicitFlags, ctx);
               return null;
            } finally {
               result.notifyDone();
            }
         }
      });
      result.setActual(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return putIfAbsentAsync(key, value, metadata, null, null);
   }

   final NotifyingFuture<V> putIfAbsentAsync(final K key, final V value, final Metadata metadata,
         final EnumSet<Flag> explicitFlags,final ClassLoader explicitClassLoader) {
      final NotifyingFutureAdaptor<V> result = new NotifyingFutureAdaptor<V>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<V> returnValue = asyncExecutor.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               return putIfAbsentInternal(key, value, metadata, explicitFlags, ctx);
            } finally {
               result.notifyDone();
            }
         }
      });
      result.setActual(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<V> removeAsync(Object key) {
      return removeAsync(key, null, null);
   }

   final NotifyingFuture<V> removeAsync(final Object key, final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureAdaptor<V> result = new NotifyingFutureAdaptor<V>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<V> returnValue = asyncExecutor.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               return removeInternal(key, explicitFlags, ctx);
            } finally {
               result.notifyDone();
            }
         }
      });
      result.setActual(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return removeAsync(key, value, null, null);
   }

   final NotifyingFuture<Boolean> removeAsync(final Object key, final Object value, final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureAdaptor<Boolean> result = new NotifyingFutureAdaptor<Boolean>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<Boolean> returnValue = asyncExecutor.submit(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               return removeInternal(key, value, explicitFlags, ctx);
            } finally {
               result.notifyDone();
            }
         }
      });
      result.setActual(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return replaceAsync(key, value, metadata, null, null);
   }

   final NotifyingFuture<V> replaceAsync(final K key, final V value, final Metadata metadata,
         final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureAdaptor<V> result = new NotifyingFutureAdaptor<V>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<V> returnValue = asyncExecutor.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               return replaceInternal(key, value, metadata, explicitFlags, ctx);
            } finally {
               result.notifyDone();
            }
         }
      });
      result.setActual(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return replaceAsync(key, oldValue, newValue, metadata, null, null);
   }

   final NotifyingFuture<Boolean> replaceAsync(final K key, final V oldValue, final V newValue,
         final Metadata metadata, final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureAdaptor<Boolean> result = new NotifyingFutureAdaptor<Boolean>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<Boolean> returnValue = asyncExecutor.submit(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               return replaceInternal(key, oldValue, newValue, metadata, explicitFlags, ctx);
            } finally {
               result.notifyDone();
            }
         }
      });
      result.setActual(returnValue);
      return result;
   }

   @Override
   public NotifyingFuture<V> getAsync(K key) {
      return getAsync(key, null, null);
   }

   @SuppressWarnings("unchecked")
   NotifyingFuture<V> getAsync(final K key, final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      // Optimization to not start a new thread only when the operation is cheap:
      if (asyncSkipsThread(explicitFlags, key)) {
         return wrapInFuture(get(key, explicitFlags, explicitClassLoader));
      } else {
         // Make sure the flags are cleared
         final EnumSet<Flag> appliedFlags;
         if (explicitFlags == null) {
            appliedFlags = null;
         } else {
            appliedFlags = explicitFlags.clone();
            explicitFlags.clear();
         }
         final NotifyingFutureAdaptor<V> f = new NotifyingFutureAdaptor<V>();

         Callable<V> c = new Callable<V>() {
            @Override
            public V call() throws Exception {
               try {
                  return get(key, appliedFlags, explicitClassLoader);
               } finally {
                  f.notifyDone();
               }
            }
         };
         f.setActual(asyncExecutor.submit(c));
         return f;
      }
   }

   /**
    * Encodes the cases for an asyncGet operation in which it makes sense to actually perform the operation in sync.
    *
    * @return true if we skip the thread (performing it in sync)
    */
   private boolean asyncSkipsThread(EnumSet<Flag> flags, K key) {
      boolean isSkipLoader = isSkipLoader(flags);
      if (!isSkipLoader) {
         // if we can't skip the cacheloader, we really want a thread for async.
         return false;
      }
      if (!config.clustering().cacheMode().isDistributed()) {
         //in these cluster modes we won't RPC for a get, so no need to fork a thread.
         return true;
      } else if (flags != null && (flags.contains(Flag.SKIP_REMOTE_LOOKUP) || flags.contains(Flag.CACHE_MODE_LOCAL))) {
         //with these flags we won't RPC either
         return true;
      }
      //finally, we will skip the thread if the key maps to the local node
      return distributionManager.getLocality(key).isLocal();
   }

   private boolean isSkipLoader(EnumSet<Flag> flags) {
      boolean hasCacheLoaderConfig = !config.loaders().cacheLoaders().isEmpty();
      return !hasCacheLoaderConfig
            || (flags != null && (flags.contains(Flag.SKIP_CACHE_LOAD) || flags.contains(Flag.SKIP_CACHE_STORE)));
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      return this;
   }

   @Override
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

   @Override
   public RpcManager getRpcManager() {
      return rpcManager;
   }

   @Override
   public AdvancedCache<K, V> withFlags(final Flag... flags) {
      if (flags == null || flags.length == 0)
         return this;
      else
         return new DecoratedCache<K, V>(this, flags);
   }

   private Transaction getOngoingTransaction() {
      try {
         Transaction transaction = null;
         if (transactionManager != null) {
            transaction = transactionManager.getTransaction();
            if (transaction == null && config.invocationBatching().enabled()) {
               transaction = batchContainer.getBatchTransaction();
            }
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   private Object executeCommandAndCommitIfNeeded(InvocationContext ctx, VisitableCommand command) {
      final boolean txInjected = isTxInjected(ctx);
      Object result;
      try {
         result = invoker.invoke(ctx, command);
      } catch (RuntimeException e) {
         if (txInjected) tryRollback();
         throw e;
      }

      if (txInjected) {
         if (trace)
            log.tracef("Committing transaction as it was implicit: %s", getOngoingTransaction());
         try {
            transactionManager.commit();
         } catch (Throwable e) {
            log.couldNotCompleteInjectedTransaction(e);
            throw new CacheException("Could not commit implicit transaction", e);
         }
      }

      return result;
   }

   private boolean isTxInjected(InvocationContext ctx) {
      return ctx.isInTxScope() && ((TxInvocationContext) ctx).isImplicitTransaction();
   }

   private void tryRollback() {
      try {
         if (transactionManager != null) transactionManager.rollback();
      } catch (Throwable t) {
         if (trace) log.trace("Could not rollback", t);//best effort
      }
   }

   @Override
   public ClassLoader getClassLoader() {
      ClassLoader cl = config.classLoader();
      if (cl != null)
         // The classloader has been set for this configuration
         return cl;
      else if (globalCfg.classLoader() != null)
         // The classloader is not set for this configuration, and we have a global config
         return globalCfg.classLoader();
      else
         // Return the default CL
         return Thread.currentThread().getContextClassLoader();
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return new DecoratedCache<K, V>(this, classLoader);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      return put(key, value, metadata, null, null);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return replace(key, oldValue, value, metadata, null, null);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return putIfAbsent(key, value, metadata, null, null);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, Metadata metadata) {
      return putAsync(key, value, metadata, null, null);
   }

   @Override
   protected void set(K key, V value) {
      withFlags(Flag.IGNORE_RETURN_VALUES)
            .put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   private void associateImplicitTransactionWithCurrentThread(InvocationContext ctx) throws InvalidTransactionException, SystemException {
      if (isTxInjected(ctx)) {
         Transaction transaction = ((TxInvocationContext) ctx).getTransaction();
         if (transaction == null)
            throw new IllegalStateException("Null transaction not possible!");
         transactionManager.resume(transaction);
      }
   }
}
