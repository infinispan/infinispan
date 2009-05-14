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

import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapCache;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.SizeCommand;
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
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManager;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.Marshaller;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.TimeoutException;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@NonVolatile
public class CacheDelegate<K, V> implements AdvancedCache<K, V>, AtomicMapCache<K, V> {
   protected InvocationContextContainer icc;
   protected CommandsFactory commandsFactory;
   protected InterceptorChain invoker;
   protected Configuration config;
   protected CacheNotifier notifier;
   protected BatchContainer batchContainer;
   protected ComponentRegistry componentRegistry;
   protected TransactionManager transactionManager;
   protected RpcManager rpcManager;
   protected Marshaller marshaller;
   private String name;
   private EvictionManager evictionManager;
   private DataContainer dataContainer;
   private static final Log log = LogFactory.getLog(CacheDelegate.class);
   private CacheManager cacheManager;
   // this is never used here but should be injected - this is a hack to make sure the StateTransferManager is properly constructed if needed.
   private StateTransferManager stateTransferManager;
   // as above for ResponseGenerator
   private ResponseGenerator responseGenerator;
   private long defaultLifespan, defaultMaxIdleTime;

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
                                  Marshaller marshaller, ResponseGenerator responseGenerator,
                                  CacheManager cacheManager, StateTransferManager stateTransferManager) {
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
   }

   public final V putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final boolean remove(Object key, Object value) {
      return remove(key, value, (Flag[]) null);
   }

   public final boolean replace(K key, V oldValue, V newValue) {
      return replace(key, oldValue, newValue, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final V replace(K key, V value) {
      return replace(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final int size() {
      SizeCommand command = commandsFactory.buildSizeCommand();
      return (Integer) invoker.invoke(getInvocationContext(), command);
   }

   public final boolean isEmpty() {
      return size() == 0;
   }

   public final boolean containsKey(Object key) {
      return containsKey(key, (Flag[]) null);
   }

   public final boolean containsValue(Object value) {
      throw new UnsupportedOperationException("Go away");
   }

   public final V get(Object key) {
      return get(key, (Flag[]) null);
   }

   public final V put(K key, V value) {
      return put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final V remove(Object key) {
      return remove(key, (Flag[]) null);
   }

   public final void putAll(Map<? extends K, ? extends V> map) {
      putAll(map, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final void clear() {
      clear((Flag[]) null);
   }

   public Set<K> keySet() {
      throw new UnsupportedOperationException("TODO implement me"); // TODO implement me
   }

   public Collection<V> values() {
      throw new UnsupportedOperationException("TODO implement me"); // TODO implement me
   }

   public Set<Map.Entry<K, V>> entrySet() {
      throw new UnsupportedOperationException("TODO implement me"); // TODO implement me
   }

   public final void putForExternalRead(K key, V value) {
      putForExternalRead(key, value, (Flag[]) null);
   }

   public final void evict(K key) {
      EvictCommand command = commandsFactory.buildEvictCommand(key);
      invoker.invoke(getInvocationContext(), command);
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

   private InvocationContext getInvocationContext() {
      return icc.createInvocationContext();
   }

   public void lock(K key) {
      if (true) throw new UnsupportedOperationException("Not yet implemented");
      if (key == null)
         throw new IllegalArgumentException("Cannot lock null key");
      lock(Collections.singletonList(key));
   }

   public void lock(Collection<? extends K> keys) {
      if (true) throw new UnsupportedOperationException("Not yet implemented");
      if (keys == null || keys.isEmpty())
         throw new IllegalArgumentException("Cannot lock empty list of keys");
      LockControlCommand command = commandsFactory.buildLockControlCommand(keys, true);
      invoker.invoke(getInvocationContext(), command);
   }

   public void unlock(K key) {
      if (true) throw new UnsupportedOperationException("Not yet implemented");
      if (key == null)
         throw new IllegalArgumentException("Cannot unlock null key");
      unlock(Collections.singletonList(key));
   }

   public void unlock(Collection<? extends K> keys) {
      if (true) throw new UnsupportedOperationException("Not yet implemented");
      if (keys == null || keys.isEmpty())
         throw new IllegalArgumentException("Cannot unlock empty list of keys");
      LockControlCommand command = commandsFactory.buildLockControlCommand(keys, false);
      invoker.invoke(getInvocationContext(), command);
   }

   public void start() {
      componentRegistry.start();
      defaultLifespan = config.getExpirationLifespan();
      defaultMaxIdleTime = config.getExpirationMaxIdle();
   }

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

   public final void putForExternalRead(K key, V value, Flag... flags) {
      InvocationContext invocationContext = getInvocationContext();
      if (flags != null) invocationContext.setFlags(flags);
      Transaction ongoingTransaction = null;
      try {
         if (transactionManager != null && (ongoingTransaction = transactionManager.getTransaction()) != null) {
            transactionManager.suspend();
         }
         // if the entry exists then this should be a no-op.
         putIfAbsent(key, value, Flag.FAIL_SILENTLY, Flag.FORCE_ASYNCHRONOUS, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
      }
      catch (Exception e) {
         if (log.isDebugEnabled()) log.debug("Caught exception while doing putForExternalRead()", e);
      }
      finally {
         try {
            if (ongoingTransaction != null) transactionManager.resume(ongoingTransaction);
         }
         catch (Exception e) {
            log.debug("Had problems trying to resume a transaction after putForExternalread()", e);
         }
      }
   }

   public final V put(K key, V value, Flag... flags) {
      return put(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   @SuppressWarnings("unchecked")
   public final V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), maxIdleTimeUnit.toMillis(maxIdleTime));
      return (V) invoker.invoke(ctx, command);
   }

   public final V putIfAbsent(K key, V value, Flag... flags) {
      return putIfAbsent(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   @SuppressWarnings("unchecked")
   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      InvocationContext context = getInvocationContext();
      context.setFlags(flags);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), maxIdleTimeUnit.toMillis(maxIdleTime));
      command.setPutIfAbsent(true);
      return (V) invoker.invoke(context, command);
   }

   public final void putAll(Map<? extends K, ? extends V> map, Flag... flags) {
      putAll(map, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      PutMapCommand command = commandsFactory.buildPutMapCommand(map, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS.toMillis(defaultMaxIdleTime));
      invoker.invoke(ctx, command);
   }

   @SuppressWarnings("unchecked")
   public final V remove(Object key, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, null);
      return (V) invoker.invoke(ctx, command);
   }

   public final boolean remove(Object key, Object oldValue, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, oldValue);
      return (Boolean) invoker.invoke(ctx, command);
   }

   public final void clear(Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ClearCommand command = commandsFactory.buildClearCommand();
      invoker.invoke(ctx, command);
   }

   public final V replace(K k, V v, Flag... flags) {
      return replace(k, v, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   public final boolean replace(K k, V oV, V nV, Flag... flags) {
      return replace(k, oV, nV, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   @SuppressWarnings("unchecked")
   public final V replace(K k, V v, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(k, null, v, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return (V) invoker.invoke(ctx, command);
   }

   public final boolean replace(K k, V oV, V nV, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(k, oV, nV, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return (Boolean) invoker.invoke(ctx, command);
   }

   public final Future<V> putAsync(K key, V value, Flag... flags) {
      return putAsync(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   public final Future<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ctx.setUseFutureReturnType(true);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), maxIdleTimeUnit.toMillis(maxIdleTime));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<V> putIfAbsentAsync(K key, V value, Flag... flags) {
      return putIfAbsentAsync(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   public final Future<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ctx.setUseFutureReturnType(true);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), maxIdleTimeUnit.toMillis(maxIdleTime));
      command.setPutIfAbsent(true);
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<Void> putAllAsync(Map<? extends K, ? extends V> map, Flag... flags) {
      return putAllAsync(map, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   public final Future<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ctx.setUseFutureReturnType(true);
      PutMapCommand command = commandsFactory.buildPutMapCommand(map, MILLISECONDS.toMillis(MILLISECONDS.toMillis(defaultLifespan)), MILLISECONDS.toMillis(MILLISECONDS.toMillis(defaultMaxIdleTime)));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<V> removeAsync(Object key, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ctx.setUseFutureReturnType(true);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, null);
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<Void> clearAsync(Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ctx.setUseFutureReturnType(true);
      ClearCommand command = commandsFactory.buildClearCommand();
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<V> replaceAsync(K k, V v, Flag... flags) {
      return replaceAsync(k, v, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   public final Future<Boolean> replaceAsync(K k, V oV, V nV, Flag... flags) {
      return replaceAsync(k, oV, nV, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS, flags);
   }

   public final Future<V> replaceAsync(K k, V v, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ctx.setUseFutureReturnType(true);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(k, null, v, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<Boolean> replaceAsync(K k, V oV, V nV, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      ctx.setUseFutureReturnType(true);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(k, oV, nV, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final boolean containsKey(Object key, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key);
      Object response = invoker.invoke(ctx, command);
      return response != null;
   }

   @SuppressWarnings("unchecked")
   public final V get(Object key, Flag... flags) {
      InvocationContext ctx = getInvocationContext();
      if (flags != null) ctx.setFlags(flags);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key);
      return (V) invoker.invoke(ctx, command);
   }

   public ComponentStatus getStatus() {
      return componentRegistry.getStatus();
   }

   public boolean startBatch() {
      if (!config.isInvocationBatchingEnabled())
         throw new ConfigurationException("Invocation batching not enabled in current configuration!  Please use the <invocationBatching /> element.");
      return batchContainer.startBatch();
   }

   public void endBatch(boolean successful) {
      if (!config.isInvocationBatchingEnabled())
         throw new ConfigurationException("Invocation batching not enabled in current configuration!  Please use the <invocationBatching /> element.");
      batchContainer.endBatch(successful);
   }

   public String getName() {
      return name;
   }

   public String getVersion() {
      return Version.version;
   }

   @Override
   public String toString() {
      return "Cache '" + name + "'@" + (config.getCacheMode().isClustered() ? getCacheManager().getAddress() : System.identityHashCode(this));
   }

   @SuppressWarnings("unchecked")
   public <AMK, AMV> AtomicMap<AMK, AMV> getAtomicMap(K key) throws ClassCastException {
      Object value = get(key);
      if (value == null) value = AtomicHashMap.newInstance(this, key);
      return ((AtomicHashMap) value).getProxy(this, key, batchContainer, icc);
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

   public CacheManager getCacheManager() {
      return cacheManager;
   }

   public final V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      return put(key, value, lifespan, lifespanUnit, maxIdleTime, idleTimeUnit, (Flag[]) null);
   }

   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      return putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, idleTimeUnit, (Flag[]) null);
   }

   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      PutMapCommand command = commandsFactory.buildPutMapCommand(map, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      invoker.invoke(getInvocationContext(), command);
   }

   public final V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      return replace(key, value, lifespan, lifespanUnit, maxIdleTime, idleTimeUnit, (Flag[]) null);
   }

   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      return replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, idleTimeUnit, (Flag[]) null);
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
   private <X> Future<X> wrapInFuture(final Object retval) {
      if (retval instanceof Future) {
         return (Future<X>) retval;
      } else {
         return new Future<X>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
               return true;
            }

            public boolean isCancelled() {
               return false;
            }

            public boolean isDone() {
               return true;
            }

            @SuppressWarnings("unchecked")
            public X get() throws InterruptedException, ExecutionException {
               return (X) retval;
            }

            public X get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
               return get();
            }
         };
      }
   }

   public final Future<V> putAsync(K key, V value) {
      return putAsync(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putAsync(key, value, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      InvocationContext ctx = getInvocationContext();
      ctx.setUseFutureReturnType(true);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return putAllAsync(data, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return putAllAsync(data, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      InvocationContext ctx = getInvocationContext();
      ctx.setUseFutureReturnType(true);
      PutMapCommand command = commandsFactory.buildPutMapCommand(data, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<Void> clearAsync() {
      InvocationContext ctx = getInvocationContext();
      ctx.setUseFutureReturnType(true);
      ClearCommand command = commandsFactory.buildClearCommand();
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<V> putIfAbsentAsync(K key, V value) {
      return putIfAbsentAsync(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsentAsync(key, value, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      InvocationContext ctx = getInvocationContext();
      ctx.setUseFutureReturnType(true);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      command.setPutIfAbsent(true);
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<V> removeAsync(Object key) {
      InvocationContext ctx = getInvocationContext();
      ctx.setUseFutureReturnType(true);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, null);
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<Boolean> removeAsync(Object key, Object value) {
      InvocationContext ctx = getInvocationContext();
      ctx.setUseFutureReturnType(true);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value);
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final Future<V> replaceAsync(K key, V value) {
      return replaceAsync(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return replaceAsync(key, value, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, (Flag[]) null);
   }

   public final Future<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return replaceAsync(key, oldValue, newValue, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return replaceAsync(key, oldValue, newValue, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final Future<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      InvocationContext ctx = getInvocationContext();
      ctx.setUseFutureReturnType(true);
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, oldValue, newValue, lifespanUnit.toMillis(lifespan), maxIdleUnit.toMillis(maxIdle));
      return wrapInFuture(invoker.invoke(ctx, command));
   }

   public final V put(K key, V value, long lifespan, TimeUnit unit) {
      return put(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsent(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      putAll(map, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public final V replace(K key, V value, long lifespan, TimeUnit unit) {
      return replace(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return replace(key, oldValue, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public AdvancedCache<K, V> getAdvancedCache() {
      return this;
   }

   public void compact() {
      for (InternalCacheEntry e : dataContainer) {
         if (e.getKey() instanceof MarshalledValue) ((MarshalledValue) e.getKey()).compact(true, true);
         if (e.getValue() instanceof MarshalledValue) ((MarshalledValue) e.getValue()).compact(true, true);
      }
   }

   public RpcManager getRpcManager() {
      return rpcManager;
   }
}
