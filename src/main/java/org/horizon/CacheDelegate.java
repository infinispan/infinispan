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
package org.horizon;

import org.horizon.atomic.AtomicHashMap;
import org.horizon.atomic.AtomicMap;
import org.horizon.atomic.AtomicMapCache;
import org.horizon.batch.BatchContainer;
import org.horizon.commands.CommandsFactory;
import org.horizon.commands.read.GetKeyValueCommand;
import org.horizon.commands.read.SizeCommand;
import org.horizon.commands.write.ClearCommand;
import org.horizon.commands.write.EvictCommand;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.commands.write.PutMapCommand;
import org.horizon.commands.write.RemoveCommand;
import org.horizon.commands.write.ReplaceCommand;
import org.horizon.config.Configuration;
import org.horizon.config.ConfigurationException;
import org.horizon.container.DataContainer;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.context.InvocationContext;
import org.horizon.eviction.EvictionManager;
import org.horizon.factories.ComponentRegistry;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.interceptors.InterceptorChain;
import org.horizon.interceptors.base.CommandInterceptor;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.invocation.Flag;
import org.horizon.lifecycle.ComponentStatus;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.manager.CacheManager;
import org.horizon.marshall.MarshalledValue;
import org.horizon.marshall.Marshaller;
import org.horizon.notifications.cachelistener.CacheNotifier;
import org.horizon.remoting.RPCManager;
import org.horizon.statetransfer.StateTransferManager;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@NonVolatile
public class CacheDelegate<K, V> implements AdvancedCache<K, V>, AtomicMapCache<K, V> {
   protected InvocationContextContainer invocationContextContainer;
   protected CommandsFactory commandsFactory;
   protected InterceptorChain invoker;
   protected Configuration config;
   protected CacheNotifier notifier;
   protected BatchContainer batchContainer;
   protected ComponentRegistry componentRegistry;
   protected TransactionManager transactionManager;
   protected RPCManager rpcManager;
   protected Marshaller marshaller;
   private String name;
   private EvictionManager evictionManager;
   private DataContainer dataContainer;
   private static final Log log = LogFactory.getLog(CacheDelegate.class);
   private CacheManager cacheManager;
   // this is never used here but should be injected - this is a hack to make sure the StateTransferManager is properly constructed if needed.
   private StateTransferManager stateTransferManager;
   private long defaultLifespan, defaultMaxIdleTime;

   public CacheDelegate(String name) {
      this.name = name;
   }

   @Inject
   public void injectDependencies(EvictionManager evictionManager,
                                  InvocationContextContainer invocationContextContainer,
                                  CommandsFactory commandsFactory,
                                  InterceptorChain interceptorChain,
                                  Configuration configuration,
                                  CacheNotifier notifier,
                                  ComponentRegistry componentRegistry,
                                  TransactionManager transactionManager,
                                  BatchContainer batchContainer,
                                  RPCManager rpcManager, DataContainer dataContainer,
                                  Marshaller marshaller,
                                  CacheManager cacheManager, StateTransferManager stateTransferManager) {
      this.invocationContextContainer = invocationContextContainer;
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
      this.stateTransferManager = stateTransferManager;
   }

   @SuppressWarnings("unchecked")
   public V putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public boolean remove(Object key, Object value) {
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value);
      return (Boolean) invoker.invoke(getInvocationContext(), command);
   }

   public boolean replace(K key, V oldValue, V newValue) {
      return replace(key, oldValue, newValue, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @SuppressWarnings("unchecked")
   public V replace(K key, V value) {
      return replace(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public int size() {
      SizeCommand command = commandsFactory.buildSizeCommand();
      return (Integer) invoker.invoke(getInvocationContext(), command);
   }

   public boolean isEmpty() {
      SizeCommand command = commandsFactory.buildSizeCommand();
      int size = (Integer) invoker.invoke(getInvocationContext(), command);
      return size == 0;
   }

   public boolean containsKey(Object key) {
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key);
      Object response = invoker.invoke(getInvocationContext(), command);
      return response != null;
   }

   public boolean containsValue(Object value) {
      throw new UnsupportedOperationException("Go away");
   }

   @SuppressWarnings("unchecked")
   public V get(Object key) {
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key);
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   @SuppressWarnings("unchecked")
   public V put(K key, V value) {
      return put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @SuppressWarnings("unchecked")
   public V remove(Object key) {
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, null);
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   public void putAll(Map<? extends K, ? extends V> map) {
      putAll(map, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public void clear() {
      ClearCommand command = commandsFactory.buildClearCommand();
      invoker.invoke(getInvocationContext(), command);
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

   public void putForExternalRead(K key, V value) {
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

   public void evict(K key) {
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
      return invocationContextContainer.get();
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

   public void putForExternalRead(K key, V value, Flag... flags) {
      getInvocationContext().setFlags(flags);
      putForExternalRead(key, value);
   }

   public V put(K key, V value, Flag... flags) {
      getInvocationContext().setFlags(flags);
      return put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      getInvocationContext().setFlags(flags);
      return put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public V putIfAbsent(K key, V value, Flag... flags) {
      getInvocationContext().setFlags(flags);
      return putIfAbsent(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      getInvocationContext().setFlags(flags);
      return putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public void putAll(Map<? extends K, ? extends V> map, Flag... flags) {
      getInvocationContext().setFlags(flags);
      putAll(map, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      getInvocationContext().setFlags(flags);
      putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public V remove(Object key, Flag... flags) {
      getInvocationContext().setFlags(flags);
      return remove(key);
   }

   public boolean remove(Object key, Object oldValue, Flag... flags) {
      getInvocationContext().setFlags(flags);
      return remove(key, oldValue);
   }

   public void clear(Flag... flags) {
      getInvocationContext().setFlags(flags);
      clear();
   }

   public boolean containsKey(Object key, Flag... flags) {
      getInvocationContext().setFlags(flags);
      return containsKey(key);
   }

   public V get(Object key, Flag... flags) {
      getInvocationContext().setFlags(flags);
      return get(key);
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
      return dataContainer == null ? super.toString() : dataContainer.toString();
   }

   public AtomicMap getAtomicMap(K key) throws ClassCastException {
      Object value = get(key);
      if (value == null) value = AtomicHashMap.newInstance(this, key);
      return ((AtomicHashMap) value).getProxy(this, key, batchContainer, invocationContextContainer);
   }

   @SuppressWarnings("unchecked")
   public <AMK, AMV> AtomicMap<AMK, AMV> getAtomicMap(K key, Class<AMK> atomicMapKeyType, Class<AMV> atomicMapValueType) throws ClassCastException {
      return getAtomicMap(key);
   }

   public BatchContainer getBatchContainer() {
      return batchContainer;
   }

   public InvocationContextContainer getInvocationContextContainer() {
      return invocationContextContainer;
   }

   public DataContainer getDataContainer() {
      return dataContainer;
   }

   public CacheManager getCacheManager() {
      return cacheManager;
   }

   @SuppressWarnings("unchecked")
   public final V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   @SuppressWarnings("unchecked")
   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      command.setPutIfAbsent(true);
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      PutMapCommand command = commandsFactory.buildPutMapCommand(map, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      invoker.invoke(getInvocationContext(), command);
   }

   @SuppressWarnings("unchecked")
   public final V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, null, value, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, oldValue, value, lifespanUnit.toMillis(lifespan), idleTimeUnit.toMillis(maxIdleTime));
      return (Boolean) invoker.invoke(getInvocationContext(), command);
   }

   @SuppressWarnings("unchecked")
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return put(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @SuppressWarnings("unchecked")
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsent(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      putAll(map, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @SuppressWarnings("unchecked")
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return replace(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
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

   public RPCManager getRpcManager() {
      return rpcManager;
   }
}
