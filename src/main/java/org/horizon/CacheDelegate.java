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
import org.horizon.context.InvocationContext;
import org.horizon.eviction.EvictionManager;
import org.horizon.factories.ComponentRegistry;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.interceptors.InterceptorChain;
import org.horizon.interceptors.base.CommandInterceptor;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.invocation.Options;
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
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value);
      command.setPutIfAbsent(true);
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   public boolean remove(Object key, Object value) {
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value);
      return (Boolean) invoker.invoke(getInvocationContext(), command);
   }

   public boolean replace(K key, V oldValue, V newValue) {
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, oldValue, newValue);
      return (Boolean) invoker.invoke(getInvocationContext(), command);
   }

   @SuppressWarnings("unchecked")
   public V replace(K key, V value) {
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, null, value);
      return (V) invoker.invoke(getInvocationContext(), command);
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
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value);
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   @SuppressWarnings("unchecked")
   public V remove(Object key) {
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, null);
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   public void putAll(Map<? extends K, ? extends V> t) {
      PutMapCommand command = commandsFactory.buildPutMapCommand(t);
      invoker.invoke(getInvocationContext(), command);
   }

   public void clear() {
      ClearCommand command = commandsFactory.buildClearCommand();
      invoker.invoke(getInvocationContext(), command);
   }

   public Set<K> keySet() {
      throw new UnsupportedOperationException("Go away");
   }

   public Collection<V> values() {
      throw new UnsupportedOperationException("Go away");
   }

   public Set<Map.Entry<K, V>> entrySet() {
      throw new UnsupportedOperationException("Go away");
   }

   public void putForExternalRead(K key, V value) {
      Transaction ongoingTransaction = null;
      try {
         if (transactionManager != null && (ongoingTransaction = transactionManager.getTransaction()) != null) {
            transactionManager.suspend();
         }
         // if the entry exists then this should be a no-op.
         putIfAbsent(key, value, Options.FAIL_SILENTLY, Options.FORCE_ASYNCHRONOUS, Options.ZERO_LOCK_ACQUISITION_TIMEOUT);
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

   public void putForExternalRead(K key, V value, Options... options) {
      getInvocationContext().setOptions(options);
      putForExternalRead(key, value);
   }

   public V put(K key, V value, Options... options) {
      getInvocationContext().setOptions(options);
      return put(key, value);
   }

   public V put(K key, V value, long lifespan, TimeUnit unit, Options... options) {
      getInvocationContext().setOptions(options);
      return put(key, value, lifespan, unit);
   }

   public V putIfAbsent(K key, V value, Options... options) {
      getInvocationContext().setOptions(options);
      return putIfAbsent(key, value);
   }

   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit, Options... options) {
      getInvocationContext().setOptions(options);
      return putIfAbsent(key, value, lifespan, unit);
   }

   public void putAll(Map<? extends K, ? extends V> map, Options... options) {
      getInvocationContext().setOptions(options);
      putAll(map);
   }

   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit, Options... options) {
      getInvocationContext().setOptions(options);
      putAll(map, lifespan, unit);
   }

   public V remove(Object key, Options... options) {
      getInvocationContext().setOptions(options);
      return remove(key);
   }

   public boolean remove(Object key, Object oldValue, Options... options) {
      getInvocationContext().setOptions(options);
      return remove(key, oldValue);
   }

   public void clear(Options... options) {
      getInvocationContext().setOptions(options);
      clear();
   }

   public boolean containsKey(Object key, Options... options) {
      getInvocationContext().setOptions(options);
      return containsKey(key);
   }

   public V get(Object key, Options... options) {
      getInvocationContext().setOptions(options);
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
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, unit.toMillis(lifespan));
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   @SuppressWarnings("unchecked")
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, unit.toMillis(lifespan));
      command.setPutIfAbsent(true);
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      PutMapCommand command = commandsFactory.buildPutMapCommand(map, unit.toMillis(lifespan));
      invoker.invoke(getInvocationContext(), command);
   }

   @SuppressWarnings("unchecked")
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, null, value, unit.toMillis(lifespan));
      return (V) invoker.invoke(getInvocationContext(), command);
   }

   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      ReplaceCommand command = commandsFactory.buildReplaceCommand(key, oldValue, value, unit.toMillis(lifespan));
      return (Boolean) invoker.invoke(getInvocationContext(), command);
   }

   public AdvancedCache<K, V> getAdvancedCache() {
      return this;
   }

   public void compact() {
      for (Object key : dataContainer.keySet()) {
         // get the key first, before attempting to serialize stuff since data.get() may deserialize the key if doing
         // a hashcode() or equals().

         Object value = dataContainer.get(key);
         if (key instanceof MarshalledValue) {
            ((MarshalledValue) key).compact(true, true);
         }

         if (value instanceof MarshalledValue) {
            ((MarshalledValue) value).compact(true, true);
         }
      }
   }
}
