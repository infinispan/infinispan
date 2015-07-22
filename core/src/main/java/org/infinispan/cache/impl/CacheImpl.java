package org.infinispan.cache.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.Version;
import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.AbstractInProcessNotifyingFuture;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.NullValueConverter;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.iteration.impl.EntryIterableFromStreamImpl;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.stats.Stats;
import org.infinispan.stats.impl.StatsImpl;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.local.ValueCacheCollection;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.FAIL_SILENTLY;
import static org.infinispan.context.Flag.FORCE_ASYNCHRONOUS;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.PUT_FOR_EXTERNAL_READ;
import static org.infinispan.context.Flag.ZERO_LOCK_ACQUISITION_TIMEOUT;
import static org.infinispan.context.InvocationContextFactory.UNBOUNDED;
import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
@SurvivesRestarts
@MBean(objectName = CacheImpl.OBJECT_NAME, description = "Component that represents an individual cache instance.")
public class CacheImpl<K, V> implements AdvancedCache<K, V> {
   public static final String OBJECT_NAME = "Cache";
   protected InvocationContextContainer icc;
   protected InvocationContextFactory invocationContextFactory;
   protected CommandsFactory commandsFactory;
   protected InterceptorChain invoker;
   protected Configuration config;
   protected CacheNotifier notifier;
   protected BatchContainer batchContainer;
   protected ComponentRegistry componentRegistry;
   protected TransactionManager transactionManager;
   protected RpcManager rpcManager;
   protected StreamingMarshaller marshaller;
   protected Metadata defaultMetadata;
   private final String name;
   private EvictionManager evictionManager;
   private ExpirationManager<K, V> expirationManager;
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
   private AuthorizationManager authorizationManager;
   private PartitionHandlingManager partitionHandlingManager;
   private GlobalConfiguration globalCfg;
   private boolean isClassLoaderInContext;
   private LocalTopologyManager localTopologyManager;

   public CacheImpl(String name) {
      this.name = name;
   }

   @Inject
   public void injectDependencies(EvictionManager evictionManager,
                                  ExpirationManager expirationManager,
                                  InvocationContextFactory invocationContextFactory,
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
                                  @ComponentName(ASYNC_OPERATIONS_EXECUTOR) ExecutorService asyncExecutor,
                                  TransactionTable txTable, RecoveryManager recoveryManager, TransactionCoordinator txCoordinator,
                                  LockManager lockManager,
                                  AuthorizationManager authorizationManager,
                                  GlobalConfiguration globalCfg,
                                  PartitionHandlingManager partitionHandlingManager,
                                  LocalTopologyManager localTopologyManager) {
      this.commandsFactory = commandsFactory;
      this.invoker = interceptorChain;
      this.config = configuration;
      this.notifier = notifier;
      this.componentRegistry = componentRegistry;
      this.transactionManager = transactionManager;
      this.batchContainer = batchContainer;
      this.rpcManager = rpcManager;
      this.evictionManager = evictionManager;
      this.expirationManager = expirationManager;
      this.dataContainer = dataContainer;
      this.marshaller = marshaller;
      this.cacheManager = cacheManager;
      this.invocationContextFactory = invocationContextFactory;
      this.icc = icc;
      this.distributionManager = distributionManager;
      this.asyncExecutor = asyncExecutor;
      this.txTable = txTable;
      this.recoveryManager = recoveryManager;
      this.txCoordinator = txCoordinator;
      this.lockManager = lockManager;
      this.authorizationManager = authorizationManager;
      this.globalCfg = globalCfg;
      this.partitionHandlingManager = partitionHandlingManager;
      this.localTopologyManager = localTopologyManager;
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

   // CacheSupport does not extend AdvancedCache, so it cannot really call up
   // to the cache methods that take Metadata parameter. Since CacheSupport
   // methods are declared final, the easiest is for CacheImpl to stop
   // extending CacheSupport and implement the base methods directly.

   @Override
   public final V put(K key, V value) {
      return put(key, value, defaultMetadata);
   }

   @Override
   public final V put(K key, V value, long lifespan, TimeUnit unit) {
      return put(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsent(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      putAll(map, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final V replace(K key, V value, long lifespan, TimeUnit unit) {
      return replace(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return replace(key, oldValue, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final V putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, defaultMetadata);
   }

   @Override
   public final boolean replace(K key, V oldValue, V newValue) {
      return replace(key, oldValue, newValue, defaultMetadata);
   }

   @Override
   public final V replace(K key, V value) {
      return replace(key, value, defaultMetadata);
   }

   @Override
   public final NotifyingFuture<V> putAsync(K key, V value) {
      return putAsync(key, value, defaultMetadata);
   }

   @Override
   public final NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putAsync(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return putAllAsync(data, defaultMetadata, null, null);
   }

   @Override
   public final NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return putAllAsync(data, lifespan, MILLISECONDS, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return putIfAbsentAsync(key, value, defaultMetadata, null, null);
   }

   @Override
   public final NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsentAsync(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final NotifyingFuture<V> replaceAsync(K key, V value) {
      return replaceAsync(key, value, defaultMetadata, null, null);
   }

   @Override
   public final NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return replaceAsync(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return replaceAsync(key, oldValue, newValue, defaultMetadata, null, null);
   }

   @Override
   public final NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return replaceAsync(key, oldValue, newValue, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final void putAll(Map<? extends K, ? extends V> m) {
      putAll(m, defaultMetadata, null, null);
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
      SizeCommand command = commandsFactory.buildSizeCommand(explicitFlags);
      return (Integer) invoker.invoke(getInvocationContextForRead(explicitClassLoader, UNBOUNDED), command);
   }

   @Override
   public final boolean isEmpty() {
      return isEmpty(null, null);
   }

   final boolean isEmpty(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      // TODO: replace with stream findAny isPresent instead
      try (CloseableIterable<CacheEntry<K, Void>> iterable = filterEntries(AcceptAllKeyValueFilter.getInstance(),
            explicitFlags, explicitClassLoader).converter(NullValueConverter.getInstance())) {
         return !iterable.iterator().hasNext();
      }
   }

   @Override
   public final boolean containsKey(Object key) {
      return containsKey(key, null, null);
   }

   final boolean containsKey(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, 1);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, explicitFlags);
      Object response = invoker.invoke(ctx, command);
      return response != null;
   }

   @Override
   public final boolean containsValue(Object value) {
      assertValueNotNull(value);
      return values().stream().anyMatch(StreamMarshalling.equalityPredicate(value));
   }

   @Override
   public final V get(Object key) {
      return get(key, null, null);
   }

   @SuppressWarnings("unchecked")
   final V get(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, 1);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, explicitFlags);
      return (V) invoker.invoke(ctx, command);
   }

   public final CacheEntry getCacheEntry(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      assertKeyNotNull(key);
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, 1);
      GetCacheEntryCommand command = commandsFactory.buildGetCacheEntryCommand(key, explicitFlags);
      Object ret = invoker.invoke(ctx, command);
      return (CacheEntry) ret;
   }

   @Override
   public final CacheEntry getCacheEntry(Object key) {
      return getCacheEntry(key, null, null);
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      return getAll(keys, null, null);
   }

   public final Map<K, V> getAll(Set<?> keys, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, keys.size());
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, explicitFlags, false);
      Map<K, V> map = (Map<K, V>) invoker.invoke(ctx, command);
      Iterator<Map.Entry<K, V>> entryIterator = map.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Map.Entry<K , V> entry = entryIterator.next();
         if (entry.getValue() == null) {
            entryIterator.remove();
         }
      }
      return map;
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      return getAllCacheEntries(keys, null, null);
   }

   public final Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys,
         EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, keys.size());
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, explicitFlags, true);
      Map<K, CacheEntry<K, V>> map = (Map<K, CacheEntry<K, V>>) invoker.invoke(ctx, command);
      Iterator<Map.Entry<K, CacheEntry<K, V>>> entryIterator = map.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Map.Entry<K , CacheEntry<K, V>> entry = entryIterator.next();
         if (entry.getValue() == null) {
            entryIterator.remove();
         }
      }
      return map;
   }

   @Override
   public EntryIterable<K, V> filterEntries(KeyValueFilter<? super K, ? super V> filter) {
      return filterEntries(filter, null, null);
   }

   protected EntryIterable<K, V> filterEntries(KeyValueFilter<? super K, ? super V> filter, EnumSet<Flag> explicitFlags,
                                               ClassLoader explicitClassLoader) {
      return new EntryIterableFromStreamImpl<>(filter, explicitFlags, this);
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      return getGroup(groupName, null, null);
   }

   protected final Map<K, V> getGroup(String groupName, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, UNBOUNDED);
      return Collections.unmodifiableMap(internalGetGroup(groupName, explicitFlags, explicitClassLoader, ctx));
   }

   private Map<K, V> internalGetGroup(String groupName, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader,
                                      InvocationContext ctx) {
      GetKeysInGroupCommand command = commandsFactory.buildGetKeysInGroupCommand(explicitFlags, groupName);
      //noinspection unchecked
      return (Map<K, V>) invoker.invoke(ctx, command);
   }

   @Override
   public void removeGroup(String groupName) {
      removeGroup(groupName, null, null);
   }

   protected final void removeGroup(String groupName, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      if (transactionManager == null) {
         nonTransactionalRemoveGroup(groupName, explicitFlags, explicitClassLoader);
      } else {
         transactionalRemoveGroup(groupName, explicitFlags, explicitClassLoader);
      }
   }

   private void transactionalRemoveGroup(String groupName, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      final boolean onGoingTransaction = getOngoingTransaction() != null;
      if (!onGoingTransaction) {
         tryBegin();
      }
      try {
         InvocationContext context = getInvocationContextWithImplicitTransaction(false, explicitClassLoader, UNBOUNDED);
         Map<K, V> keys = internalGetGroup(groupName, explicitFlags, explicitClassLoader, context);
         EnumSet<Flag> removeFlags = explicitFlags == null ? EnumSet.noneOf(Flag.class) : EnumSet.copyOf(explicitFlags);
         removeFlags.add(IGNORE_RETURN_VALUES);
         for (K key : keys.keySet()) {
            removeInternal(key, removeFlags, context);
         }
         if (!onGoingTransaction) {
            tryCommit();
         }
      } catch (RuntimeException e) {
         if (!onGoingTransaction) {
            tryRollback();
         }
         throw e;
      }
   }

   private void nonTransactionalRemoveGroup(String groupName, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext context = getInvocationContextForRead(explicitClassLoader, UNBOUNDED);
      Map<K, V> keys = internalGetGroup(groupName, explicitFlags, explicitClassLoader, context);
      EnumSet<Flag> removeFlags = explicitFlags == null ? EnumSet.noneOf(Flag.class) : EnumSet.copyOf(explicitFlags);
      removeFlags.add(IGNORE_RETURN_VALUES);
      for (K key : keys.keySet()) {
         //a new context is needed for remove since in the non-owners, the command is sent to the primary owner to be
         //executed. If the context is already populated, it throws a ClassCastException because the wrapForRemove is
         //not invoked.
         remove(key, removeFlags, explicitClassLoader);
      }
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
      clear(null, null);
   }

   @Override
   public final void clear() {
      clear(null, null);
   }

   final void clear(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      final Transaction tx = suspendOngoingTransactionIfExists();
      try {
         InvocationContext context = invocationContextFactory.createClearNonTxInvocationContext();
         setInvocationContextClassLoader(context, explicitClassLoader);
         ClearCommand command = commandsFactory.buildClearCommand(explicitFlags);
         invoker.invoke(context, command);
      } finally {
         resumePreviousOngoingTransaction(tx, true, "Had problems trying to resume a transaction after clear()");
      }
   }

   @Override
   public CacheSet<K> keySet() {
      return keySet(null, null);
   }

   @SuppressWarnings("unchecked")
   CacheSet<K> keySet(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, UNBOUNDED);
      KeySetCommand command = commandsFactory.buildKeySetCommand(explicitFlags);
      return (CacheSet<K>) invoker.invoke(ctx, command);
   }

   @Override
   public CacheCollection<V> values() {
      return new ValueCacheCollection<>(this, cacheEntrySet());
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      return cacheEntrySet(null, null);
   }

   @SuppressWarnings("unchecked")
   CacheSet<CacheEntry<K, V>> cacheEntrySet(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, UNBOUNDED);
      EntrySetCommand command = commandsFactory.buildEntrySetCommand(explicitFlags);
      return (CacheSet<CacheEntry<K, V>>) invoker.invoke(ctx, command);
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return entrySet(null, null);
   }

   @SuppressWarnings("unchecked")
   CacheSet<Map.Entry<K, V>> entrySet(EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      InvocationContext ctx = getInvocationContextForRead(explicitClassLoader, UNBOUNDED);
      EntrySetCommand command = commandsFactory.buildEntrySetCommand(explicitFlags);
      return (CacheSet<Map.Entry<K, V>>) invoker.invoke(ctx, command);
   }

   @Override
   public final void putForExternalRead(K key, V value) {
      putForExternalRead(key, value, null, null);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit) {
      putForExternalRead(key, value, lifespan, lifespanUnit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
       .lifespan(lifespan, lifespanUnit)
       .maxIdle(maxIdleTime, idleTimeUnit).build();
      putForExternalRead(key, value, metadata);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      Metadata merged = applyDefaultMetadata(metadata);
      putForExternalRead(key, value, merged, null, null);
   }

   final void putForExternalRead(K key, V value, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      putForExternalRead(key, value, defaultMetadata, explicitFlags, explicitClassLoader);
   }

   final void putForExternalRead(K key, V value, Metadata metadata, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      Transaction ongoingTransaction = null;
      try {
         ongoingTransaction = suspendOngoingTransactionIfExists();

         EnumSet<Flag> flags = EnumSet.of(FAIL_SILENTLY, FORCE_ASYNCHRONOUS, ZERO_LOCK_ACQUISITION_TIMEOUT, PUT_FOR_EXTERNAL_READ);
         if (explicitFlags != null && !explicitFlags.isEmpty()) {
            flags.addAll(explicitFlags);
         }

         // if the entry exists then this should be a no-op.
         putIfAbsent(key, value, metadata, flags, explicitClassLoader);
      } catch (Exception e) {
         if (log.isDebugEnabled()) log.debug("Caught exception while doing putForExternalRead()", e);
      }
      finally {
         resumePreviousOngoingTransaction(ongoingTransaction, true, "Had problems trying to resume a transaction after putForExternalRead()");
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
      InvocationContext ctx = invocationContextFactory.createSingleKeyNonTxInvocationContext();
      return setInvocationContextClassLoader(ctx, explicitClassLoader);
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
   public void addListener(Object listener, KeyFilter<? super K> filter) {
      notifier.addListener(listener, filter);
   }

   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter,
                               CacheEventConverter<? super K, ? super V, C> converter) {
      notifier.addListener(listener, filter, converter);
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
            ? invocationContextFactory.createSingleKeyNonTxInvocationContext()
            : invocationContextFactory.createInvocationContext(true, keyCount);
      return setInvocationContextClassLoader(ctx, explicitClassLoader);
   }

   private InvocationContext getInvocationContextForRead(ClassLoader explicitClassLoader, int keyCount) {
      if (config.transaction().transactionMode().isTransactional()) {
         Transaction transaction = getOngoingTransaction();
         //if we are in the scope of a transaction than return a transactional context. This is relevant e.g.
         // FORCE_WRITE_LOCK is used on read operations - in that case, the lock is held for the the transaction's
         // lifespan (when in tx scope) vs. call lifespan (when not in tx scope).
         if (transaction != null)
            return getInvocationContext(transaction, explicitClassLoader, false);
      }
      InvocationContext result = invocationContextFactory.createInvocationContext(false, keyCount);
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
            transaction = tryBegin();
            txInjected = true;
         }
         invocationContext = getInvocationContext(transaction, explicitClassLoader, txInjected);
      } else {
         invocationContext = getInvocationContextForWrite(explicitClassLoader, keyCount, isPutForExternalRead);
      }
      return invocationContext;
   }

   private boolean isPutForExternalRead(EnumSet<Flag> explicitFlags) {
      return explicitFlags != null && explicitFlags.contains(PUT_FOR_EXTERNAL_READ);
   }

   private InvocationContext getInvocationContext(Transaction tx, ClassLoader explicitClassLoader,
                                                  boolean implicitTransaction) {
      InvocationContext ctx = invocationContextFactory.createInvocationContext(tx, implicitTransaction);
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
      LockControlCommand command = commandsFactory.buildLockControlCommand(keys, explicitFlags);
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
      defaultMetadata = new EmbeddedMetadata.Builder()
            .lifespan(config.expiration().lifespan()).maxIdle(config.expiration().maxIdle()).build();
      // Context only needs to ship ClassLoader if marshalling will be required
      isClassLoaderInContext = config.clustering().cacheMode().isClustered()
            || config.persistence().usingStores()
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
   public ExpirationManager getExpirationManager() {
      return expirationManager;
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
   public AuthorizationManager getAuthorizationManager() {
      return authorizationManager;
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
   public AvailabilityMode getAvailability() {
      return partitionHandlingManager.getAvailabilityMode();
   }

   @Override
   public void setAvailability(AvailabilityMode availability) {
      if (localTopologyManager != null) {
         try {
            localTopologyManager.setCacheAvailability(getName(), availability);
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
   }

   @ManagedAttribute(
         description = "Returns the cache availability",
         displayName = "Cache availability",
         dataType = DataType.TRAIT,
         writable = true
   )
   public String getCacheAvailability() {
      return getAvailability().toString();
   }

   public void setCacheAvailability(String availabilityString) throws Exception {
      setAvailability(AvailabilityMode.valueOf(availabilityString));
   }

   @Override
   public boolean startBatch() {
      if (!config.invocationBatching().enabled()) {
         throw new CacheConfigurationException("Invocation batching not enabled in current configuration! Please enable it.");
      }
      return batchContainer.startBatch();
   }

   @Override
   public void endBatch(boolean successful) {
      if (!config.invocationBatching().enabled()) {
         throw new CacheConfigurationException("Invocation batching not enabled in current configuration! Please enable it.");
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
      String name = getName().equals(BasicCacheContainer.DEFAULT_CACHE_NAME) ? "Default Cache" : getName();
      return name + "(" + getCacheConfiguration().clustering().cacheMode().toString().toLowerCase() + ")";
   }

   /**
    * Returns the version of Infinispan.
    */
   @ManagedAttribute(
         description = "Returns the version of Infinispan",
         displayName = "Infinispan version",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public String getVersion() {
      return Version.getVersion();
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
      return new TransactionXaAdapter(txTable, recoveryManager, txCoordinator, commandsFactory, rpcManager, null, config, name, partitionHandlingManager);
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
      command.setValueMatcher(ValueMatcher.MATCH_EXPECTED);
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
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<V> returnValue = asyncExecutor.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               V retval = putInternal(key, value, metadata, explicitFlags, ctx);
               try {
                  result.notifyDone(retval);
                  log.trace("Finished notifying");
               } catch (Throwable e) {
                  log.trace("Exception while notifying the future", e);
               }
               return retval;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
                  log.trace("Finished notifying");
               } catch (Throwable e2) {
                  log.trace("Exception while notifying the future", e2);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
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
      final NotifyingFutureImpl<Void> result = new NotifyingFutureImpl<Void>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, data.size());
      Future<Void> returnValue = asyncExecutor.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               putAllInternal(data, metadata, explicitFlags, ctx);
               try {
                  result.notifyDone(null);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return null;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<Void> clearAsync() {
      return clearAsync(null, null);
   }

   final NotifyingFuture<Void> clearAsync(final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureImpl<Void> result = new NotifyingFutureImpl<>();
      Future<Void> returnValue = asyncExecutor.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               clear(explicitFlags, explicitClassLoader);
               try {
                  result.notifyDone(null);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return null;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
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
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<V> returnValue = asyncExecutor.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               V retval = putIfAbsentInternal(key, value, metadata, explicitFlags, ctx);
               try {
                  result.notifyDone(retval);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return retval;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<V> removeAsync(Object key) {
      return removeAsync(key, null, null);
   }

   final NotifyingFuture<V> removeAsync(final Object key, final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<V> returnValue = asyncExecutor.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               V retval = removeInternal(key, explicitFlags, ctx);
               try {
                  result.notifyDone(retval);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return retval;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
      return result;
   }

   @Override
   public final NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return removeAsync(key, value, null, null);
   }

   final NotifyingFuture<Boolean> removeAsync(final Object key, final Object value, final EnumSet<Flag> explicitFlags, final ClassLoader explicitClassLoader) {
      final NotifyingFutureImpl<Boolean> result = new NotifyingFutureImpl<Boolean>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<Boolean> returnValue = asyncExecutor.submit(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               Boolean retval = removeInternal(key, value, explicitFlags, ctx);
               try {
                  result.notifyDone(retval);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return retval;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
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
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<V> returnValue = asyncExecutor.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               V retval = replaceInternal(key, value, metadata, explicitFlags, ctx);
               try {
                  result.notifyDone(retval);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return retval;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
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
      final NotifyingFutureImpl<Boolean> result = new NotifyingFutureImpl<Boolean>();
      final InvocationContext ctx = getInvocationContextWithImplicitTransactionForAsyncOps(false, explicitClassLoader, 1);
      Future<Boolean> returnValue = asyncExecutor.submit(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            try {
               associateImplicitTransactionWithCurrentThread(ctx);
               Boolean retval = replaceInternal(key, oldValue, newValue, metadata, explicitFlags, ctx);
               try {
                  result.notifyDone(retval);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return retval;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
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
         final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();

         Callable<V> c = new Callable<V>() {
            @Override
            public V call() throws Exception {
               try {
                  V retval = get(key, appliedFlags, explicitClassLoader);
                  try {
                     result.notifyDone(retval);
                  } catch (Throwable t) {
                     log.trace("Error when notifying", t);
                  }
                  return retval;
               } catch (Exception e) {
                  try {
                     result.notifyException(e);
                  } catch (Throwable t) {
                     log.trace("Error when notifying", t);
                  }
                  throw e;
               }
            }
         };
         result.setFuture(asyncExecutor.submit(c));
         return result;
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
      boolean hasCacheLoaderConfig = !config.persistence().stores().isEmpty();
      return !hasCacheLoaderConfig
            || (flags != null && (flags.contains(Flag.SKIP_CACHE_LOAD) || flags.contains(Flag.SKIP_CACHE_STORE)));
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      return this;
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
         tryCommit();
      }

      return result;
   }

   private boolean isTxInjected(InvocationContext ctx) {
      return ctx.isInTxScope() && ((TxInvocationContext) ctx).isImplicitTransaction();
   }

   private Transaction tryBegin() {
      if (transactionManager == null) {
         return null;
      }
      try {
         transactionManager.begin();
         final Transaction transaction = getOngoingTransaction();
         if (log.isTraceEnabled()) {
            log.tracef("Implicit transaction started! Transaction: %s", transaction);
         }
         return transaction;
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException("Unable to begin implicit transaction.", e);
      }
   }

   private void tryRollback() {
      try {
         if (transactionManager != null) transactionManager.rollback();
      } catch (Throwable t) {
         if (trace) log.trace("Could not rollback", t);//best effort
      }
   }

   private void tryCommit() {
      if (transactionManager == null) {
         return;
      }
      if (trace)
         log.tracef("Committing transaction as it was implicit: %s", getOngoingTransaction());
      try {
         transactionManager.commit();
      } catch (Throwable e) {
         log.couldNotCompleteInjectedTransaction(e);
         throw new CacheException("Could not commit implicit transaction", e);
      }
   }

   @Override
   public ClassLoader getClassLoader() {
      ClassLoader classLoader = globalCfg.classLoader();
      return classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return new DecoratedCache<K, V>(this, classLoader);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      Metadata merged = applyDefaultMetadata(metadata);
      return put(key, value, merged, null, null);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      Metadata merged = applyDefaultMetadata(metadata);
      putAll(map, merged, null, null);
   }

   private Metadata applyDefaultMetadata(Metadata metadata) {
      Metadata.Builder builder = metadata.builder();
      return builder != null ? builder.merge(defaultMetadata).build() : metadata;
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      Metadata merged = applyDefaultMetadata(metadata);
      return replace(key, value, merged, null, null);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      Metadata merged = applyDefaultMetadata(metadata);
      return replace(key, oldValue, value, merged, null, null);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      Metadata merged = applyDefaultMetadata(metadata);
      return putIfAbsent(key, value, merged, null, null);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, Metadata metadata) {
      Metadata merged = applyDefaultMetadata(metadata);
      return putAsync(key, value, merged, null, null);
   }

   private void associateImplicitTransactionWithCurrentThread(InvocationContext ctx) throws InvalidTransactionException, SystemException {
      if (isTxInjected(ctx)) {
         Transaction transaction = ((TxInvocationContext) ctx).getTransaction();
         if (transaction == null)
            throw new IllegalStateException("Null transaction not possible!");
         transactionManager.resume(transaction);
      }
   }

   private Transaction suspendOngoingTransactionIfExists() {
      final Transaction tx = getOngoingTransaction();
      if (tx != null) {
         try {
            transactionManager.suspend();
         } catch (SystemException e) {
            throw new CacheException("Unable to suspend transaction.", e);
         }
      }
      return tx;
   }

   private void resumePreviousOngoingTransaction(Transaction transaction, boolean failSilently, String failMessage) {
      if (transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            if (failSilently) {
               if (log.isDebugEnabled()) {
                  log.debug(failMessage);
               }
            } else {
               throw new CacheException(failMessage, e);
            }
         }
      }
   }

   @ManagedAttribute(
         description = "Returns the cache configuration in form of properties",
         displayName = "Cache configuration properties",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public Properties getConfigurationAsProperties() {
      return new PropertyFormatter().format(config);
   }
}
