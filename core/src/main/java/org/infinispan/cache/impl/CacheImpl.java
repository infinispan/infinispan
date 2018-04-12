package org.infinispan.cache.impl;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.FAIL_SILENTLY;
import static org.infinispan.context.Flag.FORCE_ASYNCHRONOUS;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.PUT_FOR_EXTERNAL_READ;
import static org.infinispan.context.Flag.ZERO_LOCK_ACQUISITION_TIMEOUT;
import static org.infinispan.context.InvocationContextFactory.UNBOUNDED;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.LockedStream;
import org.infinispan.Version;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.atomic.impl.ApplyDelta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.functions.MergeFunction;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.filter.KeyFilter;
import org.infinispan.functional.impl.Params;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
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
import org.infinispan.notifications.cachelistener.ListenerHolder;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.stats.Stats;
import org.infinispan.stats.impl.StatsImpl;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.LockedStreamImpl;
import org.infinispan.stream.impl.TxLockedStreamImpl;
import org.infinispan.stream.impl.local.ValueCacheCollection;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   private static final Log log = LogFactory.getLog(CacheImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   public static final String OBJECT_NAME = "Cache";
   private static final long PFER_FLAGS = EnumUtil.bitSetOf(FAIL_SILENTLY, FORCE_ASYNCHRONOUS, ZERO_LOCK_ACQUISITION_TIMEOUT, PUT_FOR_EXTERNAL_READ, IGNORE_RETURN_VALUES);

   @Inject protected InvocationContextFactory invocationContextFactory;
   @Inject protected CommandsFactory commandsFactory;
   @Inject protected AsyncInterceptorChain invoker;
   @Inject protected Configuration config;
   @Inject protected CacheNotifier notifier;
   @Inject protected BatchContainer batchContainer;
   @Inject protected ComponentRegistry componentRegistry;
   @Inject protected TransactionManager transactionManager;
   @Inject protected RpcManager rpcManager;
   @Inject protected StreamingMarshaller marshaller;
   @Inject private EvictionManager evictionManager;
   @Inject private ExpirationManager<K, V> expirationManager;
   @Inject private DataContainer dataContainer;
   @Inject private EmbeddedCacheManager cacheManager;
   @Inject private LockManager lockManager;
   @Inject private DistributionManager distributionManager;
   @Inject private TransactionTable txTable;
   @Inject private AuthorizationManager authorizationManager;
   @Inject private PartitionHandlingManager partitionHandlingManager;
   @Inject private GlobalConfiguration globalCfg;
   @Inject private LocalTopologyManager localTopologyManager;

   protected Metadata defaultMetadata;
   private final String name;
   private volatile boolean stopping = false;
   private boolean transactional;
   private boolean batchingEnabled;
   private final ContextBuilder contextBuilder = this::getInvocationContextWithImplicitTransaction;
   private final ContextBuilder expiredContextBuilder = i -> this.getInvocationContextWithImplicitTransaction(i, true);
   private final ContextBuilder pferContextBuilder = this::putForExternalReadContext;

   public CacheImpl(String name) {
      this.name = name;
   }

   // This should rather be a @Start method but CacheImpl may be not an actual component but a delegate
   // of EncoderCache. ATM there's not method to invoke @Start method, just wireDependencies
   @Inject
   public void preStart() {
      // We have to do this before start, since some components may start before the actual cache and they
      // have to have access to the default metadata on some operations
      defaultMetadata = new EmbeddedMetadata.Builder()
            .lifespan(config.expiration().lifespan()).maxIdle(config.expiration().maxIdle()).build();
      transactional = config.transaction().transactionMode().isTransactional();
      batchingEnabled = config.invocationBatching().enabled();
   }

   private void assertKeyNotNull(Object key) {
      requireNonNull(key, "Null keys are not supported!");
   }

   private void assertValueNotNull(Object value) {
      requireNonNull(value, "Null values are not supported!");
   }

   void assertKeyValueNotNull(Object key, Object value) {
      assertKeyNotNull(key);
      assertValueNotNull(value);
   }

   private void assertFunctionNotNull(Object function) {
      requireNonNull(function, "Null functions are not supported!");
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
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return compute(key, remappingFunction, false);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return computeInternal(key, remappingFunction, false, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return compute(key, remappingFunction, true);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return computeInternal(key, remappingFunction, true, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   private V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent) {
      return computeInternal(key, remappingFunction, computeIfPresent, applyDefaultMetadata(defaultMetadata), addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   private V computeInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent, Metadata metadata, long flags) {
      return computeInternal(key, remappingFunction, computeIfPresent, metadata, flags, contextBuilder);
   }

   V computeInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent,
         Metadata metadata, long flags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertFunctionNotNull(remappingFunction);
      ComputeCommand command = commandsFactory.buildComputeCommand(key, remappingFunction, computeIfPresent, metadata, flags);
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }

   @Override
   public final V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      return computeIfAbsent(key, mappingFunction, defaultMetadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return computeIfAbsentInternal(key, mappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            contextBuilder);
   }

   V computeIfAbsentInternal(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata, long flags,
         ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertFunctionNotNull(mappingFunction);
      ComputeIfAbsentCommand command = commandsFactory.buildComputeIfAbsentCommand(key, mappingFunction, metadata, flags);
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return mergeInternal(key, value, remappingFunction, defaultMetadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            contextBuilder);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(defaultMetadata.maxIdle(), MILLISECONDS).build();
      return mergeInternal(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            contextBuilder);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return mergeInternal(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            contextBuilder);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return mergeInternal(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            contextBuilder);
   }

   V mergeInternal(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata,
         long flags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertValueNotNull(value);
      assertFunctionNotNull(remappingFunction);
      ReadWriteKeyCommand<K, V, V> command = commandsFactory.buildReadWriteKeyCommand(key,
            new MergeFunction<>(value, remappingFunction, metadata), Params.fromFlagsBitSet(flags),
            getKeyDataConversion(), getValueDataConversion());
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }


   @Override
   public final CompletableFuture<V> putAsync(K key, V value) {
      return putAsync(key, value, defaultMetadata);
   }

   @Override
   public final CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putAsync(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return putAllAsync(data, defaultMetadata);
   }

   @Override
   public final CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return putAllAsync(data, lifespan, MILLISECONDS, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return putIfAbsentAsync(key, value, defaultMetadata);
   }

   @Override
   public final CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsentAsync(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> replaceAsync(K key, V value) {
      return replaceAsync(key, value, defaultMetadata);
   }

   @Override
   public final CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return replaceAsync(key, value, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return replaceAsync(key, oldValue, newValue, defaultMetadata);
   }

   @Override
   public final CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return replaceAsync(key, oldValue, newValue, lifespan, unit, defaultMetadata.maxIdle(), MILLISECONDS);
   }

   @Override
   public final void putAll(Map<? extends K, ? extends V> m) {
      putAll(m, defaultMetadata);
   }

   @Override
   public final boolean remove(Object key, Object value) {
      return remove(key, value, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final boolean remove(Object key, Object value, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value, explicitFlags);
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }

   @Override
   public final int size() {
      return size(EnumUtil.EMPTY_BIT_SET);
   }

   final int size(long explicitFlags) {
      SizeCommand command = commandsFactory.buildSizeCommand(explicitFlags);
      return (Integer) invoker.invoke(invocationContextFactory.createInvocationContext(false, UNBOUNDED), command);
   }

   @Override
   public final boolean isEmpty() {
      return isEmpty(EnumUtil.EMPTY_BIT_SET);
   }

   final boolean isEmpty(long explicitFlags) {
      return !entrySet(explicitFlags).stream().anyMatch(StreamMarshalling.alwaysTruePredicate());
   }

   @Override
   public final boolean containsKey(Object key) {
      return containsKey(key, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, 1));
   }

   final boolean containsKey(Object key, long explicitFlags, InvocationContext ctx) {
      return get(key, explicitFlags, ctx) != null;
   }

   @Override
   public final boolean containsValue(Object value) {
      assertValueNotNull(value);
      return values().stream().anyMatch(StreamMarshalling.equalityPredicate(value));
   }

   @Override
   public final V get(Object key) {
      return get(key, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, 1));
   }

   @SuppressWarnings("unchecked")
   final V get(Object key, long explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, explicitFlags);
      return (V) invoker.invoke(ctx, command);
   }

   final CacheEntry getCacheEntry(Object key, long explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      GetCacheEntryCommand command = commandsFactory.buildGetCacheEntryCommand(key, explicitFlags);
      Object ret = invoker.invoke(ctx, command);
      return (CacheEntry) ret;
   }

   @Override
   public final CacheEntry getCacheEntry(Object key) {
      return getCacheEntry(key, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, 1));
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key) {
      return getCacheEntryAsync(key, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, 1));
   }

   final CompletableFuture<CacheEntry<K,V>> getCacheEntryAsync(Object key, long explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      GetCacheEntryCommand command = commandsFactory.buildGetCacheEntryCommand(key, explicitFlags);
      return invoker.invokeAsync(ctx, command).thenApply(CacheEntry.class::cast);
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      return getAll(keys, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, keys.size()));
   }

   final Map<K, V> getAll(Set<?> keys, long explicitFlags, InvocationContext ctx) {
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, explicitFlags, false);
      Map<K, V> map = (Map<K, V>) invoker.invoke(ctx, command);
      return dropNullEntries(map);
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return getAllAsync(keys, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, keys.size()));
   }

   final CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys, long explicitFlags, InvocationContext ctx) {
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, explicitFlags, false);
      return invoker.invokeAsync(ctx, command).thenApply(map -> dropNullEntries((Map<K, V>) map));
   }

   private Map<K, V> dropNullEntries(Map<K, V> map) {
      Iterator<Entry<K, V>> entryIterator = map.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Entry<K, V> entry = entryIterator.next();
         if (entry.getValue() == null) {
            entryIterator.remove();
         }
      }
      return map;
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      return getAllCacheEntries(keys, EnumUtil.EMPTY_BIT_SET,
            invocationContextFactory.createInvocationContext(false, keys.size()));
   }

   public final Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys,
                                                            long explicitFlags, InvocationContext ctx) {
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, explicitFlags, true);
      Map<K, CacheEntry<K, V>> map = (Map<K, CacheEntry<K, V>>) invoker.invoke(ctx, command);
      Iterator<Map.Entry<K, CacheEntry<K, V>>> entryIterator = map.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Map.Entry<K, CacheEntry<K, V>> entry = entryIterator.next();
         if (entry.getValue() == null) {
            entryIterator.remove();
         }
      }
      return map;
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      return getGroup(groupName, EnumUtil.EMPTY_BIT_SET);
   }

   final Map<K, V> getGroup(String groupName, long explicitFlags) {
      InvocationContext ctx = invocationContextFactory.createInvocationContext(false, UNBOUNDED);
      return Collections.unmodifiableMap(internalGetGroup(groupName, explicitFlags, ctx));
   }

   private Map<K, V> internalGetGroup(String groupName, long explicitFlagsBitSet, InvocationContext ctx) {
      GetKeysInGroupCommand command = commandsFactory.buildGetKeysInGroupCommand(explicitFlagsBitSet, groupName);
      //noinspection unchecked
      return (Map<K, V>) invoker.invoke(ctx, command);
   }

   @Override
   public void removeGroup(String groupName) {
      removeGroup(groupName, EnumUtil.EMPTY_BIT_SET);
   }

   final void removeGroup(String groupName, long explicitFlags) {
      if (!transactional) {
         nonTransactionalRemoveGroup(groupName, explicitFlags);
      } else {
         transactionalRemoveGroup(groupName, explicitFlags);
      }
   }

   private void transactionalRemoveGroup(String groupName, long explicitFlagsBitSet) {
      final boolean onGoingTransaction = getOngoingTransaction(true) != null;
      if (!onGoingTransaction) {
         tryBegin();
      }
      try {
         InvocationContext context = getInvocationContextWithImplicitTransaction(UNBOUNDED);
         Map<K, V> keys = internalGetGroup(groupName, explicitFlagsBitSet, context);
         long removeFlags = addIgnoreReturnValuesFlag(explicitFlagsBitSet);
         for (K key : keys.keySet()) {
            executeCommandAndCommitIfNeeded(i -> context, createRemoveCommand(key, removeFlags), UNBOUNDED);
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

   private void nonTransactionalRemoveGroup(String groupName, long explicitFlags) {
      InvocationContext context = invocationContextFactory.createInvocationContext(false, UNBOUNDED);
      Map<K, V> keys = internalGetGroup(groupName, explicitFlags, context);
      long removeFlags = addIgnoreReturnValuesFlag(explicitFlags);
      for (K key : keys.keySet()) {
         //a new context is needed for remove since in the non-owners, the command is sent to the primary owner to be
         //executed. If the context is already populated, it throws a ClassCastException because the wrapForRemove is
         //not invoked.
         assertKeyNotNull(key);
         executeCommandAndCommitIfNeeded(contextBuilder, createRemoveCommand(key, removeFlags), 1);
      }
   }

   @Override
   public final V remove(Object key) {
      return remove(key, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final V remove(Object key, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      RemoveCommand command = createRemoveCommand(key, explicitFlags);
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }

   private RemoveCommand createRemoveCommand(Object key, long explicitFlags) {
      long flags = addUnsafeFlags(explicitFlags);
      return commandsFactory.buildRemoveCommand(key, null, flags);
   }

   @Override
   public CompletableFuture<Void> removeLifespanExpired(K key, V value, Long lifespan) {
      RemoveExpiredCommand command = commandsFactory.buildRemoveExpiredCommand(key, value, lifespan);
      // Remove expired returns a boolean - just ignore it, the caller just needs to know that the expired
      // entry is removed when this completes
      CompletableFuture<Boolean> completableFuture = performRemoveExpiredCommand(command);
      return completableFuture.thenApply(b -> null);
   }

   @Override
   public CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value) {
      RemoveExpiredCommand command = commandsFactory.buildRemoveExpiredCommand(key, value);
      return performRemoveExpiredCommand(command);
   }

   private CompletableFuture<Boolean> performRemoveExpiredCommand(RemoveExpiredCommand command) {
      Transaction ongoingTransaction = null;
      try {
         ongoingTransaction = suspendOngoingTransactionIfExists();
         return executeCommandAndCommitIfNeededAsync(expiredContextBuilder, command, 1);
      } catch (Exception e) {
         if (log.isDebugEnabled()) log.debug("Caught exception while doing removeExpired()", e);
         return CompletableFutures.completedExceptionFuture(e);
      } finally {
         resumePreviousOngoingTransaction(ongoingTransaction, true, "Had problems trying to resume a transaction after removeExpired()");
      }
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> encoderClass) {
      if (encoderClass == IdentityEncoder.class) {
         return this;
      }
      return new EncoderCache<>(this, getKeyDataConversion().withEncoding(encoderClass), getValueDataConversion().withEncoding(encoderClass));
   }

   @Override
   public AdvancedCache<?, ?> withKeyEncoding(Class<? extends Encoder> encoderClass) {
      if (encoderClass == IdentityEncoder.class) {
         return this;
      }
      return new EncoderCache<>(this, getKeyDataConversion().withEncoding(encoderClass), getValueDataConversion());
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> keyEncoderClass, Class<? extends Encoder> valueEncoderClass) {
      if (keyEncoderClass == IdentityEncoder.class && valueEncoderClass == IdentityEncoder.class) {
         return this;
      }
      return new EncoderCache<>(this, getKeyDataConversion().withEncoding(keyEncoderClass), getValueDataConversion().withEncoding(valueEncoderClass));
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapperClass) {
      if (wrapperClass == IdentityWrapper.class) {
         return this;
      }
      return new EncoderCache<>(this, getKeyDataConversion().withWrapping(wrapperClass), getValueDataConversion().withWrapping(wrapperClass));
   }

   @Override
   public AdvancedCache<K, V> withMediaType(String keyMediaType, String valueMediaType) {
      MediaType km = MediaType.fromString(keyMediaType);
      MediaType vm = MediaType.fromString(valueMediaType);
      return new EncoderCache<>(this, getKeyDataConversion().withRequestMediaType(km), getValueDataConversion().withRequestMediaType(vm));
   }


   @Override
   public Encoder getKeyEncoder() {
      return getKeyDataConversion().getEncoder();
   }

   @Override
   public Encoder getValueEncoder() {
      return getValueDataConversion().getEncoder();
   }

   @Override
   public Wrapper getKeyWrapper() {
      return getKeyDataConversion().getWrapper();
   }

   @Override
   public Wrapper getValueWrapper() {
      return getValueDataConversion().getWrapper();
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapperClass, Class<? extends Wrapper> valueWrapperClass) {
      if (keyWrapperClass == IdentityWrapper.class && valueWrapperClass == IdentityWrapper.class) {
         return this;
      }
      return new EncoderCache<>(this, getKeyDataConversion().withWrapping(keyWrapperClass), getValueDataConversion().withWrapping(valueWrapperClass));
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return DataConversion.IDENTITY_KEY;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return DataConversion.IDENTITY_VALUE;
   }

   @ManagedOperation(
         description = "Clears the cache",
         displayName = "Clears the cache", name = "clear"
   )
   public final void clearOperation() {
      clear(EnumUtil.EMPTY_BIT_SET);
   }

   @Override
   public final void clear() {
      clear(EnumUtil.EMPTY_BIT_SET);
   }

   final void clear(long explicitFlags) {
      final Transaction tx = suspendOngoingTransactionIfExists();
      try {
         InvocationContext context = invocationContextFactory.createClearNonTxInvocationContext();
         ClearCommand command = commandsFactory.buildClearCommand(explicitFlags);
         invoker.invoke(context, command);
      } finally {
         resumePreviousOngoingTransaction(tx, true, "Had problems trying to resume a transaction after clear()");
      }
   }

   @Override
   public CacheSet<K> keySet() {
      return keySet(EnumUtil.EMPTY_BIT_SET);
   }

   @SuppressWarnings("unchecked")
   CacheSet<K> keySet(long explicitFlags) {
      InvocationContext ctx = invocationContextFactory.createInvocationContext(false, UNBOUNDED);
      KeySetCommand command = commandsFactory.buildKeySetCommand(explicitFlags);
      return (CacheSet<K>) invoker.invoke(ctx, command);
   }

   @Override
   public CacheCollection<V> values() {
      return values(EnumUtil.EMPTY_BIT_SET);
   }

   CacheCollection<V> values(long explicitFlags) {
      return new ValueCacheCollection<>(this, cacheEntrySet(explicitFlags));
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      return cacheEntrySet(EnumUtil.EMPTY_BIT_SET);
   }

   @Override
   public LockedStream<K, V> lockedStream() {
      if (transactional) {
         if (config.transaction().lockingMode() == LockingMode.OPTIMISTIC) {
            throw new UnsupportedOperationException("Method lockedStream is not supported in OPTIMISTIC transactional caches!");
         }
         return new TxLockedStreamImpl<>(transactionManager, cacheEntrySet().stream(), config.locking().lockAcquisitionTimeout(), TimeUnit.MILLISECONDS);
      }
      return new LockedStreamImpl<>(cacheEntrySet().stream(), config.locking().lockAcquisitionTimeout(), TimeUnit.MILLISECONDS);
   }

   @SuppressWarnings("unchecked")
   CacheSet<CacheEntry<K, V>> cacheEntrySet(long explicitFlags) {
      return cacheEntrySet(explicitFlags, invocationContextFactory.createInvocationContext(false, UNBOUNDED));
   }

   @SuppressWarnings("unchecked")
   CacheSet<CacheEntry<K, V>> cacheEntrySet(long explicitFlags, InvocationContext ctx) {
      EntrySetCommand command = commandsFactory.buildEntrySetCommand(explicitFlags);
      return (CacheSet<CacheEntry<K, V>>) invoker.invoke(ctx, command);
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return entrySet(EnumUtil.EMPTY_BIT_SET);
   }

   @SuppressWarnings("unchecked")
   CacheSet<Map.Entry<K, V>> entrySet(long explicitFlags) {
      InvocationContext ctx = invocationContextFactory.createInvocationContext(false, UNBOUNDED);
      EntrySetCommand command = commandsFactory.buildEntrySetCommand(explicitFlags);
      return (CacheSet<Map.Entry<K, V>>) invoker.invoke(ctx, command);
   }

   @Override
   public final void putForExternalRead(K key, V value) {
      putForExternalRead(key, value, EnumUtil.EMPTY_BIT_SET);
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
      putForExternalRead(key, value, merged, EnumUtil.EMPTY_BIT_SET);
   }

   final void putForExternalRead(K key, V value, long explicitFlags) {
      putForExternalRead(key, value, defaultMetadata, explicitFlags);
   }

   final void putForExternalRead(K key, V value, Metadata metadata, long explicitFlags) {
      Transaction ongoingTransaction = null;
      try {
         ongoingTransaction = suspendOngoingTransactionIfExists();
         // if the entry exists then this should be a no-op.
         putIfAbsent(key, value, metadata, EnumUtil.mergeBitSets(PFER_FLAGS, explicitFlags), pferContextBuilder);
      } catch (Exception e) {
         if (log.isDebugEnabled()) log.debug("Caught exception while doing putForExternalRead()", e);
      } finally {
         resumePreviousOngoingTransaction(ongoingTransaction, true, "Had problems trying to resume a transaction after putForExternalRead()");
      }
   }

   @Override
   public final void evict(K key) {
      evict(key, EnumUtil.EMPTY_BIT_SET);
   }

   final void evict(K key, long explicitFlags) {
      assertKeyNotNull(key);
      if (!config.memory().isEvictionEnabled()) {
         log.evictionDisabled(name);
      }
      InvocationContext ctx = createSingleKeyNonTxInvocationContext();
      EvictCommand command = commandsFactory.buildEvictCommand(key, explicitFlags);
      invoker.invoke(ctx, command);
   }

   private InvocationContext createSingleKeyNonTxInvocationContext() {
      return invocationContextFactory.createSingleKeyNonTxInvocationContext();
   }

   @Override
   public Configuration getCacheConfiguration() {
      return config;
   }

   @Override
   public void addListener(Object listener) {
      notifier.addListener(listener);
   }

   void addListener(ListenerHolder listenerHolder) {
      notifier.addListener(listenerHolder, null, null, null);
   }

   <C> void addListener(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter,
                        CacheEventConverter<? super K, ? super V, C> converter) {
      notifier.addListener(listenerHolder, filter, converter, null);
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

   @Override
   public <C> void addFilteredListener(Object listener,
                                       CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      notifier.addFilteredListener(listener, filter, converter, filterAnnotations);
   }

   <C> void addFilteredListener(ListenerHolder listener,
                                CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                Set<Class<? extends Annotation>> filterAnnotations) {
      notifier.addFilteredListener(listener, filter, converter, filterAnnotations);
   }

   /**
    * Creates an invocation context with an implicit transaction if it is required. An implicit transaction is created
    * if there is no current transaction and autoCommit is enabled.
    * @param keyCount how many keys are expected to be changed
    * @return the invocation context
    */
   InvocationContext getInvocationContextWithImplicitTransaction(int keyCount) {
      return getInvocationContextWithImplicitTransaction(keyCount, false);
   }

   /**
    * Same as {@link #getInvocationContextWithImplicitTransaction(int)} except if <b>forceCreateTransaction</b>
    * is true then autoCommit doesn't have to be enabled to start a new transaction.
    * @param keyCount how many keys are expected to be changed
    * @param forceCreateTransaction if true then a transaction is always started if there wasn't one
    * @return the invocation context
    */
   InvocationContext getInvocationContextWithImplicitTransaction(int keyCount, boolean forceCreateTransaction) {
      InvocationContext invocationContext;
      boolean txInjected = false;
      if (transactional) {
         Transaction transaction = getOngoingTransaction(true);
         if (transaction == null && (forceCreateTransaction || config.transaction().autoCommit())) {
            transaction = tryBegin();
            txInjected = true;
         }
         invocationContext = invocationContextFactory.createInvocationContext(transaction, txInjected);
      } else {
         invocationContext = invocationContextFactory.createInvocationContext(true, keyCount);
      }
      return invocationContext;
   }

   private InvocationContext putForExternalReadContext(int keyCount) {
      return transactional ?
            invocationContextFactory.createSingleKeyNonTxInvocationContext() :
            invocationContextFactory.createInvocationContext(true, keyCount);
   }

   @Override
   public boolean lock(K... keys) {
      assertKeyNotNull(keys);
      return lock(Arrays.asList(keys), EnumUtil.EMPTY_BIT_SET);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return lock(keys, EnumUtil.EMPTY_BIT_SET);
   }

   boolean lock(Collection<? extends K> keys, long flagsBitSet) {
      if (!transactional)
         throw new UnsupportedOperationException("Calling lock() on non-transactional caches is not allowed");

      if (keys == null || keys.isEmpty()) {
         throw new IllegalArgumentException("Cannot lock empty list of keys");
      }
      InvocationContext ctx = invocationContextFactory.createInvocationContext(true, UNBOUNDED);
      LockControlCommand command = commandsFactory.buildLockControlCommand(keys, flagsBitSet);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(command.getKeyLockOwner());
      }
      return (Boolean) invoker.invoke(ctx, command);
   }

   @Override
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire) {
      if (locksToAcquire == null || locksToAcquire.length == 0) {
         throw new IllegalArgumentException("Cannot lock empty list of keys");
      } else if (locksToAcquire.length != 1) {
         throw new IllegalArgumentException("Only one lock is permitted.");
      } else if (!Objects.equals(locksToAcquire[0], deltaAwareValueKey)) {
         throw new IllegalArgumentException("The delta aware key and locked key must match.");
      }
      assertKeyNotNull(deltaAwareValueKey);
      InvocationContext ctx = invocationContextFactory.createInvocationContext(true, 1);
      ReadWriteKeyValueCommand<K, Object, Object, Object> command = createApplyDelta(deltaAwareValueKey, delta, FlagBitSets.IGNORE_RETURN_VALUES);
      checkLockOwner(ctx, command);
      invoker.invoke(ctx, command);
   }

   private ReadWriteKeyValueCommand<K, Object, Object, Object> createApplyDelta(K deltaAwareValueKey, Delta delta, long explicitFlags) {
      ReadWriteKeyValueCommand<K, Object, Object, Object> command = commandsFactory.buildReadWriteKeyValueCommand(
            deltaAwareValueKey, delta, new ApplyDelta<>(marshaller), Params.create(), getKeyDataConversion(), getValueDataConversion());
      command.setFlagsBitSet(explicitFlags);
      return command;
   }

   @Override
   @ManagedOperation(
         description = "Starts the cache.",
         displayName = "Starts cache."
   )
   public void start() {
      componentRegistry.start();
      if (log.isDebugEnabled()) log.debugf("Started cache %s on %s", getName(), getCacheManager().getAddress());
   }

   @Override
   @ManagedOperation(
         description = "Stops the cache.",
         displayName = "Stops cache."
   )
   public void stop() {
      performImmediateShutdown();
   }

   @Override
   @ManagedOperation(
         description = "Shuts down the cache across the cluster",
         displayName = "Clustered cache shutdown"
   )
   public void shutdown() {
      if (log.isDebugEnabled())
         log.debugf("Shutting down cache %s on %s", getName(), getCacheManager().getAddress());

      synchronized (this) {
         if (!stopping && componentRegistry.getStatus() == ComponentStatus.RUNNING) {
            stopping = true;
            requestClusterWideShutdown();
         }
      }
   }

   private void requestClusterWideShutdown() {
      // If the cache is clustered, perform a cluster-wide shutdown, otherwise do it immediately
      if (config.clustering().cacheMode().isClustered()) {
         try {
            localTopologyManager.cacheShutdown(name);
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
      performImmediateShutdown();
   }

   private void performImmediateShutdown() {
      if (log.isDebugEnabled())
         log.debugf("Stopping cache %s on %s", getName(), getCacheManager().getAddress());
      componentRegistry.stop();
   }

   public void performGracefulShutdown() {
      // Perform any orderly shutdown operations here
      PassivationManager passivationManager = componentRegistry.getComponent(PassivationManager.class);
      if (passivationManager != null) {
         passivationManager.passivateAll();
      }
   }

   @Override
   public List<CommandInterceptor> getInterceptorChain() {
      List<AsyncInterceptor> interceptors = invoker.getInterceptors();
      ArrayList<CommandInterceptor> list = new ArrayList<>(interceptors.size());
      interceptors.forEach(interceptor -> {
         if (interceptor instanceof CommandInterceptor) {
            list.add((CommandInterceptor) interceptor);
         }
      });
      return list;
   }

   @Override
   public void addInterceptor(CommandInterceptor i, int position) {
      invoker.addInterceptor(i, position);
   }

   @Override
   public AsyncInterceptorChain getAsyncInterceptorChain() {
      return invoker;
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
   public AdvancedCache<K, V> lockAs(Object lockOwner) {
      return new DecoratedCache<>(this, requireNonNull(lockOwner, "lockOwner can't be null"));
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

   @ManagedAttribute(
         description = "Returns whether cache rebalancing is enabled",
         displayName = "Cache rebalacing",
         dataType = DataType.TRAIT,
         writable = true
   )
   public boolean isRebalancingEnabled() {
      if (localTopologyManager != null) {
         try {
            return localTopologyManager.isCacheRebalancingEnabled(getName());
         } catch (Exception e) {
            throw new CacheException(e);
         }
      } else {
         return false;
      }
   }

   public void setRebalancingEnabled(boolean enabled) {
      if (localTopologyManager != null) {
         try {
            localTopologyManager.setCacheRebalancingEnabled(getName(), enabled);
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
   }

   @Override
   public boolean startBatch() {
      if (!batchingEnabled) {
         throw log.invocationBatchingNotEnabled();
      }
      return batchContainer.startBatch();
   }

   @Override
   public void endBatch(boolean successful) {
      if (!batchingEnabled) {
         throw log.invocationBatchingNotEnabled();
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
      return "Cache '" + name + "'@" + (config != null && config.clustering().cacheMode().isClustered() ? getCacheManager().getAddress() : Util.hexIdHashCode(getCacheManager()));
   }

   @Override
   public BatchContainer getBatchContainer() {
      return batchContainer;
   }

   @Override
   public InvocationContextContainer getInvocationContextContainer() {
      return null;
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
      return new TransactionXaAdapter((XaTransactionTable) txTable);
   }

   @Override
   public final V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return put(key, value, metadata);
   }

   final V put(K key, V value, Metadata metadata, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      DataWriteCommand command;
      // CACHE_MODE_LOCAL is used for example when preloading - the entry has empty changeset (unless it's better
      // defined) and that wouldn't store the value properly.
      if (value instanceof Delta) {
         command = createApplyDelta(key, (Delta) value, explicitFlags);
      } else if (value instanceof DeltaAware && (explicitFlags & FlagBitSets.CACHE_MODE_LOCAL) == 0) {
         command = createApplyDelta(key, ((DeltaAware) value).delta(), explicitFlags);
      } else {
         command = createPutCommand(key, value, metadata, explicitFlags);
      }
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }

   @SuppressWarnings("unchecked")
   private PutKeyValueCommand createPutCommand(K key, V value, Metadata metadata, long explicitFlags) {
      long flags = addUnsafeFlags(explicitFlags);
      Metadata merged = applyDefaultMetadata(metadata);
      return commandsFactory.buildPutKeyValueCommand(key, value, merged, flags);
   }

   private long addIgnoreReturnValuesFlag(long flagBitSet) {
      return EnumUtil.mergeBitSets(flagBitSet, FlagBitSets.IGNORE_RETURN_VALUES);
   }

   private long addUnsafeFlags(long flagBitSet) {
      return config.unsafe().unreliableReturnValues() ? addIgnoreReturnValuesFlag(flagBitSet) :
            flagBitSet;
   }

   @Override
   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return putIfAbsent(key, value, metadata, EnumUtil.EMPTY_BIT_SET);
   }

   private V putIfAbsent(K key, V value, Metadata metadata, long explicitFlags) {
      return putIfAbsent(key, value, metadata, explicitFlags, contextBuilder);
   }

   final V putIfAbsent(K key, V value, Metadata metadata, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      DataWriteCommand command;
      if (value instanceof Delta) {
         command = createApplyDelta(key, (Delta) value, explicitFlags);
      } else if (value instanceof DeltaAware && (explicitFlags & FlagBitSets.CACHE_MODE_LOCAL) == 0) {
         command = createApplyDelta(key, ((DeltaAware) value).delta(), explicitFlags);
      } else {
         command = createPutIfAbsentCommand(key, value, metadata, explicitFlags);
      }
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }

   private PutKeyValueCommand createPutIfAbsentCommand(K key, V value, Metadata metadata, long explicitFlags) {
      long flags = addUnsafeFlags(explicitFlags);
      Metadata merged = applyDefaultMetadata(metadata);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, merged, flags);
      command.setPutIfAbsent(true);
      command.setValueMatcher(ValueMatcher.MATCH_EXPECTED);
      return command;
   }

   @Override
   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      putAll(map, metadata);
   }

   final void putAll(Map<? extends K, ? extends V> map, Metadata metadata, long explicitFlags, ContextBuilder contextBuilder) {
      // Vanilla PutMapCommand returns previous values; add IGNORE_RETURN_VALUES as the API will drop the return value.
      // Interceptors are free to clear this flag if appropriate (since interceptors are the only consumers of the
      // return value).
      explicitFlags = EnumUtil.mergeBitSets(explicitFlags, FlagBitSets.IGNORE_RETURN_VALUES);
      PutMapCommand command = createPutAllCommand(map, metadata, explicitFlags);
      executeCommandAndCommitIfNeeded(contextBuilder, command, map.size());
   }

   public final Map<K, V> getAndPutAll(Map<? extends K, ? extends V> map) {
      return getAndPutAll(map, defaultMetadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final Map<K, V> getAndPutAll(Map<? extends K, ? extends V> map, Metadata metadata, long explicitFlags,
         ContextBuilder contextBuilder) {
      PutMapCommand command = createPutAllCommand(map, metadata, explicitFlags);
      return dropNullEntries(executeCommandAndCommitIfNeeded(contextBuilder, command, map.size()));
   }

   private PutMapCommand createPutAllCommand(Map<? extends K, ? extends V> map, Metadata metadata, long explicitFlags) {
      InfinispanCollections.assertNotNullEntries(map, "map");
      Metadata merged = applyDefaultMetadata(metadata);
      return commandsFactory.buildPutMapCommand(map, merged, explicitFlags);
   }

   @Override
   public final V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return replace(key, value, metadata);
   }

   final V replace(K key, V value, Metadata metadata, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      ReplaceCommand command = createReplaceCommand(key, value, metadata, explicitFlags);
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }

   private ReplaceCommand createReplaceCommand(K key, V value, Metadata metadata, long explicitFlags) {
      long flags = addUnsafeFlags(explicitFlags);
      Metadata merged = applyDefaultMetadata(metadata);
      return commandsFactory.buildReplaceCommand(key, null, value, merged, flags);
   }

   @Override
   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return replace(key, oldValue, value, metadata);
   }

   final boolean replace(K key, V oldValue, V value, Metadata metadata, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      assertValueNotNull(oldValue);
      ReplaceCommand command = createReplaceConditionalCommand(key, oldValue, value, metadata, explicitFlags);
      return executeCommandAndCommitIfNeeded(contextBuilder, command, 1);
   }

   private ReplaceCommand createReplaceConditionalCommand(K key, V oldValue, V value, Metadata metadata, long explicitFlags) {
      Metadata merged = applyDefaultMetadata(metadata);
      return commandsFactory.buildReplaceCommand(key, oldValue, value, merged, explicitFlags);
   }

   @Override
   public final CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return putAsync(key, value, metadata);
   }

   final CompletableFuture<V> putAsync(final K key, final V value, final Metadata metadata, final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      PutKeyValueCommand command = createPutCommand(key, value, metadata, explicitFlags);
      return executeCommandAndCommitIfNeededAsync(contextBuilder, command, 1);
   }

   @Override
   public final CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return putAllAsync(data, metadata);
   }

   @Override
   public final CompletableFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final Metadata metadata) {
      return putAllAsync(data, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final CompletableFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final Metadata metadata,
                                             long explicitFlags, ContextBuilder contextBuilder) {
      explicitFlags = EnumUtil.mergeBitSets(explicitFlags, FlagBitSets.IGNORE_RETURN_VALUES);
      PutMapCommand command = createPutAllCommand(data, metadata, explicitFlags);
      return executeCommandAndCommitIfNeededAsync(contextBuilder, command, data.size());
   }

   @Override
   public final CompletableFuture<Void> clearAsync() {
      return clearAsync(EnumUtil.EMPTY_BIT_SET);
   }

   final CompletableFuture<Void> clearAsync(final long explicitFlags) {
      InvocationContext context = invocationContextFactory.createClearNonTxInvocationContext();
      ClearCommand command = commandsFactory.buildClearCommand(explicitFlags);
      return invoker.invokeAsync(context, command).thenApply(nil -> null);
   }

   @Override
   public final CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return putIfAbsentAsync(key, value, metadata);
   }

   @Override
   public final CompletableFuture<V> putIfAbsentAsync(final K key, final V value, final Metadata metadata) {
      return putIfAbsentAsync(key, value, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final CompletableFuture<V> putIfAbsentAsync(final K key, final V value, final Metadata metadata,
                                               final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      PutKeyValueCommand command = createPutIfAbsentCommand(key, value, metadata, explicitFlags);
      return executeCommandAndCommitIfNeededAsync(contextBuilder, command, 1);
   }

   @Override
   public final CompletableFuture<V> removeAsync(Object key) {
      return removeAsync(key, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final CompletableFuture<V> removeAsync(final Object key, final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      RemoveCommand command = createRemoveCommand(key, explicitFlags);
      return executeCommandAndCommitIfNeededAsync(contextBuilder, command, 1);
   }

   @Override
   public final CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return removeAsync(key, value, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final CompletableFuture<Boolean> removeAsync(final Object key, final Object value, final long explicitFlags,
                                                ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value, explicitFlags);
      return executeCommandAndCommitIfNeededAsync(contextBuilder, command, 1);
   }

   @Override
   public final CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return replaceAsync(key, value, metadata);
   }

   @Override
   public final CompletableFuture<V> replaceAsync(final K key, final V value, final Metadata metadata) {
      return replaceAsync(key, value, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final CompletableFuture<V> replaceAsync(final K key, final V value, final Metadata metadata,
                                           final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      ReplaceCommand command = createReplaceCommand(key, value, metadata, explicitFlags);
      return executeCommandAndCommitIfNeededAsync(contextBuilder, command, 1);
   }

   @Override
   public final CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return replaceAsync(key, oldValue, newValue, metadata);
   }

   @Override
   public final CompletableFuture<Boolean> replaceAsync(final K key, final V oldValue, final V newValue, final Metadata metadata) {
      return replaceAsync(key, oldValue, newValue, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   final CompletableFuture<Boolean> replaceAsync(final K key, final V oldValue, final V newValue,
                                                 final Metadata metadata, final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, newValue);
      assertValueNotNull(oldValue);
      ReplaceCommand command = createReplaceConditionalCommand(key, oldValue, newValue, metadata, explicitFlags);
      return executeCommandAndCommitIfNeededAsync(contextBuilder, command, 1);
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return getAsync(key, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, 1));
   }

   @SuppressWarnings("unchecked")
   CompletableFuture<V> getAsync(final K key, final long explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, explicitFlags);
      return (CompletableFuture<V>) invoker.invokeAsync(ctx, command);
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
         return new DecoratedCache<>(this, flags);
   }

   @Override
   public AdvancedCache<K, V> withFlags(Collection<Flag> flags) {
      if (flags == null || flags.isEmpty())
         return this;
      else
         return new DecoratedCache<>(this, flags);
   }

   @Override
   public AdvancedCache<K, V> noFlags() {
      return this;
   }

   @Override
   public AdvancedCache<K, V> transform(Function<AdvancedCache<K, V>, ? extends AdvancedCache<K, V>> transformation) {
      return transformation.apply(this);
   }

   @Override
   public AdvancedCache<K, V> withSubject(Subject subject) {
      return this; // NO-OP
   }

   private Transaction getOngoingTransaction(boolean includeBatchTx) {
      try {
         Transaction transaction = null;
         if (transactionManager != null) {
            transaction = transactionManager.getTransaction();
            if (includeBatchTx && transaction == null && batchingEnabled) {
               transaction = batchContainer.getBatchTransaction();
            }
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   /**
    * Executes a {@link VisitableCommand}.
    * <p>
    * This method creates the {@link InvocationContext} using {@link ContextBuilder} and initializes it.
    * <p>
    * If the cache is transactional and no transaction is running, a transaction is created and committed for the command (i.e. an injected transaction)
    */
   private <T> T executeCommandAndCommitIfNeeded(ContextBuilder contextBuilder, VisitableCommand command,
         int keyCount) {
      InvocationContext ctx = contextBuilder.create(keyCount);
      checkLockOwner(ctx, command);
      //noinspection unchecked
      return isTxInjected(ctx) ?
            (T) executeCommandWithInjectedTx(ctx, command) :
            (T) invoker.invoke(ctx, command);
   }

   /**
    * The asynchronous method of {@link #executeCommandAndCommitIfNeeded(ContextBuilder, VisitableCommand, int)}.
    */
   private <T> CompletableFuture<T> executeCommandAndCommitIfNeededAsync(ContextBuilder contextBuilder, VisitableCommand command, int keyCount) {
      InvocationContext ctx = contextBuilder.create(keyCount);
      checkLockOwner(ctx, command);
      //noinspection unchecked
      return isTxInjected(ctx) ?
            executeCommandAsyncWithInjectedTx(ctx, command) :
            (CompletableFuture<T>) invoker.invokeAsync(ctx, command);
   }

   /**
    * Executes the {@link VisitableCommand} with an injected transaction.
    */
   private Object executeCommandWithInjectedTx(InvocationContext ctx, VisitableCommand command) {
      final Object result;
      try {
         result = invoker.invoke(ctx, command);
      } catch (Throwable e) {
         tryRollback();
         throw e;
      }
      tryCommit();
      return result;
   }

   /**
    * The asynchronous method of {@link #executeCommandWithInjectedTx(InvocationContext, VisitableCommand)}
    */
   private <T> CompletableFuture<T> executeCommandAsyncWithInjectedTx(InvocationContext ctx, VisitableCommand command) {
      CompletableFuture<T> cf;
      final Transaction implicitTransaction;
      try {
         // interceptors must not access thread-local transaction anyway
         implicitTransaction = transactionManager.suspend();
         assert implicitTransaction != null;
         //noinspection unchecked
         cf = (CompletableFuture<T>) invoker.invokeAsync(ctx, command);
      } catch (SystemException e) {
         throw new CacheException("Cannot suspend implicit transaction", e);
      } catch (Throwable e) {
         tryRollback();
         throw e;
      }
      return cf.handle((result, throwable) -> {
         if (throwable != null) {
            try {
               implicitTransaction.rollback();
            } catch (SystemException e) {
               log.trace("Could not rollback", e);
               throwable.addSuppressed(e);
            }
            throw CompletableFutures.asCompletionException(throwable);
         }
         try {
            implicitTransaction.commit();
         } catch (Exception e) {
            log.couldNotCompleteInjectedTransaction(e);
            throw CompletableFutures.asCompletionException(e);
         }
         return result;
      });
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
         final Transaction transaction = getOngoingTransaction(true);
         if (trace) {
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
         log.tracef("Committing transaction as it was implicit: %s", getOngoingTransaction(true));
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
      return this;
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      return put(key, value, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      putAll(map, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   private Metadata applyDefaultMetadata(Metadata metadata) {
      if (metadata == null) {
         return defaultMetadata;
      }
      Metadata.Builder builder = metadata.builder();
      return builder != null ? builder.merge(defaultMetadata).build() : metadata;
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      return replace(key, value, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return replace(key, oldValue, value, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return putIfAbsent(key, value, metadata, EnumUtil.EMPTY_BIT_SET);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return putAsync(key, value, metadata, EnumUtil.EMPTY_BIT_SET, contextBuilder);
   }

   private Transaction suspendOngoingTransactionIfExists() {
      final Transaction tx = getOngoingTransaction(false);
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

   private static void checkLockOwner(InvocationContext context, VisitableCommand command) {
      if (context.getLockOwner() == null && command instanceof RemoteLockCommand) {
         context.setLockOwner(((RemoteLockCommand) command).getKeyLockOwner());
      }
   }

   interface ContextBuilder {
      InvocationContext create(int keyCount);
   }

}
