package org.infinispan.cache.impl;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.FAIL_SILENTLY;
import static org.infinispan.context.Flag.FORCE_ASYNCHRONOUS;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.PUT_FOR_EXTERNAL_READ;
import static org.infinispan.context.Flag.ZERO_LOCK_ACQUISITION_TIMEOUT;
import static org.infinispan.context.InvocationContextFactory.UNBOUNDED;
import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.LockedStream;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.functions.MergeFunction;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.SizeCommand;
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
import org.infinispan.commons.api.query.ContinuousQuery;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.ExpirationConfiguration;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.impl.Params;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.jmx.annotations.DataType;
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
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.statetransfer.StateTransferManager;
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
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
@MBean(objectName = CacheImpl.OBJECT_NAME, description = "Component that represents an individual cache instance.")
public class CacheImpl<K, V> implements AdvancedCache<K, V>, InternalCache<K, V> {
   private static final Log log = LogFactory.getLog(CacheImpl.class);
   public static final String OBJECT_NAME = "Cache";
   private static final long PFER_FLAGS = EnumUtil.bitSetOf(FAIL_SILENTLY, FORCE_ASYNCHRONOUS, ZERO_LOCK_ACQUISITION_TIMEOUT, PUT_FOR_EXTERNAL_READ, IGNORE_RETURN_VALUES);

   @Inject protected InvocationContextFactory invocationContextFactory;
   @Inject protected CommandsFactory commandsFactory;
   @Inject protected AsyncInterceptorChain invoker;
   @Inject protected Configuration config;
   @Inject protected CacheNotifier<K,V> notifier;
   @Inject protected CacheManagerNotifier cacheManagerNotifier;
   @Inject protected BatchContainer batchContainer;
   @Inject protected ComponentRegistry componentRegistry;
   @Inject protected TransactionManager transactionManager;
   @Inject protected RpcManager rpcManager;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject EvictionManager<K,V> evictionManager;
   @Inject InternalExpirationManager<K, V> expirationManager;
   @Inject InternalDataContainer<K,V> dataContainer;
   @Inject EmbeddedCacheManager cacheManager;
   @Inject LockManager lockManager;
   @Inject DistributionManager distributionManager;
   @Inject TransactionTable txTable;
   @Inject AuthorizationManager authorizationManager;
   @Inject PartitionHandlingManager partitionHandlingManager;
   @Inject GlobalConfiguration globalCfg;
   @Inject LocalTopologyManager localTopologyManager;
   @Inject StateTransferManager stateTransferManager;
   @Inject InvocationHelper invocationHelper;
   @Inject StorageConfigurationManager storageConfigurationManager;
   // TODO Remove after all ISPN-11584 is fixed and the AdvancedCache methods are implemented in EncoderCache
   @Inject ComponentRef<AdvancedCache> encoderCache;
   @Inject GroupManager groupManager;

   protected volatile Metadata defaultMetadata;
   private final String name;
   private volatile boolean stopping = false;
   private boolean transactional;
   private boolean batchingEnabled;
   private final ContextBuilder nonTxContextBuilder = this::nonTxContextBuilder;
   private final ContextBuilder defaultBuilder = i -> invocationHelper.createInvocationContextWithImplicitTransaction(i, false);
   private QueryProducer queryProducer;

   public CacheImpl(String name) {
      this.name = name;
   }

   // This should rather be a @Start method but CacheImpl may be not an actual component but a delegate
   // of EncoderCache. ATM there's not method to invoke @Start method, just wireDependencies
   @Inject
   public void preStart() {
      // We have to do this before start, since some components may start before the actual cache and they
      // have to have access to the default metadata on some operations
      updateDefaultMetadata();
      // Listen for changes to the defaults
      config.expiration().attributes().attribute(ExpirationConfiguration.LIFESPAN).addListener((attribute, oldValue) -> updateDefaultMetadata());
      config.expiration().attributes().attribute(ExpirationConfiguration.MAX_IDLE).addListener((attribute, oldValue) -> updateDefaultMetadata());
      transactional = config.transaction().transactionMode().isTransactional();
      batchingEnabled = config.invocationBatching().enabled();
   }

   private void updateDefaultMetadata() {
      defaultMetadata = Configurations.newDefaultMetadata(config);
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      return componentRegistry;
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
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(defaultMetadata.maxIdle(), MILLISECONDS).build();
      return computeInternal(key, remappingFunction, false, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit).build();
      return computeInternal(key, remappingFunction, false, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
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
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit).build();
      return computeInternal(key, remappingFunction, true, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit).build();
      return computeInternal(key, remappingFunction, true, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return computeInternal(key, remappingFunction, true, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   private V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent) {
      return computeInternal(key, remappingFunction, computeIfPresent, applyDefaultMetadata(defaultMetadata), addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   private V computeInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent, Metadata metadata, long flags) {
      return computeInternal(key, remappingFunction, computeIfPresent, metadata, flags, defaultContextBuilderForWrite());
   }

   V computeInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent,
                     Metadata metadata, long flags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertFunctionNotNull(remappingFunction);
      ComputeCommand command = commandsFactory.buildComputeCommand(key, remappingFunction, computeIfPresent,
            keyPartitioner.getSegment(key), metadata, flags);
      return invocationHelper.invoke(contextBuilder, command, 1);
   }

   @Override
   public final V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      return computeIfAbsent(key, mappingFunction, defaultMetadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit).build();
      return computeIfAbsent(key, mappingFunction, metadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit).build();
      return computeIfAbsent(key, mappingFunction, metadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return computeIfAbsentInternal(key, mappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   V computeIfAbsentInternal(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata, long flags,
                             ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertFunctionNotNull(mappingFunction);
      ComputeIfAbsentCommand command = commandsFactory.buildComputeIfAbsentCommand(key, mappingFunction,
            keyPartitioner.getSegment(key), metadata, flags);
      return invocationHelper.invoke(contextBuilder, command, 1);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return mergeInternal(key, value, remappingFunction, defaultMetadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(defaultMetadata.maxIdle(), MILLISECONDS).build();
      return mergeInternal(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return mergeInternal(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return mergeInternal(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   V mergeInternal(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata,
                   long flags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertValueNotNull(value);
      assertFunctionNotNull(remappingFunction);
      DataConversion keyDataConversion;
      DataConversion valueDataConversion;
      //TODO: Correctly propagate DataConversion objects https://issues.redhat.com/browse/ISPN-11584
      if (remappingFunction instanceof BiFunctionMapper) {
         BiFunctionMapper biFunctionMapper = (BiFunctionMapper) remappingFunction;
         keyDataConversion = biFunctionMapper.getKeyDataConversion();
         valueDataConversion = biFunctionMapper.getValueDataConversion();
      } else {
         keyDataConversion = encoderCache.running().getKeyDataConversion();
         valueDataConversion = encoderCache.running().getValueDataConversion();
      }
      ReadWriteKeyCommand<K, V, V> command = commandsFactory.buildReadWriteKeyCommand(key,
            new MergeFunction<>(value, remappingFunction, metadata), keyPartitioner.getSegment(key),
            Params.fromFlagsBitSet(flags), keyDataConversion, valueDataConversion);
      return invocationHelper.invoke(contextBuilder, command, 1);
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
      return remove(key, value, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final boolean remove(Object key, Object value, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value, keyPartitioner.getSegment(key), explicitFlags);
      return invocationHelper.invoke(contextBuilder, command, 1);
   }

   @Override
   public final int size() {
      return size(EnumUtil.EMPTY_BIT_SET);
   }

   final int size(long explicitFlags) {
      SizeCommand command = commandsFactory.buildSizeCommand(null, explicitFlags);
      long size = invocationHelper.invoke(invocationContextFactory.createInvocationContext(false, UNBOUNDED), command);
      return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
   }

   @Override
   public CompletableFuture<Long> sizeAsync() {
      return sizeAsync(EnumUtil.EMPTY_BIT_SET);
   }

   final CompletableFuture<Long> sizeAsync(long explicitFlags) {
      SizeCommand command = commandsFactory.buildSizeCommand(null, explicitFlags);
      return invocationHelper.invokeAsync(invocationContextFactory.createInvocationContext(false, UNBOUNDED), command);
   }

   @Override
   public final boolean isEmpty() {
      return isEmpty(EnumUtil.EMPTY_BIT_SET);
   }

   final boolean isEmpty(long explicitFlags) {
      return entrySet(explicitFlags, null).stream().noneMatch(StreamMarshalling.alwaysTruePredicate());
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

   final V get(Object key, long explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, keyPartitioner.getSegment(key), explicitFlags);
      return invocationHelper.invoke(ctx, command);
   }

   final CacheEntry<K, V> getCacheEntry(Object key, long explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      GetCacheEntryCommand command = commandsFactory.buildGetCacheEntryCommand(key, keyPartitioner.getSegment(key),
            explicitFlags);
      return invocationHelper.invoke(ctx, command);
   }

   @Override
   public final CacheEntry<K, V> getCacheEntry(Object key) {
      return getCacheEntry(key, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, 1));
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key) {
      return getCacheEntryAsync(key, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, 1));
   }

   final CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key, long explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      GetCacheEntryCommand command = commandsFactory.buildGetCacheEntryCommand(key, keyPartitioner.getSegment(key),
            explicitFlags);
      return invocationHelper.invokeAsync(ctx, command);
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      return getAll(keys, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, keys.size()));
   }

   final Map<K, V> getAll(Set<?> keys, long explicitFlags, InvocationContext ctx) {
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, explicitFlags, false);
      return dropNullEntries(invocationHelper.invoke(ctx, command));
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return getAllAsync(keys, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, keys.size()));
   }

   final CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys, long explicitFlags, InvocationContext ctx) {
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, explicitFlags, false);
      return invocationHelper.<Map<K, V>>invokeAsync(ctx, command).thenApply(this::dropNullEntries);
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
      Map<K, CacheEntry<K, V>> map = invocationHelper.invoke(ctx, command);
      map.entrySet().removeIf(entry -> entry.getValue() == null);
      return map;
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      return getGroup(groupName, EnumUtil.EMPTY_BIT_SET);
   }

   final Map<K, V> getGroup(String groupName, long explicitFlags) {
      return Collections.unmodifiableMap(internalGetGroup(groupName, explicitFlags, invocationContextFactory.createInvocationContext(false, UNBOUNDED)));
   }

   private Map<K, V> internalGetGroup(String groupName, long explicitFlagsBitSet, InvocationContext ctx) {
      if (groupManager == null) {
         return Collections.emptyMap();
      }
      try (CacheStream<CacheEntry<K, V>> stream = cacheEntrySet(explicitFlagsBitSet, null).stream()) {
         return groupManager.collect(stream, ctx, groupName);
      }
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
         InvocationContext context = defaultContextBuilderForWrite().create(UNBOUNDED);
         Map<K, V> keys = internalGetGroup(groupName, explicitFlagsBitSet, context);
         long removeFlags = addIgnoreReturnValuesFlag(explicitFlagsBitSet);
         for (K key : keys.keySet()) {
            invocationHelper.invoke(context, createRemoveCommand(key, removeFlags, false));
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
         invocationHelper.invoke(createRemoveCommand(key, removeFlags, false), 1);
      }
   }

   @Override
   public final V remove(Object key) {
      return remove(key, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   @Override
   public <T> Query<T> query(String query) {
      if (queryProducer == null) {
         throw log.queryNotSupported();
      }

      return queryProducer.query(query);
   }

   @Override
   public ContinuousQuery<K, V> continuousQuery() {
      if (queryProducer == null) {
         throw log.queryNotSupported();
      }

      return queryProducer.continuousQuery(this);
   }

   final V remove(Object key, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      RemoveCommand command = createRemoveCommand(key, explicitFlags, false);
      return invocationHelper.invoke(contextBuilder, command, 1);
   }

   private RemoveCommand createRemoveCommand(Object key, long explicitFlags, boolean returnEntry) {
      long flags = addUnsafeFlags(explicitFlags);
      return commandsFactory.buildRemoveCommand(key, null, keyPartitioner.getSegment(key), flags, returnEntry);
   }

   @Override
   public CompletableFuture<Boolean> removeLifespanExpired(K key, V value, Long lifespan) {
      return removeLifespanExpired(key, value, lifespan, EnumUtil.EMPTY_BIT_SET);
   }

   final CompletableFuture<Boolean> removeLifespanExpired(K key, V value, Long lifespan, long explicitFlags) {
      RemoveExpiredCommand command = commandsFactory.buildRemoveExpiredCommand(key, value, keyPartitioner.getSegment(key),
            lifespan, explicitFlags | FlagBitSets.SKIP_CACHE_LOAD | FlagBitSets.SKIP_XSITE_BACKUP);
      return performVisitableNonTxCommand(command);
   }

   @Override
   public CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value) {
      return removeMaxIdleExpired(key, value, EnumUtil.EMPTY_BIT_SET);
   }

   final CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value, long explicitFlags) {
      RemoveExpiredCommand command = commandsFactory.buildRemoveExpiredCommand(key, value, keyPartitioner.getSegment(key),
            explicitFlags | FlagBitSets.SKIP_CACHE_LOAD);
      return performVisitableNonTxCommand(command);
   }

   private CompletableFuture<Boolean> performVisitableNonTxCommand(VisitableCommand command) {
      Transaction ongoingTransaction = null;
      try {
         ongoingTransaction = suspendOngoingTransactionIfExists();
         return invocationHelper.invokeAsync(nonTxContextBuilder, command, 1);
      } catch (Exception e) {
         if (log.isDebugEnabled()) log.debug("Caught exception while doing removeExpired()", e);
         return CompletableFuture.failedFuture(e);
      } finally {
         resumePreviousOngoingTransaction(ongoingTransaction,
               "Had problems trying to resume a transaction after removeExpired()");
      }
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> encoderClass) {
      throw new UnsupportedOperationException("Encoding requires EncoderCache");
   }

   @Override
   public AdvancedCache<?, ?> withKeyEncoding(Class<? extends Encoder> encoderClass) {
      throw new UnsupportedOperationException("Encoding requires EncoderCache");
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> keyEncoderClass, Class<? extends Encoder> valueEncoderClass) {
      throw new UnsupportedOperationException("Encoding requires EncoderCache");
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapperClass) {
      throw new UnsupportedOperationException("Wrapping requires EncoderCache");
   }

   @Override
   public AdvancedCache<K, V> withMediaType(String keyMediaType, String valueMediaType) {
      throw new UnsupportedOperationException("Conversion requires EncoderCache");
   }

   @Override
   public <K1, V1> AdvancedCache<K1, V1> withMediaType(MediaType keyMediaType, MediaType valueMediaType) {
      throw new UnsupportedOperationException("Conversion requires EncoderCache");
   }

   @Override
   public AdvancedCache<K, V> withStorageMediaType() {
      throw new UnsupportedOperationException("Conversion requires EncoderCache");
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapperClass, Class<? extends Wrapper> valueWrapperClass) {
      throw new UnsupportedOperationException("Conversion requires EncoderCache");
   }

   @Override
   public DataConversion getKeyDataConversion() {
      throw new UnsupportedOperationException("Conversion requires EncoderCache");
   }

   @Override
   public DataConversion getValueDataConversion() {
      throw new UnsupportedOperationException("Conversion requires EncoderCache");
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
         invocationHelper.invoke(context, command);
      } finally {
         resumePreviousOngoingTransaction(tx, "Had problems trying to resume a transaction after clear()");
      }
   }

   @Override
   public CacheSet<K> keySet() {
      return keySet(EnumUtil.EMPTY_BIT_SET, null);
   }

   CacheSet<K> keySet(long explicitFlags, Object lockOwner) {
      return new CacheBackedKeySet(this, lockOwner, explicitFlags);
   }

   @Override
   public CacheCollection<V> values() {
      return values(EnumUtil.EMPTY_BIT_SET, null);
   }

   CacheCollection<V> values(long explicitFlags, Object lockOwner) {
      return new ValueCacheCollection<>(this, cacheEntrySet(explicitFlags, lockOwner));
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      return cacheEntrySet(EnumUtil.EMPTY_BIT_SET, null);
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

   CacheSet<CacheEntry<K, V>> cacheEntrySet(long explicitFlags, Object lockOwner) {
      return new CacheBackedEntrySet(this, lockOwner, explicitFlags);
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return entrySet(EnumUtil.EMPTY_BIT_SET, null);
   }

   CacheSet<Map.Entry<K, V>> entrySet(long explicitFlags, Object lockOwner) {
      return new CacheBackedEntrySet(this, lockOwner, explicitFlags);
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
         putIfAbsent(key, value, metadata, EnumUtil.mergeBitSets(PFER_FLAGS, explicitFlags), nonTxContextBuilder);
      } catch (Exception e) {
         if (log.isDebugEnabled()) log.debug("Caught exception while doing putForExternalRead()", e);
      } finally {
         resumePreviousOngoingTransaction(ongoingTransaction, "Had problems trying to resume a transaction after putForExternalRead()");
      }
   }

   @Override
   public final void evict(K key) {
      evict(key, EnumUtil.EMPTY_BIT_SET);
   }

   final void evict(K key, long explicitFlags) {
      assertKeyNotNull(key);
      if (!config.memory().isEvictionEnabled() && config.memory().whenFull() != EvictionStrategy.MANUAL) {
         log.evictionDisabled(name);
      }
      InvocationContext ctx = createSingleKeyNonTxInvocationContext();
      EvictCommand command = commandsFactory.buildEvictCommand(key, keyPartitioner.getSegment(key), explicitFlags);
      invocationHelper.invoke(ctx, command);
   }

   private InvocationContext createSingleKeyNonTxInvocationContext() {
      return invocationContextFactory.createSingleKeyNonTxInvocationContext();
   }

   @Override
   public Configuration getCacheConfiguration() {
      return config;
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      return notifier.addListenerAsync(listener);
   }

   CompletionStage<Void> addListenerAsync(ListenerHolder listenerHolder) {
      return notifier.addListenerAsync(listenerHolder, null, null, null);
   }

   <C> CompletionStage<Void> addListenerAsync(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter,
                        CacheEventConverter<? super K, ? super V, C> converter) {
      return notifier.addListenerAsync(listenerHolder, filter, converter, null);
   }

   @Override
   public <C> CompletionStage<Void> addListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter,
                               CacheEventConverter<? super K, ? super V, C> converter) {
      return notifier.addListenerAsync(listener, filter, converter);
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      return notifier.removeListenerAsync(listener);
   }

   @Override
   public <C> CompletionStage<Void> addFilteredListenerAsync(Object listener,
                                       CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      return notifier.addFilteredListenerAsync(listener, filter, converter, filterAnnotations);
   }

   @Override
   public <C> CompletionStage<Void> addStorageFormatFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter,
                                                    CacheEventConverter<? super K, ? super V, C> converter,
                                                    Set<Class<? extends Annotation>> filterAnnotations) {
      return notifier.addStorageFormatFilteredListenerAsync(listener, filter, converter, filterAnnotations);
   }

   <C> CompletionStage<Void> addFilteredListenerAsync(ListenerHolder listener,
                                CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                Set<Class<? extends Annotation>> filterAnnotations) {
      return notifier.addFilteredListenerAsync(listener, filter, converter, filterAnnotations);
   }

   private InvocationContext nonTxContextBuilder(int keyCount) {
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
      return invocationHelper.invoke(ctx, command);
   }

   @Override
   @ManagedOperation(
         description = "Starts the cache.",
         displayName = "Starts cache."
   )
   public void start() {
      componentRegistry.start();
      queryProducer = componentRegistry.getComponent(QueryProducer.class);

      if (stateTransferManager != null) {
         try {
            stateTransferManager.waitForInitialStateTransferToComplete();
         } catch (Throwable t) {
            log.debugf("Stopping cache as exception encountered waiting for state transfer", t);
            componentRegistry.stop();
            throw t;
         }
      }
      log.debugf("Started cache %s on %s", getName(), managerIdentifier());
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
      log.debugf("Shutting down cache %s on %s", getName(), managerIdentifier());

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
      log.debugf("Stopping cache %s on %s", getName(), managerIdentifier());
      componentRegistry.stop();
   }

   @Override
   public ExpirationManager<K, V> getExpirationManager() {
      return expirationManager;
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
      return new DecoratedCache<>(this, requireNonNull(lockOwner, "lockOwner can't be null"), EnumUtil.EMPTY_BIT_SET);
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
         dataType = DataType.TRAIT
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
         throw CONFIG.invocationBatchingNotEnabled();
      }
      return batchContainer.startBatch();
   }

   @Override
   public void endBatch(boolean successful) {
      if (!batchingEnabled) {
         throw CONFIG.invocationBatchingNotEnabled();
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
         dataType = DataType.TRAIT
   )
   public String getCacheName() {
      return getName() + "(" + getCacheConfiguration().clustering().cacheMode().toString().toLowerCase() + ")";
   }

   /**
    * Returns the version of Infinispan.
    */
   @ManagedAttribute(
         description = "Returns the version of Infinispan",
         displayName = "Infinispan version",
         dataType = DataType.TRAIT
   )
   @Override
   public String getVersion() {
      return Version.getVersion();
   }

   @Override
   public String toString() {
      return "Cache '" + name + "'@" + managerIdentifier();
   }

   private String managerIdentifier() {
      if (rpcManager != null) {
         return rpcManager.getAddress().toString();
      } else if (globalCfg.transport().nodeName() != null){
         return globalCfg.transport().nodeName();
      } else {
         return globalCfg.cacheManagerName();
      }
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, boolean touchEvenIfExpired) {
      return touch(key, -1, touchEvenIfExpired, EnumUtil.EMPTY_BIT_SET);
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, int segment, boolean touchEvenIfExpired) {
      return touch(key, segment, touchEvenIfExpired, EnumUtil.EMPTY_BIT_SET);
   }

   public CompletionStage<Boolean> touch(Object key, int segment, boolean touchEvenIfExpired, long flagBitSet) {
      if (segment < 0) {
         segment = keyPartitioner.getSegment(key);
      }
      TouchCommand command = commandsFactory.buildTouchCommand(key, segment, touchEvenIfExpired, flagBitSet);
      return performVisitableNonTxCommand(command);
   }

   @Override
   public BatchContainer getBatchContainer() {
      return batchContainer;
   }

   @Override
   public DataContainer<K, V> getDataContainer() {
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
      return StatsImpl.create(config, invoker);
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
      DataWriteCommand command = createPutCommand(key, value, metadata, explicitFlags, false);
      return invocationHelper.invoke(contextBuilder, command, 1);
   }

   private PutKeyValueCommand createPutCommand(K key, V value, Metadata metadata, long explicitFlags, boolean returnEntry) {
      long flags = addUnsafeFlags(explicitFlags);
      Metadata merged = applyDefaultMetadata(metadata);
      return commandsFactory.buildPutKeyValueCommand(key, value, keyPartitioner.getSegment(key), merged, flags, returnEntry);
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
      return putIfAbsent(key, value, metadata, explicitFlags, defaultContextBuilderForWrite());
   }

   final V putIfAbsent(K key, V value, Metadata metadata, long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      DataWriteCommand command = createPutIfAbsentCommand(key, value, metadata, explicitFlags, false);
      return invocationHelper.invoke(contextBuilder, command, 1);
   }

   private PutKeyValueCommand createPutIfAbsentCommand(K key, V value, Metadata metadata, long explicitFlags, boolean returnEntry) {
      long flags = addUnsafeFlags(explicitFlags);
      Metadata merged = applyDefaultMetadata(metadata);
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(key, value, keyPartitioner.getSegment(key),
            merged, flags, returnEntry);
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
      invocationHelper.invoke(contextBuilder, command, map.size());
   }

   public final Map<K, V> getAndPutAll(Map<? extends K, ? extends V> map) {
      return getAndPutAll(map, defaultMetadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final Map<K, V> getAndPutAll(Map<? extends K, ? extends V> map, Metadata metadata, long explicitFlags,
                                ContextBuilder contextBuilder) {
      PutMapCommand command = createPutAllCommand(map, metadata, explicitFlags);
      return dropNullEntries(invocationHelper.invoke(contextBuilder, command, map.size()));
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
      ReplaceCommand command = createReplaceCommand(key, value, metadata, explicitFlags, false);
      return invocationHelper.invoke(contextBuilder, command, 1);
   }

   private ReplaceCommand createReplaceCommand(K key, V value, Metadata metadata, long explicitFlags, boolean returnEntry) {
      long flags = addUnsafeFlags(explicitFlags);
      Metadata merged = applyDefaultMetadata(metadata);
      return commandsFactory.buildReplaceCommand(key, null, value, keyPartitioner.getSegment(key), merged, flags, returnEntry);
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
      return invocationHelper.invoke(contextBuilder, command, 1);
   }

   private ReplaceCommand createReplaceConditionalCommand(K key, V oldValue, V value, Metadata metadata, long explicitFlags) {
      Metadata merged = applyDefaultMetadata(metadata);
      return commandsFactory.buildReplaceCommand(key, oldValue, value, keyPartitioner.getSegment(key), merged, explicitFlags);
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
      PutKeyValueCommand command = createPutCommand(key, value, metadata, explicitFlags, false);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   final CompletableFuture<CacheEntry<K, V>> putAsyncEntry(final K key, final V value, final Metadata metadata, final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      PutKeyValueCommand command = createPutCommand(key, value, metadata, explicitFlags, true);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
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
      return putAllAsync(data, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final Metadata metadata,
                                             long explicitFlags, ContextBuilder contextBuilder) {
      explicitFlags = EnumUtil.mergeBitSets(explicitFlags, FlagBitSets.IGNORE_RETURN_VALUES);
      PutMapCommand command = createPutAllCommand(data, metadata, explicitFlags);
      return invocationHelper.invokeAsync(contextBuilder, command, data.size());
   }

   @Override
   public final CompletableFuture<Void> clearAsync() {
      return clearAsync(EnumUtil.EMPTY_BIT_SET);
   }

   final CompletableFuture<Void> clearAsync(final long explicitFlags) {
      InvocationContext context = invocationContextFactory.createClearNonTxInvocationContext();
      ClearCommand command = commandsFactory.buildClearCommand(explicitFlags);
      return invocationHelper.invokeAsync(context, command).thenApply(nil -> null);
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
      return putIfAbsentAsync(key, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<V> putIfAbsentAsync(final K key, final V value, final Metadata metadata,
                                               final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      PutKeyValueCommand command = createPutIfAbsentCommand(key, value, metadata, explicitFlags, false);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putIfAbsentAsyncEntry(K key, V value, Metadata metadata) {
      return putIfAbsentAsyncEntry(key, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<CacheEntry<K, V>> putIfAbsentAsyncEntry(final K key, final V value, final Metadata metadata,
                                                                   final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      PutKeyValueCommand command = createPutIfAbsentCommand(key, value, metadata, explicitFlags, true);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   @Override
   public final CompletableFuture<V> removeAsync(Object key) {
      return removeAsync(key, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<V> removeAsync(final Object key, final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      RemoveCommand command = createRemoveCommand(key, explicitFlags, false);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> removeAsyncEntry(Object key) {
      return removeAsyncEntry(key, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<CacheEntry<K, V>> removeAsyncEntry(final Object key, final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      RemoveCommand command = createRemoveCommand(key, explicitFlags, true);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   @Override
   public final CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return removeAsync(key, value, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<Boolean> removeAsync(final Object key, final Object value, final long explicitFlags,
                                                ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      RemoveCommand command = commandsFactory.buildRemoveCommand(key, value, keyPartitioner.getSegment(key), explicitFlags);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
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
      return replaceAsync(key, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<V> replaceAsync(final K key, final V value, final Metadata metadata,
                                           final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      ReplaceCommand command = createReplaceCommand(key, value, metadata, explicitFlags, false);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> replaceAsyncEntry(K key, V value, Metadata metadata) {
      return replaceAsyncEntry(key, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<CacheEntry<K, V>> replaceAsyncEntry(final K key, final V value, final Metadata metadata,
                                                                 final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, value);
      ReplaceCommand command = createReplaceCommand(key, value, metadata, explicitFlags, true);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
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
      return replaceAsync(key, oldValue, newValue, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   final CompletableFuture<Boolean> replaceAsync(final K key, final V oldValue, final V newValue,
                                                 final Metadata metadata, final long explicitFlags, ContextBuilder contextBuilder) {
      assertKeyValueNotNull(key, newValue);
      assertValueNotNull(oldValue);
      ReplaceCommand command = createReplaceConditionalCommand(key, oldValue, newValue, metadata, explicitFlags);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return getAsync(key, EnumUtil.EMPTY_BIT_SET, invocationContextFactory.createInvocationContext(false, 1));
   }

   CompletableFuture<V> getAsync(final K key, final long explicitFlags, InvocationContext ctx) {
      assertKeyNotNull(key);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, keyPartitioner.getSegment(key), explicitFlags);
      return invocationHelper.invokeAsync(ctx, command);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeAsync(key, remappingFunction, false);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return computeAsyncInternal(key, remappingFunction, false, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(defaultMetadata.maxIdle(), MILLISECONDS).build();
      return computeAsyncInternal(key, remappingFunction, false, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET), defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return computeAsyncInternal(key, remappingFunction, false, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET), defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeAsync(key, remappingFunction, true);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return computeAsyncInternal(key, remappingFunction, true, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   private CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent) {
      return computeAsyncInternal(key, remappingFunction, computeIfPresent, applyDefaultMetadata(defaultMetadata), addUnsafeFlags(EnumUtil.EMPTY_BIT_SET));
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(defaultMetadata.maxIdle(), MILLISECONDS).build();
      return computeAsyncInternal(key, remappingFunction, false, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET), defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return computeAsyncInternal(key, remappingFunction, true, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET), defaultContextBuilderForWrite());
   }

   private CompletableFuture<V> computeAsyncInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent, Metadata metadata, long flags) {
      return computeAsyncInternal(key, remappingFunction, computeIfPresent, metadata, flags, defaultContextBuilderForWrite());
   }

   CompletableFuture<V> computeAsyncInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent,
                                             Metadata metadata, long flags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertFunctionNotNull(remappingFunction);
      ComputeCommand command = commandsFactory.buildComputeCommand(key, remappingFunction, computeIfPresent,
            keyPartitioner.getSegment(key), metadata, flags);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      return computeIfAbsentAsync(key, mappingFunction, defaultMetadata);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return computeIfAbsentAsyncInternal(key, mappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(defaultMetadata.maxIdle(), MILLISECONDS).build();
      return computeIfAbsentAsyncInternal(key, mappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET), defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit).build();
      return computeIfAbsentAsyncInternal(key, mappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET), defaultContextBuilderForWrite());
   }

   CompletableFuture<V> computeIfAbsentAsyncInternal(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata, long flags,
                                                     ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertFunctionNotNull(mappingFunction);
      ComputeIfAbsentCommand command = commandsFactory.buildComputeIfAbsentCommand(key, mappingFunction,
            keyPartitioner.getSegment(key), metadata, flags);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return mergeInternalAsync(key, value, remappingFunction, defaultMetadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(defaultMetadata.maxIdle(), MILLISECONDS).build();
      return mergeInternalAsync(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit idleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, idleTimeUnit).build();
      return mergeInternalAsync(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return mergeInternalAsync(key, value, remappingFunction, metadata, addUnsafeFlags(EnumUtil.EMPTY_BIT_SET),
            defaultContextBuilderForWrite());
   }

   CompletableFuture<V> mergeInternalAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata,
                                           long flags, ContextBuilder contextBuilder) {
      assertKeyNotNull(key);
      assertValueNotNull(value);
      assertFunctionNotNull(remappingFunction);
      DataConversion keyDataConversion;
      DataConversion valueDataConversion;
      //TODO: Correctly propagate DataConversion objects https://issues.redhat.com/browse/ISPN-11584
      if (remappingFunction instanceof BiFunctionMapper) {
         BiFunctionMapper biFunctionMapper = (BiFunctionMapper) remappingFunction;
         keyDataConversion = biFunctionMapper.getKeyDataConversion();
         valueDataConversion = biFunctionMapper.getValueDataConversion();
      } else {
         keyDataConversion = encoderCache.running().getKeyDataConversion();
         valueDataConversion = encoderCache.running().getValueDataConversion();
      }
      ReadWriteKeyCommand<K, V, V> command = commandsFactory.buildReadWriteKeyCommand(key,
            new MergeFunction<>(value, remappingFunction, metadata), keyPartitioner.getSegment(key),
            Params.fromFlagsBitSet(flags), keyDataConversion, valueDataConversion);
      return invocationHelper.invokeAsync(contextBuilder, command, 1);
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
   public AdvancedCache<K, V> withFlags(Flag flag) {
      return new DecoratedCache<>(this, EnumUtil.bitSetOf(flag));
   }

   @Override
   public AdvancedCache<K, V> withFlags(final Flag... flags) {
      if (flags == null || flags.length == 0)
         return this;
      else
         return new DecoratedCache<>(this, EnumUtil.bitSetOf(flags));
   }

   @Override
   public AdvancedCache<K, V> withFlags(Collection<Flag> flags) {
      if (flags == null || flags.isEmpty())
         return this;
      else
         return new DecoratedCache<>(this, EnumUtil.bitSetOf(flags));
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

   private void tryBegin() {
      if (transactionManager == null) {
         return;
      }
      try {
         transactionManager.begin();
         final Transaction transaction = getOngoingTransaction(true);
         if (log.isTraceEnabled()) {
            log.tracef("Implicit transaction started! Transaction: %s", transaction);
         }
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
         if (log.isTraceEnabled()) log.trace("Could not rollback", t);//best effort
      }
   }

   private void tryCommit() {
      if (transactionManager == null) {
         return;
      }
      if (log.isTraceEnabled())
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
      return globalCfg.classLoader();
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      return put(key, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      putAll(map, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
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
      return replace(key, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return replace(key, oldValue, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return putIfAbsent(key, value, metadata, EnumUtil.EMPTY_BIT_SET);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return putAsync(key, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putAsyncEntry(K key, V value, Metadata metadata) {
      return putAsyncEntry(key, value, metadata, EnumUtil.EMPTY_BIT_SET, defaultContextBuilderForWrite());
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

   private void resumePreviousOngoingTransaction(Transaction transaction, String failMessage) {
      if (transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            if (log.isDebugEnabled()) {
               log.debug(failMessage);
            }
         }
      }
   }

   @ManagedAttribute(
         description = "Returns the cache configuration in form of properties",
         displayName = "Cache configuration properties",
         dataType = DataType.TRAIT
   )
   public Properties getConfigurationAsProperties() {
      return new PropertyFormatter().format(config);
   }

   /**
    * @return The default {@link ContextBuilder} implementation for write operations.
    */
   public ContextBuilder defaultContextBuilderForWrite() {
      return defaultBuilder;
   }
}
