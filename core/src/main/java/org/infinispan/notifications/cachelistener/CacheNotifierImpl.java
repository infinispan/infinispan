package org.infinispan.notifications.cachelistener;

import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_ACTIVATED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_CREATED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_EVICTED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_EXPIRED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_INVALIDATED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_LOADED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_MODIFIED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_PASSIVATED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_REMOVED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.CACHE_ENTRY_VISITED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.DATA_REHASHED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.PARTITION_STATUS_CHANGED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.PERSISTENCE_AVAILABILITY_CHANGED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.TOPOLOGY_CHANGED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.TRANSACTION_COMPLETED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.TRANSACTION_REGISTERED;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.lang.annotation.Annotation;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.EncoderEntryMapper;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.CacheFilters;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.annotation.PersistenceAvailabilityChanged;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerRemoveCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.notifications.cachelistener.cluster.RemoteClusterListener;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.notifications.cachelistener.event.PersistenceAvailabilityChangedEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.notifications.cachelistener.event.impl.EventImpl;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterAsConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterAsKeyValueFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterAsKeyValueFilterConverter;
import org.infinispan.notifications.cachelistener.filter.DelegatingCacheEntryListenerInvocation;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.notifications.impl.AbstractListenerImpl;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.reactive.publisher.PublisherTransformers;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.stream.impl.CacheIntermediatePublisher;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.object.FilterOperation;
import org.infinispan.stream.impl.intops.object.MapOperation;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import jakarta.transaction.Status;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/**
 * Helper class that handles all notifications to registered listeners.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @author William Burns
 * @author anistor@redhat.com
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public class CacheNotifierImpl<K, V> extends AbstractListenerImpl<Event<K, V>, CacheEntryListenerInvocation<K, V>>
      implements ClusterCacheNotifier<K, V> {

   private static final Log log = LogFactory.getLog(CacheNotifierImpl.class);

   private static final Map<Class<? extends Annotation>, Class<?>> allowedListeners = new HashMap<>(16);
   private static final Map<Class<? extends Annotation>, Class<?>> clusterAllowedListeners = new HashMap<>(4);

   static {
      allowedListeners.put(CacheEntryCreated.class, CacheEntryCreatedEvent.class);
      allowedListeners.put(CacheEntryRemoved.class, CacheEntryRemovedEvent.class);
      allowedListeners.put(CacheEntryVisited.class, CacheEntryVisitedEvent.class);
      allowedListeners.put(CacheEntryModified.class, CacheEntryModifiedEvent.class);
      allowedListeners.put(CacheEntryActivated.class, CacheEntryActivatedEvent.class);
      allowedListeners.put(CacheEntryPassivated.class, CacheEntryPassivatedEvent.class);
      allowedListeners.put(CacheEntryLoaded.class, CacheEntryLoadedEvent.class);
      allowedListeners.put(CacheEntriesEvicted.class, CacheEntriesEvictedEvent.class);
      allowedListeners.put(TransactionRegistered.class, TransactionRegisteredEvent.class);
      allowedListeners.put(TransactionCompleted.class, TransactionCompletedEvent.class);
      allowedListeners.put(CacheEntryInvalidated.class, CacheEntryInvalidatedEvent.class);
      allowedListeners.put(CacheEntryExpired.class, CacheEntryExpiredEvent.class);
      allowedListeners.put(DataRehashed.class, DataRehashedEvent.class);
      allowedListeners.put(TopologyChanged.class, TopologyChangedEvent.class);
      allowedListeners.put(PartitionStatusChanged.class, PartitionStatusChangedEvent.class);
      allowedListeners.put(PersistenceAvailabilityChanged.class, PersistenceAvailabilityChangedEvent.class);

      clusterAllowedListeners.put(CacheEntryCreated.class, CacheEntryCreatedEvent.class);
      clusterAllowedListeners.put(CacheEntryModified.class, CacheEntryModifiedEvent.class);
      clusterAllowedListeners.put(CacheEntryRemoved.class, CacheEntryRemovedEvent.class);
      clusterAllowedListeners.put(CacheEntryExpired.class, CacheEntryExpiredEvent.class);
   }

   final List<CacheEntryListenerInvocation<K, V>> cacheEntryCreatedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntryRemovedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntryVisitedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntryModifiedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntryActivatedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntryPassivatedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntryLoadedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntryInvalidatedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntryExpiredListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> cacheEntriesEvictedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> transactionRegisteredListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> transactionCompletedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> dataRehashedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> topologyChangedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> partitionChangedListeners = new CopyOnWriteArrayList<>();
   final List<CacheEntryListenerInvocation<K, V>> persistenceChangedListeners = new CopyOnWriteArrayList<>();

   @Inject TransactionManager transactionManager;
   @Inject Configuration config;
   @Inject GlobalConfiguration globalConfiguration;
   @Inject InternalEntryFactory entryFactory;
   @Inject ClusterEventManager<K, V> eventManager;
   @Inject BasicComponentRegistry componentRegistry;
   @Inject KeyPartitioner keyPartitioner;
   @Inject RpcManager rpcManager;
   @Inject EncoderRegistry encoderRegistry;

   @Inject ComponentRef<AdvancedCache<K, V>> cache;
   @Inject ComponentRef<ClusteringDependentLogic> clusteringDependentLogic;
   @Inject ComponentRef<AsyncInterceptorChain> interceptorChain;
   @Inject ComponentRef<ClusterPublisherManager<K, V>> publisherManager;

   private ClusterExecutor clusterExecutor;
   private final Map<Object, UUID> clusterListenerIDs = new ConcurrentHashMap<>();

   private Collection<FilterIndexingServiceProvider> filterIndexingServiceProviders;

   /**
    * This map is used to store the handler used when a listener is registered which has includeCurrentState and
    * is only used for that listener during the initial state transfer
    */
   private final ConcurrentMap<UUID, QueueingSegmentListener<K, V, ? extends Event<K, V>>> segmentHandler;

   public CacheNotifierImpl() {
      this(new ConcurrentHashMap<>());
   }

   CacheNotifierImpl(ConcurrentMap<UUID, QueueingSegmentListener<K, V, ? extends Event<K, V>>> handler) {
      segmentHandler = handler;

      listenersMap.put(CacheEntryCreated.class, cacheEntryCreatedListeners);
      listenersMap.put(CacheEntryRemoved.class, cacheEntryRemovedListeners);
      listenersMap.put(CacheEntryVisited.class, cacheEntryVisitedListeners);
      listenersMap.put(CacheEntryModified.class, cacheEntryModifiedListeners);
      listenersMap.put(CacheEntryActivated.class, cacheEntryActivatedListeners);
      listenersMap.put(CacheEntryPassivated.class, cacheEntryPassivatedListeners);
      listenersMap.put(CacheEntryLoaded.class, cacheEntryLoadedListeners);
      listenersMap.put(CacheEntriesEvicted.class, cacheEntriesEvictedListeners);
      listenersMap.put(CacheEntryExpired.class, cacheEntryExpiredListeners);
      listenersMap.put(TransactionRegistered.class, transactionRegisteredListeners);
      listenersMap.put(TransactionCompleted.class, transactionCompletedListeners);
      listenersMap.put(CacheEntryInvalidated.class, cacheEntryInvalidatedListeners);
      listenersMap.put(DataRehashed.class, dataRehashedListeners);
      listenersMap.put(TopologyChanged.class, topologyChangedListeners);
      listenersMap.put(PartitionStatusChanged.class, partitionChangedListeners);
      listenersMap.put(PersistenceAvailabilityChanged.class, persistenceChangedListeners);
   }

   @Start
   public void start() {
      if (!config.simpleCache()) {
         clusterExecutor = SecurityActions.getClusterExecutor(cache.wired());
      }

      Collection<FilterIndexingServiceProvider> providers = ServiceFinder.load(FilterIndexingServiceProvider.class);
      filterIndexingServiceProviders = new ArrayList<>(providers.size());
      for (FilterIndexingServiceProvider provider : providers) {
         componentRegistry.wireDependencies(provider, false);
         provider.start();
         filterIndexingServiceProviders.add(provider);
      }
   }

   @Override
   public void stop() {
      super.stop();

      // The other nodes will remove the listener automatically
      clusterListenerIDs.clear();

      if (filterIndexingServiceProviders != null) {
         for (FilterIndexingServiceProvider provider : filterIndexingServiceProviders) {
            provider.stop();
         }
         filterIndexingServiceProviders = null;
      }
   }

   @Override
   protected Log getLog() {
      return log;
   }


   @Override
   protected Map<Class<? extends Annotation>, Class<?>> getAllowedMethodAnnotations(Listener l) {
      if (l.clustered()) {
         // Cluster listeners only allow a subset of types
         return clusterAllowedListeners;
      }
      return allowedListeners;
   }

   private K convertKey(CacheEntryListenerInvocation listenerInvocation, K key) {
      if (key == null) return null;
      DataConversion keyDataConversion = listenerInvocation.getKeyDataConversion();
      Wrapper wrp = keyDataConversion.getWrapper();
      Object unwrappedKey = keyDataConversion.getEncoder().fromStorage(wrp.unwrap(key));
      CacheEventFilter filter = listenerInvocation.getFilter();
      CacheEventConverter converter = listenerInvocation.getConverter();
      if (filter == null && converter == null) {
         if (listenerInvocation.useStorageFormat()) {
            return (K) unwrappedKey;
         }
         // If no filter is present, convert to the requested format directly
         return (K) keyDataConversion.fromStorage(key);
      }
      MediaType convertFormat = filter == null ? converter.format() : filter.format();
      if (listenerInvocation.useStorageFormat() || convertFormat == null) {
         // Filter will be run on the storage format, return the unwrapped key
         return (K) unwrappedKey;
      }

      // Filter has a specific format to run, convert to that format
      return (K) encoderRegistry.convert(unwrappedKey, keyDataConversion.getStorageMediaType(), convertFormat);
   }

   private V convertValue(CacheEntryListenerInvocation listenerInvocation, V value) {
      if (value == null) return null;
      DataConversion valueDataConversion = listenerInvocation.getValueDataConversion();
      Wrapper wrp = valueDataConversion.getWrapper();
      Object unwrappedValue = valueDataConversion.getEncoder().fromStorage(wrp.unwrap(value));
      CacheEventFilter filter = listenerInvocation.getFilter();
      CacheEventConverter converter = listenerInvocation.getConverter();
      if (filter == null && converter == null) {
         if (listenerInvocation.useStorageFormat()) {
            return (V) unwrappedValue;
         }
         // If no filter is present, convert to the requested format directly
         return (V) valueDataConversion.fromStorage(value);
      }
      MediaType convertFormat = filter == null ? converter.format() : filter.format();
      if (listenerInvocation.useStorageFormat() || convertFormat == null) {
         // Filter will be run on the storage format, return the unwrapped key
         return (V) unwrappedValue;
      }
      // Filter has a specific format to run, convert to that format
      return (V) encoderRegistry.convert(unwrappedValue, valueDataConversion.getStorageMediaType(), convertFormat);
   }

   @Override
   protected final Transaction suspendIfNeeded() {
      if (transactionManager == null) {
         return null;
      }

      try {
         switch (transactionManager.getStatus()) {
            case Status.STATUS_NO_TRANSACTION:
               return null;
            case Status.STATUS_ACTIVE:
            case Status.STATUS_MARKED_ROLLBACK:
            case Status.STATUS_PREPARED:
            case Status.STATUS_COMMITTED:
            case Status.STATUS_ROLLEDBACK:
            case Status.STATUS_UNKNOWN:
            case Status.STATUS_PREPARING:
            case Status.STATUS_COMMITTING:
            case Status.STATUS_ROLLING_BACK:
            default:
               //suspend in default and in unknown status to be safer
               return transactionManager.suspend();
         }
      } catch (Exception e) {
         if (log.isTraceEnabled()) {
            log.trace("An error occurred while trying to suspend a transaction.", e);
         }
         return null;
      }
   }

   @Override
   protected final void resumeIfNeeded(Transaction transaction) {
      if (transaction == null || transactionManager == null) {
         return;
      }
      try {
         transactionManager.resume(transaction);
      } catch (Exception e) {
         if (log.isTraceEnabled()) {
            log.tracef(e, "An error occurred while trying to resume a suspended transaction. tx=%s", transaction);
         }
      }
   }

   int extractSegment(FlagAffectedCommand command, Object key) {
      return SegmentSpecificCommand.extractSegment(command, key, keyPartitioner);
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryCreated(K key, V value, Metadata metadata, boolean pre,
                                       InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryCreatedListeners)) {
         return resumeOnCPU(doNotifyCreated(key, value, metadata, pre, ctx, command), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyCreated(K key, V value, Metadata metadata, boolean pre, InvocationContext ctx,
         FlagAffectedCommand command) {
      if (clusteringDependentLogic.running().commitType(command, ctx, extractSegment(command, key), false).isLocal()
            && (command == null || !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER))) {
         EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_CREATED);
         boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);
         Object batchIdentifier = ctx.isInTxScope() ? null : Thread.currentThread();
         try {
            AggregateCompletionStage<Void> aggregateCompletionStage = null;
            for (CacheEntryListenerInvocation<K, V> listener : cacheEntryCreatedListeners) {
               // Need a wrapper per invocation since converter could modify the entry in it
               configureEvent(listener, e, key, value, metadata, pre, ctx, command, null, null);
               aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                     listener.invoke(new EventWrapper<>(key, e, command), isLocalNodePrimaryOwner));
            }
            if (batchIdentifier != null) {
               return sendEvents(batchIdentifier, aggregateCompletionStage);
            } else if (aggregateCompletionStage != null) {
               return aggregateCompletionStage.freeze();
            }
         } finally {
            if (batchIdentifier != null) {
               eventManager.dropEvents(batchIdentifier);
            }
         }
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryModified(K key, V value, Metadata metadata, V previousValue,
         Metadata previousMetadata, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryModifiedListeners)) {
         return resumeOnCPU(doNotifyModified(key, value, metadata, previousValue, previousMetadata, pre, ctx, command), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyModified(K key, V value, Metadata metadata, V previousValue,
         Metadata previousMetadata, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (clusteringDependentLogic.running().commitType(command, ctx, extractSegment(command, key), false).isLocal()
            && (command == null || !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER))) {
         EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_MODIFIED);
         boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);
         Object batchIdentifier = ctx.isInTxScope() ? null : Thread.currentThread();
         try {
            AggregateCompletionStage<Void> aggregateCompletionStage = null;
            for (CacheEntryListenerInvocation<K, V> listener : cacheEntryModifiedListeners) {
               // Need a wrapper per invocation since converter could modify the entry in it
               configureEvent(listener, e, key, value, metadata, pre, ctx, command, previousValue, previousMetadata);
               aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                     listener.invoke(new EventWrapper<>(key, e, command), isLocalNodePrimaryOwner));
            }
            if (batchIdentifier != null) {
               return sendEvents(batchIdentifier, aggregateCompletionStage);
            } else if (aggregateCompletionStage != null) {
               return aggregateCompletionStage.freeze();
            }
         } finally {
            if (batchIdentifier != null) {
               eventManager.dropEvents(batchIdentifier);
            }
         }
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryRemoved(K key, V previousValue, Metadata previousMetadata, boolean pre,
                                       InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryRemovedListeners)) {
         return resumeOnCPU(doNotifyRemoved(key, previousValue, previousMetadata, pre, ctx, command), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyRemoved(K key, V previousValue, Metadata previousMetadata, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command) {
      if (clusteringDependentLogic.running().commitType(command, ctx, extractSegment(command, key), true).isLocal()) {
         EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_REMOVED);
         boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);
         Object batchIdentifier = ctx.isInTxScope() ? null : Thread.currentThread();
         try {
            AggregateCompletionStage<Void> aggregateCompletionStage = null;
            for (CacheEntryListenerInvocation<K, V> listener : cacheEntryRemovedListeners) {
               // Need a wrapper per invocation since converter could modify the entry in it
               if (pre) {
                  configureEvent(listener, e, key, previousValue, previousMetadata, true, ctx, command, previousValue, previousMetadata);
               } else {
                  // to be consistent it would be better to pass null as previousMetadata but certain server code
                  // depends on ability to retrieve these metadata when pre=false from CacheEntryEvent.getMetadata
                  // instead of having proper method getOldMetadata() there.
                  configureEvent(listener, e, key, null, previousMetadata, false, ctx, command, previousValue, previousMetadata);
               }
               aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                     listener.invoke(new EventWrapper<>(key, e, command), isLocalNodePrimaryOwner));
            }
            if (batchIdentifier != null) {
               return sendEvents(batchIdentifier, aggregateCompletionStage);
            } else if (aggregateCompletionStage != null) {
               return aggregateCompletionStage.freeze();
            }
         } finally {
            if (batchIdentifier != null) {
               eventManager.dropEvents(batchIdentifier);
            }
         }
      }
      return CompletableFutures.completedNull();
   }

   /**
    * Configure event data. Currently used for 'created', 'modified', 'removed', 'invalidated' events.
    */
   private void configureEvent(CacheEntryListenerInvocation listenerInvocation,
                               EventImpl<K, V> e, K key, V value, Metadata metadata, boolean pre, InvocationContext ctx,
                               FlagAffectedCommand command, V previousValue, Metadata previousMetadata) {
      key = convertKey(listenerInvocation, key);
      value = convertValue(listenerInvocation, value);
      previousValue = convertValue(listenerInvocation, previousValue);

      e.setOriginLocal(ctx.isOriginLocal());
      e.setPre(pre);
      e.setValue(pre ? previousValue : value);
      e.setNewValue(value);
      e.setOldValue(previousValue);
      e.setOldMetadata(previousMetadata);
      e.setMetadata(metadata);
      if (command != null && command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         e.setCommandRetried(true);
      }
      e.setKey(key);
      setSource(e, ctx, command);
   }

   /**
    * Configure event data. Currently used for 'activated', 'loaded', 'visited' events.
    */
   private void configureEvent(CacheEntryListenerInvocation listenerInvocation,
                               EventImpl<K, V> e, K key, V value, boolean pre, InvocationContext ctx) {
      e.setPre(pre);
      e.setKey(convertKey(listenerInvocation, key));
      e.setValue(convertValue(listenerInvocation, value));
      e.setOriginLocal(ctx.isOriginLocal());
      setSource(e, ctx, null);
   }

   /**
    * Configure event data. Currently used for 'expired' events.
    */
   private void configureEvent(CacheEntryListenerInvocation listenerInvocation,
                               EventImpl<K, V> e, K key, V value, Metadata metadata, InvocationContext ctx) {
      e.setKey(convertKey(listenerInvocation, key));
      e.setValue(convertValue(listenerInvocation, value));
      e.setMetadata(metadata);
      e.setOriginLocal(true);
      e.setPre(false);
      setSource(e, ctx, null);
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryVisited(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryVisitedListeners)) {
         return resumeOnCPU(doNotifyVisited(key, value, pre, ctx, command), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyVisited(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_VISITED);
      boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);
      for (CacheEntryListenerInvocation<K, V> listener : cacheEntryVisitedListeners) {
         // Need a wrapper per invocation since converter could modify the entry in it
         configureEvent(listener, e, key, value, pre, ctx);
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
               listener.invoke(new EventWrapper<>(key, e, command), isLocalNodePrimaryOwner));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCacheEntriesEvicted(Collection<Map.Entry<K, V>> entries, InvocationContext ctx, FlagAffectedCommand command) {
      if (!entries.isEmpty() && isNotificationAllowed(command, cacheEntriesEvictedListeners)) {
         return resumeOnCPU(doNotifyEvicted(entries), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyEvicted(Collection<Map.Entry<K, V>> entries) {
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_EVICTED);
      for (CacheEntryListenerInvocation<K, V> listener : cacheEntriesEvictedListeners) {
         Map<K, V> evictedKeysAndValues = new HashMap<>();
         for (Map.Entry<? extends K, ? extends V> entry : entries) {
            evictedKeysAndValues.put(convertKey(listener, entry.getKey()),
                  convertValue(listener, entry.getValue()));
         }
         e.setEntries(evictedKeysAndValues);
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage, listener.invoke(e));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   private CompletionStage<Void> sendEvents(Object batchIdentifier, AggregateCompletionStage<Void> aggregateCompletionStage) {
      CompletionStage<Void> managerStage = eventManager.sendEvents(batchIdentifier);
      if (aggregateCompletionStage != null) {
         if (managerStage != null) {
            aggregateCompletionStage.dependsOn(managerStage);
         }
         return aggregateCompletionStage.freeze();
      }
      return managerStage;
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryExpired(K key, V value, Metadata metadata, InvocationContext ctx) {
      if (!cacheEntryExpiredListeners.isEmpty()) {
         return resumeOnCPU(doNotifyExpired(key, value, metadata, ctx), key);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyExpired(K key, V value, Metadata metadata, InvocationContext ctx) {
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_EXPIRED);
      boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);

      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      Object batchIdentifier = (ctx != null && ctx.isInTxScope()) ? null : Thread.currentThread();
      try {
         for (CacheEntryListenerInvocation<K, V> listener : cacheEntryExpiredListeners) {
            // Need a wrapper per invocation since converter could modify the entry in it
            configureEvent(listener, e, key, value, metadata, ctx);
            aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                  listener.invoke(new EventWrapper<>(key, e, null), isLocalNodePrimaryOwner));
         }
         if (batchIdentifier != null) {
            return sendEvents(batchIdentifier, aggregateCompletionStage);
         }
      } finally {
         if (batchIdentifier != null) {
            eventManager.dropEvents(batchIdentifier);
         }
      }

      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryInvalidated(final K key, V value, Metadata metadata,
                                           final boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryInvalidatedListeners)) {
         return resumeOnCPU(doNotifyInvalidated(key, value, metadata, pre, ctx, command), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyInvalidated(K key, V value, Metadata metadata, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command) {
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_INVALIDATED);
      boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);
      for (CacheEntryListenerInvocation<K, V> listener : cacheEntryInvalidatedListeners) {
         // Need a wrapper per invocation since converter could modify the entry in it
         configureEvent(listener, e, key, value, metadata, pre, ctx, command, value, metadata);
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
               listener.invoke(new EventWrapper<>(key, e, command), isLocalNodePrimaryOwner));
      }

      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryLoaded(K key, V value, boolean pre,
                                      InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryLoadedListeners)) {
         return resumeOnCPU(doNotifyLoaded(key, value, pre, ctx, command), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyLoaded(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_LOADED);
      boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);
      for (CacheEntryListenerInvocation<K, V> listener : cacheEntryLoadedListeners) {
         // Need a wrapper per invocation since converter could modify the entry in it
         configureEvent(listener, e, key, value, pre, ctx);
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
               listener.invoke(new EventWrapper<>(key, e, command), isLocalNodePrimaryOwner));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryActivated(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryActivatedListeners)) {
         return resumeOnCPU(doNotifyActivated(key, value, pre, ctx, command), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyActivated(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_ACTIVATED);
      boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);
      for (CacheEntryListenerInvocation<K, V> listener : cacheEntryActivatedListeners) {
         // Need a wrapper per invocation since converter could modify the entry in it
         configureEvent(listener, e, key, value, pre, ctx);
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
               listener.invoke(new EventWrapper<>(key, e, command), isLocalNodePrimaryOwner));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   private void setSource(EventImpl<K, V> e, InvocationContext ctx, FlagAffectedCommand command) {
      if (ctx != null && ctx.isInTxScope()) {
         GlobalTransaction tx = ((TxInvocationContext) ctx).getGlobalTransaction();
         e.setSource(tx);
      } else if (command instanceof WriteCommand) {
         CommandInvocationId invocationId = ((WriteCommand) command).getCommandInvocationId();
         e.setSource(invocationId);
      }
   }

   @Override
   public CompletionStage<Void> notifyCacheEntryPassivated(K key, V value, boolean pre, InvocationContext ctx,
         FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryPassivatedListeners)) {
         return resumeOnCPU(doNotifyPassivated(key, value, pre, command), command);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyPassivated(K key, V value, boolean pre, FlagAffectedCommand command) {
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_PASSIVATED);
      boolean isLocalNodePrimaryOwner = isLocalNodePrimaryOwner(key);
      AggregateCompletionStage aggregateCompletionStage = null;
      for (CacheEntryListenerInvocation<K, V> listener : cacheEntryPassivatedListeners) {
         // Need a wrapper per invocation since converter could modify the entry in it
         key = convertKey(listener, key);
         value = convertValue(listener, value);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
               listener.invoke(new EventWrapper<>(key, e, command), isLocalNodePrimaryOwner));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   private boolean isLocalNodePrimaryOwner(K key) {
      return clusteringDependentLogic.running().getCacheTopology().getDistribution(key).isPrimary();
   }

   @Override
   public CompletionStage<Void> notifyTransactionCompleted(GlobalTransaction transaction, boolean successful,
         InvocationContext ctx) {
      if (!transactionCompletedListeners.isEmpty()) {
         return resumeOnCPU(doNotifyTransactionCompleted(transaction, successful, ctx), transaction);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyTransactionCompleted(GlobalTransaction transaction, boolean successful,
         InvocationContext ctx) {
      boolean isOriginLocal = ctx.isOriginLocal();
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), TRANSACTION_COMPLETED);
      e.setOriginLocal(isOriginLocal);
      e.setTransactionId(transaction);
      e.setTransactionSuccessful(successful);
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      for (CacheEntryListenerInvocation<K, V> listener : transactionCompletedListeners) {
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage, listener.invoke(e));
      }
      if (ctx.isInTxScope()) {
         if (successful) {
            return sendEvents(transaction, aggregateCompletionStage);
         } else {
            eventManager.dropEvents(transaction);
         }
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyTransactionRegistered(GlobalTransaction globalTransaction, boolean isOriginLocal) {
      if (!transactionRegisteredListeners.isEmpty()) {
         return resumeOnCPU(doNotifyTransactionRegistered(globalTransaction, isOriginLocal), globalTransaction);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyTransactionRegistered(GlobalTransaction globalTransaction, boolean isOriginLocal) {
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), TRANSACTION_REGISTERED);
      e.setOriginLocal(isOriginLocal);
      e.setTransactionId(globalTransaction);
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      for (CacheEntryListenerInvocation<K, V> listener : transactionRegisteredListeners) {
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage, listener.invoke(e));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyDataRehashed(ConsistentHash oldCH, ConsistentHash newCH, ConsistentHash unionCH,
         int newTopologyId, boolean pre) {
      if (!dataRehashedListeners.isEmpty()) {
         return resumeOnCPU(doNotifyDataRehashed(oldCH, newCH, unionCH, newTopologyId, pre), newTopologyId);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyDataRehashed(ConsistentHash oldCH, ConsistentHash newCH, ConsistentHash unionCH,
         int newTopologyId, boolean pre) {
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), DATA_REHASHED);
      e.setPre(pre);
      e.setReadConsistentHashAtStart(oldCH);
      e.setWriteConsistentHashAtStart(oldCH);
      e.setReadConsistentHashAtEnd(newCH);
      e.setWriteConsistentHashAtEnd(newCH);
      e.setUnionConsistentHash(unionCH);
      e.setNewTopologyId(newTopologyId);
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      for (CacheEntryListenerInvocation<K, V> listener : dataRehashedListeners) {
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage, listener.invoke(e));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyTopologyChanged(CacheTopology oldTopology, CacheTopology newTopology,
         int newTopologyId, boolean pre) {
      if (!topologyChangedListeners.isEmpty()) {
         return resumeOnCPU(doNotifyTopologyChanged(oldTopology, newTopology, newTopologyId, pre), newTopology.getTopologyId());
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyTopologyChanged(CacheTopology oldTopology, CacheTopology newTopology,
         int newTopologyId, boolean pre) {
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), TOPOLOGY_CHANGED);
      e.setPre(pre);
      if (oldTopology != null) {
         e.setReadConsistentHashAtStart(oldTopology.getReadConsistentHash());
         e.setWriteConsistentHashAtStart(oldTopology.getWriteConsistentHash());
      }
      e.setReadConsistentHashAtEnd(newTopology.getReadConsistentHash());
      e.setWriteConsistentHashAtEnd(newTopology.getWriteConsistentHash());
      e.setNewTopologyId(newTopologyId);
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      for (CacheEntryListenerInvocation<K, V> listener : topologyChangedListeners) {
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage, listener.invoke(e));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyPartitionStatusChanged(AvailabilityMode mode, boolean pre) {
      if (!partitionChangedListeners.isEmpty()) {
         return resumeOnCPU(doNotifyPartitionStatusChanged(mode, pre), mode);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyPartitionStatusChanged(AvailabilityMode mode, boolean pre) {
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), PARTITION_STATUS_CHANGED);
      e.setPre(pre);
      e.setAvailabilityMode(mode);
      AggregateCompletionStage aggregateCompletionStage = null;
      for (CacheEntryListenerInvocation<K, V> listener : partitionChangedListeners) {
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage, listener.invoke(e));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyPersistenceAvailabilityChanged(boolean available) {
      if (!persistenceChangedListeners.isEmpty()) {
         return resumeOnCPU(doNotifyPersistenceAvailabilityChanged(available), available);
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> doNotifyPersistenceAvailabilityChanged(boolean available) {
      EventImpl<K, V> e = EventImpl.createEvent(cache.wired(), PERSISTENCE_AVAILABILITY_CHANGED);
      e.setAvailable(available);
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      for (CacheEntryListenerInvocation<K, V> listener : persistenceChangedListeners) {
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage, listener.invoke(e));
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyClusterListeners(Collection<ClusterEvent<K, V>> events, UUID uuid) {
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      // We don't need to unwrap key or value as the node where the event originated did this already
      for (ClusterEvent<K, V> event : events) {
         if (event.isPre()) {
            throw new IllegalArgumentException("Events for cluster listener should never be pre change");
         }
         switch (event.getType()) {
            case CACHE_ENTRY_MODIFIED:
               for (CacheEntryListenerInvocation<K, V> listener : cacheEntryModifiedListeners) {
                  if (listener.isClustered() && uuid.equals(listener.getIdentifier())) {
                     // We force invocation, since it means the owning node passed filters already and they
                     // already converted so don't run converter either
                     aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                           listener.invokeNoChecks(new EventWrapper<>(event.getKey(), event, null), false, true, false));
                     break;
                  }
               }
               break;
            case CACHE_ENTRY_CREATED:
               for (CacheEntryListenerInvocation<K, V> listener : cacheEntryCreatedListeners) {
                  if (listener.isClustered() && uuid.equals(listener.getIdentifier())) {
                     // We force invocation, since it means the owning node passed filters already and they
                     // already converted so don't run converter either
                     aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                           listener.invokeNoChecks(new EventWrapper<>(event.getKey(), event, null), false, true, false));
                     break;
                  }
               }
               break;
            case CACHE_ENTRY_REMOVED:
               for (CacheEntryListenerInvocation<K, V> listener : cacheEntryRemovedListeners) {
                  if (listener.isClustered() && uuid.equals(listener.getIdentifier())) {
                     // We force invocation, since it means the owning node passed filters already and they
                     // already converted so don't run converter either
                     aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                           listener.invokeNoChecks(new EventWrapper<>(event.getKey(), event, null), false, true, false));
                     break;
                  }
               }
               break;
            case CACHE_ENTRY_EXPIRED:
               for (CacheEntryListenerInvocation<K, V> listener : cacheEntryExpiredListeners) {
                  if (listener.isClustered() && uuid.equals(listener.getIdentifier())) {
                     // We force invocation, since it means the owning node passed filters already and they
                     // already converted so don't run converter either
                     aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                           listener.invokeNoChecks(new EventWrapper<>(event.getKey(), event, null), false, true, false));
                     break;
                  }
               }
               break;
            default:
               throw new IllegalArgumentException("Unexpected event type encountered!");
         }
      }
      return aggregateCompletionStage != null ? resumeOnCPU(aggregateCompletionStage.freeze(), uuid) : CompletableFutures.completedNull();
   }

   @Override
   public Collection<ClusterListenerReplicateCallable<K, V>> retrieveClusterListenerCallablesToInstall() {
      Set<Object> enlistedAlready = new HashSet<>();
      Set<ClusterListenerReplicateCallable<K, V>> callables = new HashSet<>();

      if (log.isTraceEnabled()) {
         log.tracef("Request received to get cluster listeners currently registered");
      }

      registerClusterListenerCallablesToInstall(enlistedAlready, callables, cacheEntryModifiedListeners);
      registerClusterListenerCallablesToInstall(enlistedAlready, callables, cacheEntryCreatedListeners);
      registerClusterListenerCallablesToInstall(enlistedAlready, callables, cacheEntryRemovedListeners);

      if (log.isTraceEnabled()) {
         log.tracef("Cluster listeners found %s", callables);
      }

      return callables;
   }

   private void registerClusterListenerCallablesToInstall(Set<Object> enlistedAlready,
                                                          Set<ClusterListenerReplicateCallable<K, V>> callables,
                                                          List<CacheEntryListenerInvocation<K, V>> listenerInvocations) {
      for (CacheEntryListenerInvocation<K, V> listener : listenerInvocations) {
         if (!enlistedAlready.contains(listener.getTarget())) {
            // If clustered means it is local - so use our address
            if (listener.isClustered()) {
               Set<Class<? extends Annotation>> filterAnnotations = listener.getFilterAnnotations();
               callables.add(new ClusterListenerReplicateCallable(cache.wired().getName(), listener.getIdentifier(),
                     rpcManager.getAddress(), listener.getFilter(), listener.getConverter(), listener.isSync(),
                     filterAnnotations, listener.getKeyDataConversion(), listener.getValueDataConversion(), listener.useStorageFormat()));
               enlistedAlready.add(listener.getTarget());
            } else if (listener.getTarget() instanceof RemoteClusterListener) {
               RemoteClusterListener lcl = (RemoteClusterListener) listener.getTarget();
               Set<Class<? extends Annotation>> filterAnnotations = listener.getFilterAnnotations();
               callables.add(new ClusterListenerReplicateCallable(cache.wired().getName(), lcl.getId(), lcl.getOwnerAddress(),
                     listener.getFilter(), listener.getConverter(), listener.isSync(),
                     filterAnnotations, listener.getKeyDataConversion(), listener.getValueDataConversion(), listener.useStorageFormat()));
               enlistedAlready.add(listener.getTarget());
            }
         }
      }
   }

   public boolean isNotificationAllowed(FlagAffectedCommand cmd, List<CacheEntryListenerInvocation<K, V>> listeners) {
      return !listeners.isEmpty() && (cmd == null || !cmd.hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION));
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      return addListenerAsync(listener, null, null, null);
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener, ClassLoader classLoader) {
      return addListenerAsync(listener, null, null, classLoader);
   }

   private <C> CompletionStage<Void> addListenerInternal(Object listener, DataConversion keyDataConversion, DataConversion valueDataConversion,
                                        CacheEventFilter<? super K, ? super V> filter,
                                        CacheEventConverter<? super K, ? super V, C> converter, ClassLoader classLoader, boolean useStorageFormat) {
      final Listener l = testListenerClassValidity(listener.getClass());
      final UUID generatedId = Util.threadLocalRandomUUID();
      final CacheMode cacheMode = config.clustering().cacheMode();
      FilterIndexingServiceProvider indexingProvider = null;
      boolean foundMethods = false;
      // We use identity for null as this means it was invoked by a non encoder cache
      DataConversion keyConversion = keyDataConversion == null ? DataConversion.IDENTITY_KEY : keyDataConversion;
      DataConversion valueConversion = valueDataConversion == null ? DataConversion.IDENTITY_VALUE : valueDataConversion;
      Set<Class<? extends Annotation>> filterAnnotations = findListenerCallbacks(listener);
      if (filter instanceof IndexedFilter) {
         indexingProvider = findIndexingServiceProvider((IndexedFilter) filter);
         if (indexingProvider != null) {
            DelegatingCacheInvocationBuilder builder = new DelegatingCacheInvocationBuilder(indexingProvider);
            adjustCacheInvocationBuilder(builder, filter, converter, filterAnnotations, l, useStorageFormat, generatedId,
                                         keyConversion, valueConversion, classLoader);
            foundMethods = validateAndAddListenerInvocations(listener, builder);
            builder.registerListenerInvocations();
         }
      }
      if (indexingProvider == null) {
         CacheInvocationBuilder builder = new CacheInvocationBuilder();
         adjustCacheInvocationBuilder(builder, filter, converter, filterAnnotations, l, useStorageFormat, generatedId,
                                      keyConversion, valueConversion, classLoader);


         foundMethods = validateAndAddListenerInvocations(listener, builder);
      }

      CompletionStage<Void> stage = CompletableFutures.completedNull();

      if (foundMethods && l.clustered()) {
         if (l.observation() == Listener.Observation.PRE) {
            throw CONTAINER.clusterListenerRegisteredWithOnlyPreEvents(listener.getClass());
         } else if (cacheMode.isInvalidation()) {
            throw new UnsupportedOperationException("Cluster listeners cannot be used with Invalidation Caches!");
         } else if (clusterListenerOnPrimaryOnly()) {
            clusterListenerIDs.put(listener, generatedId);
            Address ourAddress;
            List<Address> members;
            if (rpcManager != null) {
               ourAddress = rpcManager.getAddress();
               members = rpcManager.getMembers();
            } else {
               ourAddress = null;
               members = null;
            }

            // If we are the only member don't even worry about sending listeners
            if (members != null && members.size() > 1) {
               stage = registerClusterListeners(members, generatedId, ourAddress, filter, converter, l, listener,
                     keyDataConversion, valueDataConversion, useStorageFormat);
            }
         }
      }

      // If we have a segment listener handler, it means we have to do initial state
      QueueingSegmentListener<K, V, ? extends Event<K, V>> handler = segmentHandler.remove(generatedId);
      if (handler != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Listener %s requests initial state for cache", generatedId);
         }
         Queue<IntermediateOperation<?, ?, ?, ?>> intermediateOperations = new ArrayDeque<>();

         if (keyDataConversion != DataConversion.IDENTITY_KEY && valueDataConversion != DataConversion.IDENTITY_VALUE) {
            intermediateOperations.add(new MapOperation<>(EncoderEntryMapper.newCacheEntryMapper(
                  keyDataConversion, valueDataConversion, entryFactory)));
         }

         if (filter instanceof CacheEventFilterConverter && (filter == converter || converter == null)) {
            intermediateOperations.add(new MapOperation<>(CacheFilters.converterToFunction(
                  new CacheEventFilterConverterAsKeyValueFilterConverter<>((CacheEventFilterConverter<?, ?, ?>) filter))));
            intermediateOperations.add(new FilterOperation<>(CacheFilters.notNullCacheEntryPredicate()));
         } else {
            if (filter != null) {
               intermediateOperations.add(new FilterOperation<>(CacheFilters.predicate(
                     new CacheEventFilterAsKeyValueFilter<>(filter))));
            }
            if (converter != null) {
               intermediateOperations.add(new MapOperation<>(CacheFilters.function(
                     new CacheEventConverterAsConverter<>(converter))));
            }
         }

         stage = handlePublisher(stage, intermediateOperations, handler, generatedId, l, null, null);
      }
      return stage;
   }

   private CompletionStage<Void> handlePublisher(CompletionStage<Void> currentStage,
                                                 Queue<IntermediateOperation<?, ?, ?, ?>> intermediateOperations, QueueingSegmentListener<K, V, ? extends Event<K, V>> handler,
                                                 UUID generatedId, Listener l, Function<Object, Object> kc, Function<Object, Object> kv) {
      SegmentPublisherSupplier<CacheEntry<K, V>> publisher = publisherManager.running().entryPublisher(
            null, null, null, EnumUtil.EMPTY_BIT_SET,
            // TODO: do we really need EXACTLY_ONCE? AT_LEAST_ONCE should be fine I think
            DeliveryGuarantee.EXACTLY_ONCE, config.clustering().stateTransfer().chunkSize(),
            intermediateOperations.isEmpty() ? PublisherTransformers.identity() : new CacheIntermediatePublisher(intermediateOperations));

      currentStage = currentStage.thenCompose(ignore ->
            Flowable.fromPublisher(publisher.publisherWithSegments())
                  .concatMap(handler)
                  .flatMapCompletable(ice -> Completable.fromCompletionStage(
                        raiseEventForInitialTransfer(generatedId, ice, l.clustered(), kc, kv)), false, 20)
                  .toCompletionStage(null));

      currentStage = currentStage.thenCompose(ignore -> handler.transferComplete());

      if (log.isTraceEnabled()) {
         currentStage = currentStage.whenComplete((v, t) ->
               log.tracef("Listener %s initial state for cache completed", generatedId));
      }
      return currentStage;
   }

   private <C> CompletionStage<Void> registerClusterListeners(List<Address> members, UUID generatedId, Address ourAddress,
         CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
         Listener l, Object listener, DataConversion keyDataConversion, DataConversion valueDataConversion,
         boolean useStorageFormat) {
      if (log.isTraceEnabled()) {
         log.tracef("Replicating cluster listener to other nodes %s for cluster listener with id %s",
               members, generatedId);
      }
      ClusterListenerReplicateCallable<K, V> callable = new ClusterListenerReplicateCallable(cache.wired().getName(),
            generatedId, ourAddress, filter, converter, l.sync(),
            findListenerCallbacks(listener), keyDataConversion, valueDataConversion, useStorageFormat);
      TriConsumer<Address, Void, Throwable> handleSuspect = (a, ignore, t) -> {
         if (t != null && !(t instanceof SuspectException)) {
            log.debugf(t, "Address: %s encountered an exception while adding cluster listener", a);
            throw new CacheListenerException(t);
         }
      };
      // Send to all nodes but ours
      CompletionStage<Void> completionStage = clusterExecutor.filterTargets(a -> !ourAddress.equals(a))
            .submitConsumer(callable, handleSuspect);

      // We have to try any nodes that have been added since we sent the request - as they may not have requested
      // the listener - unfortunately if there are no nodes it throws a SuspectException, so we ignore that
      return completionStage.thenCompose(v ->
            clusterExecutor.filterTargets(a -> !members.contains(a) && !a.equals(ourAddress))
                  .submitConsumer(callable, handleSuspect).exceptionally(t -> {
               // Ignore any suspect exception
               if (!(t instanceof SuspectException)) {
                  throw new CacheListenerException(t);
               }
               return null;
            })
      );
   }

   /**
    * Adds the listener using the provided filter converter and class loader.  The provided builder is used to add
    * additional configuration including (clustered, onlyPrimary and identifier) which can be used after this method is
    * completed to see what values were used in the addition of this listener
    */
   @Override
   public <C> CompletionStage<Void> addListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter,
                               CacheEventConverter<? super K, ? super V, C> converter, ClassLoader classLoader) {
      return addListenerInternal(listener, DataConversion.IDENTITY_KEY, DataConversion.IDENTITY_VALUE, filter, converter, classLoader, false);
   }

   /**
    * Gets a suitable indexing provider for the given indexed filter.
    *
    * @param indexedFilter the filter
    * @return the FilterIndexingServiceProvider that supports the given IndexedFilter or {@code null} if none was found
    */
   private FilterIndexingServiceProvider findIndexingServiceProvider(IndexedFilter indexedFilter) {
      if (filterIndexingServiceProviders != null) {
         for (FilterIndexingServiceProvider provider : filterIndexingServiceProviders) {
            if (provider.supportsFilter(indexedFilter)) {
               return provider;
            }
         }
      }
      log.noFilterIndexingServiceProviderFound(indexedFilter.getClass().getName());
      return null;
   }

   @Override
   public List<CacheEntryListenerInvocation<K, V>> getListenerCollectionForAnnotation(Class<? extends Annotation> annotation) {
      return super.getListenerCollectionForAnnotation(annotation);
   }

   private CompletionStage<Void> raiseEventForInitialTransfer(UUID identifier, CacheEntry entry, boolean clustered,
         Function<Object, Object> kc, Function<Object, Object> kv) {
      EventImpl preEvent;
      if (kc == null) kc = Function.identity();
      if (kv == null) kv = Function.identity();
      if (clustered) {
         // In clustered mode we only send post event
         preEvent = null;
      } else {
         preEvent = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_CREATED);
         preEvent.setKey(kc.apply(entry.getKey()));
         preEvent.setPre(true);
         preEvent.setCurrentState(true);
      }

      EventImpl postEvent = EventImpl.createEvent(cache.wired(), CACHE_ENTRY_CREATED);
      postEvent.setKey(kc.apply(entry.getKey()));
      postEvent.setValue(kv.apply(entry.getValue()));
      postEvent.setMetadata(entry.getMetadata());
      postEvent.setPre(false);
      postEvent.setCurrentState(true);

      AggregateCompletionStage aggregateCompletionStage = null;
      for (CacheEntryListenerInvocation<K, V> invocation : cacheEntryCreatedListeners) {
         // Now notify all our methods of the creates
         if (invocation.getIdentifier() == identifier) {
            if (preEvent != null) {
               // Non clustered notifications are done twice
               aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                     invocation.invokeNoChecks(new EventWrapper<>(null, preEvent, null), true, true, false));
            }
            aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage,
                  invocation.invokeNoChecks(new EventWrapper<>(null, postEvent, null), true, true, false));
         }
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   public <C> CompletionStage<Void> addListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
      return addListenerAsync(listener, filter, converter, null);
   }

   @Override
   public <C> CompletionStage<Void> addFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      return addFilteredListenerInternal(listener, null, null, filter, converter, filterAnnotations, false);
   }

   @Override
   public <C> CompletionStage<Void> addStorageFormatFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
      return addFilteredListenerInternal(listener, null, null, filter, converter, filterAnnotations, false);
   }

   @Override
   public <C> CompletionStage<Void> addListenerAsync(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, ClassLoader classLoader) {
      return addListenerInternal(listenerHolder.getListener(), listenerHolder.getKeyDataConversion(), listenerHolder.getValueDataConversion(), filter, converter, classLoader, false);
   }

   @Override
   public <C> CompletionStage<Void> addFilteredListenerAsync(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter,
                                       CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      return addFilteredListenerInternal(listenerHolder.getListener(), listenerHolder.getKeyDataConversion(), listenerHolder.getValueDataConversion(), filter, converter, filterAnnotations, listenerHolder.isFilterOnStorageFormat());
   }

   protected boolean clusterListenerOnPrimaryOnly() {
      CacheMode mode = config.clustering().cacheMode();
      boolean zeroCapacity = config.clustering().hash().capacityFactor() == 0f || globalConfiguration.isZeroCapacityNode();
      return mode.isDistributed() || (mode.isReplicated() && zeroCapacity);
   }

   private <C> CompletionStage<Void> addFilteredListenerInternal(Object listener, DataConversion keyDataConversion, DataConversion valueDataConversion,
                                                CacheEventFilter<? super K, ? super V> filter,
                                                CacheEventConverter<? super K, ? super V, C> converter,
                                                Set<Class<? extends Annotation>> filterAnnotations, boolean useStorageFormat) {
      final Listener l = testListenerClassValidity(listener.getClass());
      final UUID generatedId = Util.threadLocalRandomUUID();
      final CacheMode cacheMode = config.clustering().cacheMode();

      FilterIndexingServiceProvider indexingProvider = null;
      boolean foundMethods = false;
      // We use identity for null as this means it was invoked by a non encoder cache
      DataConversion keyConversion = keyDataConversion == null ? DataConversion.IDENTITY_KEY : keyDataConversion;
      DataConversion valueConversion = valueDataConversion == null ? DataConversion.IDENTITY_VALUE : valueDataConversion;
      if (filter instanceof IndexedFilter) {
         indexingProvider = findIndexingServiceProvider((IndexedFilter) filter);
         if (indexingProvider != null) {
            DelegatingCacheInvocationBuilder builder = new DelegatingCacheInvocationBuilder(indexingProvider);
            adjustCacheInvocationBuilder(builder, filter, converter, filterAnnotations, l, useStorageFormat, generatedId,
                                         keyConversion, valueConversion, null);
            foundMethods = validateAndAddFilterListenerInvocations(listener, builder, filterAnnotations);
            builder.registerListenerInvocations();
         }
      }
      if (indexingProvider == null) {
         CacheInvocationBuilder builder = new CacheInvocationBuilder();
         adjustCacheInvocationBuilder(builder, filter, converter, filterAnnotations, l, useStorageFormat, generatedId,
                                      keyConversion, valueConversion, null);

         foundMethods = validateAndAddFilterListenerInvocations(listener, builder, filterAnnotations);
      }

      CompletionStage<Void> stage = CompletableFutures.completedNull();

      if (foundMethods && l.clustered()) {
         if (l.observation() == Listener.Observation.PRE) {
            throw CONTAINER.clusterListenerRegisteredWithOnlyPreEvents(listener.getClass());
         } else if (cacheMode.isInvalidation()) {
            throw new UnsupportedOperationException("Cluster listeners cannot be used with Invalidation Caches!");
         } else if (clusterListenerOnPrimaryOnly()) {
            clusterListenerIDs.put(listener, generatedId);
            // This way we only retrieve members of the cache itself
            Address ourAddress = rpcManager.getAddress();
            List<Address> members = rpcManager.getMembers();
            // If we are the only member don't even worry about sending listeners
            if (members != null && members.size() > 1) {
               stage = registerClusterListeners(members, generatedId, ourAddress, filter, converter, l, listener,
                     keyDataConversion, valueDataConversion, useStorageFormat);
            }
         }
      }

      // If we have a segment listener handler, it means we have to do initial state
      QueueingSegmentListener<K, V, ? extends Event<K, V>> handler = segmentHandler.remove(generatedId);
      if (handler != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Listener %s requests initial state for cache", generatedId);
         }

         Queue<IntermediateOperation<?, ?, ?, ?>> intermediateOperations = new ArrayDeque<>();

         MediaType storage = valueConversion.getStorageMediaType();
         MediaType keyReq = keyConversion.getRequestMediaType();
         MediaType valueReq = valueConversion.getRequestMediaType();

         AdvancedCache advancedCache = cache.running();
         DataConversion chainedKeyDataConversion = advancedCache.getKeyDataConversion();
         DataConversion chainedValueDataConversion = advancedCache.getValueDataConversion();

         if (keyReq != null && valueReq != null) {
            chainedKeyDataConversion = chainedKeyDataConversion.withRequestMediaType(keyReq);
            chainedValueDataConversion = chainedValueDataConversion.withRequestMediaType(valueReq);
         }

         boolean hasFilter = false;

         MediaType filterMediaType = null;
         if (filter != null) {
            hasFilter = true;
            filterMediaType = useStorageFormat ? null : filter.format();
            if (filterMediaType == null) {
               // iterate in the storage format
               chainedKeyDataConversion = chainedKeyDataConversion.withRequestMediaType(storage);
               chainedValueDataConversion = chainedValueDataConversion.withRequestMediaType(storage);
            } else {
               // iterate in the filter format
               chainedKeyDataConversion = chainedKeyDataConversion.withRequestMediaType(filterMediaType);
               chainedValueDataConversion = chainedValueDataConversion.withRequestMediaType(filterMediaType);
            }
         }
         if (converter != null) {
            hasFilter = true;
            filterMediaType = useStorageFormat ? null : converter.format();
            if (filterMediaType == null) {
               // iterate in the storage format
               chainedKeyDataConversion = chainedKeyDataConversion.withRequestMediaType(storage);
               chainedValueDataConversion = chainedValueDataConversion.withRequestMediaType(storage);
            } else {
               // iterate in the filter format
               chainedKeyDataConversion = chainedKeyDataConversion.withRequestMediaType(filterMediaType);
               chainedValueDataConversion = chainedValueDataConversion.withRequestMediaType(filterMediaType);
            }
         }

         if (!Objects.equals(chainedKeyDataConversion, keyDataConversion) ||
               !Objects.equals(chainedValueDataConversion, valueDataConversion)) {
            componentRegistry.wireDependencies(chainedKeyDataConversion, false);
            componentRegistry.wireDependencies(chainedValueDataConversion, false);
            intermediateOperations.add(new MapOperation<>(EncoderEntryMapper.newCacheEntryMapper(chainedKeyDataConversion,
                  chainedValueDataConversion, entryFactory)));
         }

         if (filter instanceof CacheEventFilterConverter && (filter == converter || converter == null)) {
            intermediateOperations.add(new MapOperation<>(CacheFilters.converterToFunction(
                  new CacheEventFilterConverterAsKeyValueFilterConverter<>((CacheEventFilterConverter<?, ?, ?>) filter))));
            intermediateOperations.add(new FilterOperation<>(CacheFilters.notNullCacheEntryPredicate()));
         } else {
            if (filter != null) {
               intermediateOperations.add(new FilterOperation<>(CacheFilters.predicate(
                     new CacheEventFilterAsKeyValueFilter<>(filter))));
            }
            if (converter != null) {
               intermediateOperations.add(new MapOperation<>(CacheFilters.function(
                     new CacheEventConverterAsConverter<>(converter))));
            }
         }

         boolean finalHasFilter = hasFilter;
         MediaType finalFilterMediaType = filterMediaType;

         Function<Object, Object> kc = k -> {
            if (!finalHasFilter) return k;
            if (finalFilterMediaType == null || useStorageFormat || keyReq == null) {
               return keyDataConversion.fromStorage(k);
            }
            return encoderRegistry.convert(k, finalFilterMediaType, keyDataConversion.getRequestMediaType());
         };
         Function<Object, Object> kv = v -> {
            if (!finalHasFilter) return v;
            if (finalFilterMediaType == null || useStorageFormat || valueReq == null) {
               return valueConversion.fromStorage(v);
            }
            return encoderRegistry.convert(v, finalFilterMediaType, valueConversion.getRequestMediaType());
         };

         stage = handlePublisher(stage, intermediateOperations, handler, generatedId, l, kc, kv);
      }

      return stage;
   }

   private <C> void adjustCacheInvocationBuilder(CacheInvocationBuilder builder,
                                                 CacheEventFilter<? super K, ? super V> filter,
                                                 CacheEventConverter<? super K, ? super V, C> converter,
                                                 Set<Class<? extends Annotation>> filterAnnotations,
                                                 Listener l, boolean useStorageFormat, UUID generatedId,
                                                 DataConversion keyConversion, DataConversion valueConversion,
                                                 ClassLoader classLoader) {
      builder
            .setIncludeCurrentState(l.includeCurrentState())
            .setClustered(l.clustered())
            .setOnlyPrimary(l.clustered() ? clusterListenerOnPrimaryOnly() : l.primaryOnly())
            .setObservation(l.clustered() ? Listener.Observation.POST : l.observation())
            .setFilter(filter)
            .setConverter(converter)
            .useStorageFormat(useStorageFormat)
            .setKeyDataConversion(keyConversion)
            .setValueDataConversion(valueConversion)
            .setIdentifier(generatedId)
            .setClassLoader(classLoader);

      builder.setFilterAnnotations(filterAnnotations);
   }

   protected class CacheInvocationBuilder extends AbstractInvocationBuilder {
      CacheEventFilter<? super K, ? super V> filter;
      CacheEventConverter<? super K, ? super V, ?> converter;
      boolean onlyPrimary;
      boolean clustered;
      boolean includeCurrentState;
      UUID identifier;
      DataConversion keyDataConversion;
      DataConversion valueDataConversion;
      Listener.Observation observation;
      Set<Class<? extends Annotation>> filterAnnotations;
      boolean storageFormat;

      public CacheEventFilter<? super K, ? super V> getFilter() {
         return filter;
      }

      public CacheInvocationBuilder setFilter(CacheEventFilter<? super K, ? super V> filter) {
         this.filter = filter;
         return this;
      }

      public CacheEventConverter<? super K, ? super V, ?> getConverter() {
         return converter;
      }

      public CacheInvocationBuilder setConverter(CacheEventConverter<? super K, ? super V, ?> converter) {
         this.converter = converter;
         return this;
      }

      public CacheInvocationBuilder useStorageFormat(boolean useStorageFormat) {
         this.storageFormat = useStorageFormat;
         return this;
      }

      public boolean isOnlyPrimary() {
         return onlyPrimary;
      }

      public CacheInvocationBuilder setOnlyPrimary(boolean onlyPrimary) {
         this.onlyPrimary = onlyPrimary;
         return this;
      }

      public boolean isClustered() {
         return clustered;
      }

      public CacheInvocationBuilder setClustered(boolean clustered) {
         this.clustered = clustered;
         return this;
      }

      public UUID getIdentifier() {
         return identifier;
      }

      public CacheInvocationBuilder setIdentifier(UUID identifier) {
         this.identifier = identifier;
         return this;
      }

      public CacheInvocationBuilder setKeyDataConversion(DataConversion dataConversion) {
         this.keyDataConversion = dataConversion;
         return this;
      }

      public CacheInvocationBuilder setValueDataConversion(DataConversion dataConversion) {
         this.valueDataConversion = dataConversion;
         return this;
      }

      public boolean isIncludeCurrentState() {
         return includeCurrentState;
      }

      public CacheInvocationBuilder setIncludeCurrentState(boolean includeCurrentState) {
         this.includeCurrentState = includeCurrentState;
         return this;
      }

      public Listener.Observation getObservation() {
         return observation;
      }

      public CacheInvocationBuilder setObservation(Listener.Observation observation) {
         this.observation = observation;
         return this;
      }

      public CacheInvocationBuilder setFilterAnnotations(Set<Class<? extends Annotation>> filterAnnotations) {
         this.filterAnnotations = filterAnnotations;
         return this;
      }

      @Override
      public CacheEntryListenerInvocation<K, V> build() {
         ListenerInvocation<Event<K, V>> invocation = new ListenerInvocationImpl(target, method, sync, classLoader, subject);

         wireDependencies(filter, converter);

         // If we are dealing with clustered events that forces the cluster listener to only use primary only else we would
         // have duplicate events
         CacheEntryListenerInvocation<K, V> returnValue;

         if (includeCurrentState) {
            // If it is a clustered listener and distributed cache we can do some extra optimizations
            if (clustered) {
               QueueingSegmentListener handler = segmentHandler.get(identifier);
               if (handler == null) {
                  int segments = config.clustering().hash().numSegments();
                  if (clusterListenerOnPrimaryOnly()) {
                     handler = new DistributedQueueingSegmentListener(entryFactory, segments, keyPartitioner);
                  } else {
                     handler = new QueueingAllSegmentListener(entryFactory, segments, keyPartitioner);
                  }
                  QueueingSegmentListener currentQueue = segmentHandler.putIfAbsent(identifier, handler);
                  if (currentQueue != null) {
                     handler = currentQueue;
                  }
               }
               returnValue = new ClusteredListenerInvocation<>(encoderRegistry, invocation, handler, filter, converter, annotation,
                     onlyPrimary, identifier, sync, observation, filterAnnotations, keyDataConversion, valueDataConversion, storageFormat);
            } else {
//               TODO: this is removed until non cluster listeners are supported
//               QueueingSegmentListener handler = segmentHandler.get(identifier);
//               if (handler == null) {
//                  handler = new QueueingAllSegmentListener();
//                  QueueingSegmentListener currentQueue = segmentHandler.putIfAbsent(identifier, handler);
//                  if (currentQueue != null) {
//                     handler = currentQueue;
//                  }
//               }
//               returnValue = new NonClusteredListenerInvocation(invocation, handler, filter, converter, annotation,
//                                                                onlyPrimary, identifier, sync);
               returnValue = new BaseCacheEntryListenerInvocation(encoderRegistry, invocation, filter, converter, annotation,
                     onlyPrimary, clustered, identifier, sync, observation, filterAnnotations, keyDataConversion, valueDataConversion, storageFormat);
            }
         } else {
            // If no includeCurrentState just use the base listener invocation which immediately passes all notifications
            // off
            returnValue = new BaseCacheEntryListenerInvocation(encoderRegistry, invocation, filter, converter, annotation, onlyPrimary,
                  clustered, identifier, sync, observation, filterAnnotations, keyDataConversion, valueDataConversion, storageFormat);
         }
         return returnValue;
      }

      protected <C> void wireDependencies(CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
         if (filter != null) {
            componentRegistry.wireDependencies(filter, false);
         }
         if (converter != null && converter != filter) {
            componentRegistry.wireDependencies(converter, false);
         }
         if (keyDataConversion != null) {
            componentRegistry.wireDependencies(keyDataConversion, false);
         }
         if (valueDataConversion != null) {
            componentRegistry.wireDependencies(valueDataConversion, false);
         }
      }
   }

   protected class DelegatingCacheInvocationBuilder extends CacheInvocationBuilder {

      private final FilterIndexingServiceProvider provider;

      private final Map<Class<? extends Annotation>, List<DelegatingCacheEntryListenerInvocation<K, V>>> listeners = new HashMap<>(3);

      DelegatingCacheInvocationBuilder(FilterIndexingServiceProvider provider) {
         this.provider = provider;
      }

      @Override
      public DelegatingCacheEntryListenerInvocation<K, V> build() {
         DelegatingCacheEntryListenerInvocation<K, V> invocation = provider.interceptListenerInvocation(super.build());
         List<DelegatingCacheEntryListenerInvocation<K, V>> invocations = listeners.get(invocation.getAnnotation());
         if (invocations == null) {
            invocations = new ArrayList<>(2);
            listeners.put(invocation.getAnnotation(), invocations);
         }
         invocations.add(invocation);
         return invocation;
      }

      void registerListenerInvocations() {
         if (!listeners.isEmpty()) {
            boolean filterAndConvert = filter == converter || converter == null;
            provider.registerListenerInvocations(clustered, onlyPrimary, filterAndConvert, (IndexedFilter<?, ?, ?>) filter,
                  listeners, this.keyDataConversion, this.valueDataConversion);
         }
      }
   }

   /**
    * This class is to be used with cluster listener invocations only when they have included current state.  Thus we
    * can assume all types are CacheEntryEvent, since it doesn't allow other types.
    */
   protected class ClusteredListenerInvocation<K, V> extends BaseCacheEntryListenerInvocation<K, V> {

      private final QueueingSegmentListener<K, V, CacheEntryEvent<K, V>> handler;

      public ClusteredListenerInvocation(EncoderRegistry encoderRegistry, ListenerInvocation<Event<K, V>> invocation,
                                         QueueingSegmentListener<K, V, CacheEntryEvent<K, V>> handler,
                                         CacheEventFilter<? super K, ? super V> filter,
                                         CacheEventConverter<? super K, ? super V, ?> converter,
                                         Class<? extends Annotation> annotation, boolean onlyPrimary,
                                         UUID identifier, boolean sync, Listener.Observation observation,
                                         Set<Class<? extends Annotation>> filterAnnotations, DataConversion keyDataConversion, DataConversion valueDataConversion, boolean useStorageFormat) {
         super(encoderRegistry, invocation, filter, converter, annotation, onlyPrimary, true, identifier, sync, observation, filterAnnotations, keyDataConversion, valueDataConversion, useStorageFormat);
         this.handler = handler;
      }

      @Override
      public CompletionStage<Void> invoke(Event<K, V> event) {
         throw new UnsupportedOperationException("Clustered initial transfer don't support regular events!");
      }

      @Override
      protected CompletionStage<Void> doRealInvocation(EventWrapper<K, V, CacheEntryEvent<K, V>> wrapped) {
         // This is only used with clusters and such we can safely cast this here
         if (!handler.handleEvent(wrapped, invocation)) {
            return super.doRealInvocation(wrapped.getEvent());
         }
         return null;
      }

      @Override
      public String toString() {
         return "ClusteredListenerInvocation{id=" + identifier + '}';
      }
   }

   protected static class BaseCacheEntryListenerInvocation<K, V> implements CacheEntryListenerInvocation<K, V> {

      private final EncoderRegistry encoderRegistry;
      protected final ListenerInvocation<Event<K, V>> invocation;
      protected final CacheEventFilter<? super K, ? super V> filter;
      protected final CacheEventConverter<? super K, ? super V, ?> converter;
      private final DataConversion keyDataConversion;
      private final DataConversion valueDataConversion;
      private final boolean useStorageFormat;

      protected final boolean onlyPrimary;
      protected final boolean clustered;
      protected final UUID identifier;
      protected final Class<? extends Annotation> annotation;
      protected final boolean sync;
      protected final boolean filterAndConvert;
      protected final Listener.Observation observation;
      protected final Set<Class<? extends Annotation>> filterAnnotations;


      protected BaseCacheEntryListenerInvocation(EncoderRegistry encoderRegistry, ListenerInvocation<Event<K, V>> invocation,
                                                 CacheEventFilter<? super K, ? super V> filter,
                                                 CacheEventConverter<? super K, ? super V, ?> converter,
                                                 Class<? extends Annotation> annotation, boolean onlyPrimary,
                                                 boolean clustered, UUID identifier, boolean sync,
                                                 Listener.Observation observation,
                                                 Set<Class<? extends Annotation>> filterAnnotations, DataConversion keyDataConversion,
                                                 DataConversion valueDataConversion, boolean useStorageFormat) {
         this.encoderRegistry = encoderRegistry;
         this.invocation = invocation;
         this.filter = filter;
         this.converter = converter;
         this.keyDataConversion = keyDataConversion;
         this.valueDataConversion = valueDataConversion;
         this.useStorageFormat = useStorageFormat;
         this.filterAndConvert = filter instanceof CacheEventFilterConverter && (filter == converter || converter == null);
         this.onlyPrimary = onlyPrimary;
         this.clustered = clustered;
         this.identifier = identifier;
         this.annotation = annotation;
         this.sync = sync;
         this.observation = observation;
         this.filterAnnotations = filterAnnotations;
      }


      @Override
      public CompletionStage<Void> invoke(Event<K, V> event) {
         if (shouldInvoke(event)) {
            return doRealInvocation(event);
         }
         return null;
      }

      /**
       * This is the entry point for local listeners firing events
       *
       * @param wrapped
       * @param isLocalNodePrimaryOwner
       */
      @Override
      public CompletionStage<Void> invoke(EventWrapper<K, V, CacheEntryEvent<K, V>> wrapped, boolean isLocalNodePrimaryOwner) {
         // See if this should be filtered first before evaluating
         CacheEntryEvent<K, V> resultingEvent = shouldInvoke(wrapped.getEvent(), isLocalNodePrimaryOwner);
         if (resultingEvent != null) {
            wrapped.setEvent(resultingEvent);
            return invokeNoChecks(wrapped, false, filterAndConvert, false);
         }
         return null;
      }

      /**
       * This is the entry point for remote listener events being fired
       *
       * @param wrapped
       * @param skipQueue
       */
      @Override
      public CompletionStage<Void> invokeNoChecks(EventWrapper<K, V, CacheEntryEvent<K, V>> wrapped, boolean skipQueue, boolean skipConverter, boolean needsTransform) {
         // We run the converter first, this way the converter doesn't have to run serialized when enqueued and also
         // the handler doesn't have to worry about it
         if (!skipConverter) {
            wrapped.setEvent(convertValue(converter, wrapped.getEvent()));
         }
         if (needsTransform) {
            CacheEntryEvent<K, V> event = wrapped.getEvent();
            EventImpl<K, V> eventImpl = (EventImpl<K, V>) event;
            wrapped.setEvent(convertEventToRequestFormat(eventImpl, filter, converter, eventImpl.getValue()));
         }

         if (skipQueue) {
            return invocation.invoke(wrapped.getEvent());
         } else {
            return doRealInvocation(wrapped);
         }
      }

      protected CompletionStage<Void> doRealInvocation(EventWrapper<K, V, CacheEntryEvent<K, V>> event) {
         return doRealInvocation(event.getEvent());
      }

      protected CompletionStage<Void> doRealInvocation(Event<K, V> event) {
         return invocation.invoke(event);
      }

      protected boolean shouldInvoke(Event<K, V> event) {
         return observation.shouldInvoke(event.isPre());
      }

      protected CacheEntryEvent<K, V> shouldInvoke(CacheEntryEvent<K, V> event, boolean isLocalNodePrimaryOwner) {
         if (log.isTraceEnabled()) {
            log.tracef("Should invoke %s (filter %s)? (onlyPrimary=%s, isPrimary=%s)", event, filter, onlyPrimary, isLocalNodePrimaryOwner);
         }
         if (onlyPrimary && !isLocalNodePrimaryOwner) return null;
         if (event instanceof EventImpl) {
            EventImpl<K, V> eventImpl = (EventImpl<K, V>) event;
            if (!shouldInvoke(event)) return null;
            EventType eventType;
            // Only use the filter if it was provided and we have an event that we can filter properly
            if (filter != null && (eventType = getEvent(eventImpl)) != null) {
               if (filterAndConvert) {
                  Object newValue = ((CacheEventFilterConverter) filter).filterAndConvert(eventImpl.getKey(),
                        eventImpl.getOldValue(), eventImpl.getOldMetadata(), eventImpl.getValue(),
                        eventImpl.getMetadata(), eventType);
                  return newValue != null ? convertEventToRequestFormat(eventImpl, filter, null, newValue) : null;
               } else {
                  boolean accept = filter.accept(eventImpl.getKey(), eventImpl.getOldValue(), eventImpl.getOldMetadata(),
                        eventImpl.getValue(), eventImpl.getMetadata(), eventType);
                  if (!accept) {
                     return null;
                  }
                  if (converter == null) {
                     return convertEventToRequestFormat(eventImpl, filter, null, eventImpl.getValue());
                  }

               }
            }
         }
         return event;
      }

      // We can't currently filter events that don't implement CacheEntryEvent or CACHE_ENTRY_EVICTED events.  Basically
      // events that have a single key value pair only
      private EventType getEvent(EventImpl<K, V> event) {
         switch (event.getType()) {
            case CACHE_ENTRY_ACTIVATED:
            case CACHE_ENTRY_CREATED:
            case CACHE_ENTRY_INVALIDATED:
            case CACHE_ENTRY_LOADED:
            case CACHE_ENTRY_MODIFIED:
            case CACHE_ENTRY_PASSIVATED:
            case CACHE_ENTRY_REMOVED:
            case CACHE_ENTRY_VISITED:
            case CACHE_ENTRY_EXPIRED:
               return new EventType(event.isCommandRetried(), event.isPre(), event.getType());
            default:
               return null;
         }
      }

      @Override
      public Object getTarget() {
         return invocation.getTarget();
      }

      @Override
      public CacheEventFilter<? super K, ? super V> getFilter() {
         return filter;
      }

      @Override
      public Set<Class<? extends Annotation>> getFilterAnnotations() {
         return filterAnnotations;
      }

      @Override
      public DataConversion getKeyDataConversion() {
         return keyDataConversion;
      }

      @Override
      public DataConversion getValueDataConversion() {
         return valueDataConversion;
      }

      @Override
      public boolean useStorageFormat() {
         return useStorageFormat;
      }

      @Override
      public CacheEventConverter<? super K, ? super V, ?> getConverter() {
         return converter;
      }

      @Override
      public boolean isClustered() {
         return clustered;
      }

      @Override
      public UUID getIdentifier() {
         return identifier;
      }

      @Override
      public Listener.Observation getObservation() {
         return observation;
      }

      @Override
      public Class<? extends Annotation> getAnnotation() {
         return annotation;
      }

      protected CacheEntryEvent<K, V> convertValue(CacheEventConverter<? super K, ? super V, ?> converter, CacheEntryEvent<K, V> event) {
         CacheEntryEvent<K, V> returnedEvent;
         if (converter != null) {
            if (event instanceof EventImpl) {
               // This is a bit hacky to let the C type be passed in for the V type
               EventImpl<K, V> eventImpl = (EventImpl<K, V>) event;
               EventType evType = new EventType(eventImpl.isCommandRetried(), eventImpl.isPre(), eventImpl.getType());
               Object newValue;
               if (converter.useRequestFormat()) {
                  eventImpl = convertEventToRequestFormat(eventImpl, null, converter, eventImpl.getValue());
                  newValue = converter.convert(eventImpl.getKey(), (V) eventImpl.getOldValue(),
                        eventImpl.getOldMetadata(), (V) eventImpl.getValue(),
                        eventImpl.getMetadata(), evType);
                  eventImpl.setValue((V) newValue);
               } else {
                  newValue = converter.convert(eventImpl.getKey(), (V) eventImpl.getOldValue(),
                        eventImpl.getOldMetadata(), (V) eventImpl.getValue(),
                        eventImpl.getMetadata(), evType);
               }
               if (!converter.useRequestFormat()) {
                  // Convert from the filter output to the request output
                  return convertEventToRequestFormat(eventImpl, null, converter, newValue);
               } else {
                  returnedEvent = eventImpl;
               }
            } else {
               throw new IllegalArgumentException("Provided event should be org.infinispan.notifications.cachelistener.event.impl.EventImpl " +
                     "when a converter is being used!");
            }
         } else {
            returnedEvent = event;
         }
         return returnedEvent;
      }

      private EventImpl<K, V> convertEventToRequestFormat(EventImpl<K, V> eventImpl,
                                                          CacheEventFilter<? super K, ? super V> filter,
                                                          CacheEventConverter<? super K, ? super V, ?> converter,
                                                          Object newValue) {
         MediaType keyFromFormat = keyDataConversion.getStorageMediaType();
         MediaType valueFromFormat = valueDataConversion.getStorageMediaType();
         if (converter != null) {
            if (converter.format() != null && !useStorageFormat) {
               keyFromFormat = converter.format();
               valueFromFormat = converter.format();
            }
         } else {
            if (filter != null) {
               if (filter.format() != null && !useStorageFormat) {
                  keyFromFormat = filter.format();
                  valueFromFormat = filter.format();
               }
            }
         }
         Object convertedKey = convertToRequestFormat(eventImpl.getKey(), keyFromFormat, keyDataConversion);
         Object convertedValue = convertToRequestFormat(newValue, valueFromFormat, valueDataConversion);
         Object convertedOldValue = converter == null || converter.includeOldValue() ?
               convertToRequestFormat(eventImpl.getOldValue(), valueFromFormat, valueDataConversion) : null;
         EventImpl<K, V> clone = eventImpl.clone();
         clone.setKey((K) convertedKey);
         clone.setValue((V) convertedValue);
         clone.setOldValue((V) convertedOldValue);
         return clone;
      }

      private Object convertToRequestFormat(Object object, MediaType objectFormat, DataConversion dataConversion) {
         if (object == null) return null;
         MediaType requestMediaType = dataConversion.getRequestMediaType();
         if (requestMediaType == null) return dataConversion.fromStorage(object);
         Transcoder transcoder = encoderRegistry.getTranscoder(objectFormat, requestMediaType);
         return transcoder.transcode(object, objectFormat, requestMediaType);
      }

      @Override
      public boolean isSync() {
         return sync;
      }

      @Override
      public String toString() {
         return "BaseCacheEntryListenerInvocation{id=" + identifier + '}';
      }
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      removeListenerFromMaps(listener);
      UUID id = clusterListenerIDs.remove(listener);
      if (id != null) {
         return clusterExecutor.submitConsumer(new ClusterListenerRemoveCallable(
               cache.wired().getName(), id), (a, ignore, t) -> {
            if (t != null) {
               throw new CacheException(t);
            }
         });
      }
      return CompletableFutures.completedNull();
   }

   @Override
   protected Set<CacheEntryListenerInvocation<K, V>> removeListenerInvocation(Class<? extends Annotation> annotation, Object listener) {
      Set<CacheEntryListenerInvocation<K, V>> markedForRemoval = super.removeListenerInvocation(annotation, listener);
      for (CacheEntryListenerInvocation<K, V> li : markedForRemoval) {
         if (li instanceof DelegatingCacheEntryListenerInvocation) {
            ((DelegatingCacheEntryListenerInvocation<K, V>) li).unregister();
         }
      }
      return markedForRemoval;
   }
}
