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
import static org.infinispan.notifications.cachelistener.event.Event.Type.TOPOLOGY_CHANGED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.TRANSACTION_COMPLETED;
import static org.infinispan.notifications.cachelistener.event.Event.Type.TRANSACTION_REGISTERED;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.compat.TypeConverter;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutionCompletionService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
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
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
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
import org.infinispan.notifications.cachelistener.filter.KeyFilterAsCacheEventFilter;
import org.infinispan.notifications.impl.AbstractListenerImpl;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Helper class that handles all notifications to registered listeners.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @author William Burns
 * @author anistor@redhat.com
 * @since 4.0
 */
public final class CacheNotifierImpl<K, V> extends AbstractListenerImpl<Event<K, V>, CacheEntryListenerInvocation<K, V>>
      implements ClusterCacheNotifier<K, V> {

   private static final Log log = LogFactory.getLog(CacheNotifierImpl.class);
   private static final boolean trace = log.isTraceEnabled();

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

   private Cache<K, V> cache;
   private ClusteringDependentLogic clusteringDependentLogic;
   private TransactionManager transactionManager;
   private DistributedExecutorService distExecutorService;
   private Configuration config;
   private DistributionManager distributionManager;
   private InternalEntryFactory entryFactory;
   private ClusterEventManager<K, V> eventManager;
   private ComponentRegistry componentRegistry;
   /**
    * This is the converter registered with the cache.  This should be used on any key/value first before applying another
    * converter, or else the type won't be as expected
    */
   private TypeConverter<K, V, K, V> cacheConverter;

   private final Map<Object, UUID> clusterListenerIDs = new ConcurrentHashMap<>();

   private Collection<FilterIndexingServiceProvider> filterIndexingServiceProviders;

   /**
    * This map is used to store the handler used when a listener is registered which has includeCurrentState and
    * is only used for that listener during the initial state transfer
    */
   private final ConcurrentMap<UUID, QueueingSegmentListener<K, V, ? extends Event<K, V>>> segmentHandler;

   /**
    * An optional converter, to be used for converting the key/value before sending the event to listeners.
    */
   private TypeConverter typeConverter;

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
   }

   @Inject
   void injectDependencies(Cache<K, V> cache, ClusteringDependentLogic clusteringDependentLogic,
                           TransactionManager transactionManager, Configuration config,
                           DistributionManager distributionManager, InternalEntryFactory entryFactory,
           ClusterEventManager<K, V> eventManager) {
      this.cache = cache;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.transactionManager = transactionManager;
      this.config = config;
      this.distributionManager = distributionManager;
      this.entryFactory = entryFactory;
      this.eventManager = eventManager;
   }

   @Override
   public void start() {
      super.start();
      AsyncInterceptorChain interceptorChain = SecurityActions.getAsyncInterceptorChain(cache);
      if (interceptorChain != null && !interceptorChain.getInterceptors().isEmpty()) {
         this.distExecutorService = SecurityActions.getDefaultExecutorService(cache);
      }
      componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      cacheConverter = componentRegistry.getComponent(TypeConverter.class);
      //TODO This is executed twice because component CacheNotifier is also ClusterCacheNotifier (see https://issues.jboss.org/browse/ISPN-5353)
      if (filterIndexingServiceProviders == null) {
         filterIndexingServiceProviders = ServiceFinder.load(FilterIndexingServiceProvider.class);
         for (FilterIndexingServiceProvider provider : filterIndexingServiceProviders) {
            componentRegistry.wireDependencies(provider);
            provider.start();
         }
      }
   }

   @Override
   public void stop() {
      super.stop();
      //TODO This is executed twice because component CacheNotifier is also ClusterCacheNotifier (see https://issues.jboss.org/browse/ISPN-5353)
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
   public void setTypeConverter(TypeConverter typeConverter) {
      this.typeConverter = typeConverter;
   }

   @Override
   protected Map<Class<? extends Annotation>, Class<?>> getAllowedMethodAnnotations(Listener l) {
      if (l.clustered()) {
         // Cluster listeners only allow a subset of types
         return clusterAllowedListeners;
      }
      return allowedListeners;
   }

   private K convertKey(K key) {
      if (typeConverter != null) {
         key = (K) typeConverter.unboxKey(key);
      }
      if (cacheConverter != null) {
         key = cacheConverter.unboxKey(key);
      }
      return key;
   }

   private V convertValue(V value) {
      if (typeConverter != null) {
         value = (V) typeConverter.unboxValue(value);
      }
      if (cacheConverter != null) {
         value = cacheConverter.unboxValue(value);
      }
      return value;
   }

   @Override
   protected final Transaction suspendIfNeeded() {
      if (transactionManager == null) {
         return null;
      }

      try {
         switch (transactionManager.getStatus()) {
            case Status.STATUS_ACTIVE:
            case Status.STATUS_NO_TRANSACTION:
               return null;
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
         if (trace) {
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
         if (trace) {
            log.tracef(e, "An error occurred while trying to resume a suspended transaction. tx=%s", transaction);
         }
      }
   }

   @Override
   public void notifyCacheEntryCreated(K key, V value, Metadata metadata, boolean pre,
                                       InvocationContext ctx, FlagAffectedCommand command) {
      if (!cacheEntryCreatedListeners.isEmpty() && clusteringDependentLogic.commitType(command, ctx, key, false).isLocal()) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_CREATED);
         configureEvent(e, key, value, metadata, pre, ctx, command, null, null);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         boolean sendEvents = !ctx.isInTxScope();
         try {
            for (CacheEntryListenerInvocation<K, V> listener : cacheEntryCreatedListeners) {
               // Need a wrapper per invocation since converter could modify the entry in it
               listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
            }
            if (sendEvents) {
               eventManager.sendEvents();
               sendEvents = false;
            }
         } finally {
            if (sendEvents) {
               eventManager.dropEvents();
            }
         }
      }
   }

   @Override
   public void notifyCacheEntryModified(K key, V value, Metadata metadata, V previousValue, Metadata previousMetadata, boolean pre, InvocationContext ctx,
                                        FlagAffectedCommand command) {
      if (!cacheEntryModifiedListeners.isEmpty() && clusteringDependentLogic.commitType(command, ctx, key, false).isLocal()) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_MODIFIED);
         configureEvent(e, key, value, metadata, pre, ctx, command, previousValue, previousMetadata);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         boolean sendEvents = !ctx.isInTxScope();
         try {
            for (CacheEntryListenerInvocation<K, V> listener : cacheEntryModifiedListeners) {
               // Need a wrapper per invocation since converter could modify the entry in it
               listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
            }
            if (sendEvents) {
               eventManager.sendEvents();
               sendEvents = false;
            }
         } finally {
            if (sendEvents) {
               eventManager.dropEvents();
            }
         }
      }
   }

   @Override
   public void notifyCacheEntryRemoved(K key, V previousValue, Metadata previousMetadata, boolean pre,
                                       InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryRemovedListeners) && clusteringDependentLogic.commitType(command, ctx, key, true).isLocal()) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_REMOVED);
         if (pre) {
            configureEvent(e, key, previousValue, previousMetadata, true, ctx, command, previousValue, previousMetadata);
         } else {
            // to be consistent it would be better to pass null as previousMetadata but certain server code
            // depends on ability to retrieve these metadata when pre=false from CacheEntryEvent.getMetadata
            // instead of having proper method getOldMetadata() there.
            configureEvent(e, key, null, previousMetadata, false, ctx, command, previousValue, previousMetadata);
         }
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         boolean sendEvents = !ctx.isInTxScope();
         try {
            for (CacheEntryListenerInvocation<K, V> listener : cacheEntryRemovedListeners) {
               // Need a wrapper per invocation since converter could modify the entry in it
               listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
            }
            if (sendEvents) {
               eventManager.sendEvents();
               sendEvents = false;
            }
         } finally {
            if (sendEvents) {
               eventManager.dropEvents();
            }
         }
      }
   }

   /**
    * Configure event data. Currently used for 'created', 'modified', 'removed', 'invalidated' events.
    */
   private void configureEvent(EventImpl<K, V> e, K key, V value, Metadata metadata, boolean pre, InvocationContext ctx,
                               FlagAffectedCommand command, V previousValue, Metadata previousMetadata) {
      key = convertKey(key);
      value = convertValue(value);
      previousValue = convertValue(previousValue);

      e.setOriginLocal(ctx.isOriginLocal());
      e.setValue(pre ? previousValue : value);
      e.setPre(pre);
      e.setOldValue(previousValue);
      e.setOldMetadata(previousMetadata);
      e.setMetadata(metadata);
      if (command != null && command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         e.setCommandRetried(true);
      }
      e.setKey(key);
      setTx(ctx, e);
   }

   /**
    * Configure event data. Currently used for 'activated', 'loaded', 'visited' events.
    */
   private void configureEvent(EventImpl<K, V> e, K key, V value, boolean pre, InvocationContext ctx) {
      e.setPre(pre);
      e.setKey(convertKey(key));
      e.setValue(convertValue(value));
      e.setOriginLocal(ctx.isOriginLocal());
      setTx(ctx, e);
   }

   /**
    * Configure event data. Currently used for 'expired' events.
    */
   private void configureEvent(EventImpl<K, V> e, K key, V value, Metadata metadata) {
      e.setKey(convertKey(key));
      e.setValue(convertValue(value));
      e.setMetadata(metadata);
      e.setOriginLocal(true);
      e.setPre(false);
   }

   @Override
   public void notifyCacheEntryVisited(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryVisitedListeners)) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_VISITED);
         configureEvent(e, key, value, pre, ctx);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (CacheEntryListenerInvocation<K, V> listener : cacheEntryVisitedListeners) {
            // Need a wrapper per invocation since converter could modify the entry in it
            listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
         }
      }
   }

   @Override
   public void notifyCacheEntriesEvicted(Collection<InternalCacheEntry<? extends K, ? extends V>> entries, InvocationContext ctx, FlagAffectedCommand command) {
      if (!entries.isEmpty()) {
         if (isNotificationAllowed(command, cacheEntriesEvictedListeners)) {
            EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
            Map<K, V> evictedKeysAndValues;
            if (cacheConverter != null) {
               evictedKeysAndValues = entries.stream().collect(Collectors.toMap(
                     entry -> convertKey(entry.getKey()), entry -> convertValue(entry.getValue())));
            } else {
               evictedKeysAndValues = entries.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }

            e.setEntries(evictedKeysAndValues);
            for (CacheEntryListenerInvocation<K, V> listener : cacheEntriesEvictedListeners) listener.invoke(e);
         }
      }
   }

   @Override
   public void notifyCacheEntryExpired(K key, V value, Metadata metadata, InvocationContext ctx) {
      if (isNotificationAllowed(null, cacheEntryExpiredListeners)) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_EXPIRED);
         configureEvent(e, key, value, metadata);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);

         boolean sendEvents = ctx == null || !ctx.isInTxScope();
         try {
            for (CacheEntryListenerInvocation<K, V> listener : cacheEntryExpiredListeners) {
               // Need a wrapper per invocation since converter could modify the entry in it
               listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
            }
            if (sendEvents) {
               eventManager.sendEvents();
               sendEvents = false;
            }
         } finally {
            if (sendEvents) {
               eventManager.dropEvents();
            }
         }
      }
   }

   @Override
   public void notifyCacheEntryInvalidated(final K key, V value, Metadata metadata,
                                           final boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryInvalidatedListeners)) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_INVALIDATED);
         configureEvent(e, key, value, metadata, pre, ctx, command, value, metadata);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (CacheEntryListenerInvocation<K, V> listener : cacheEntryInvalidatedListeners) {
            // Need a wrapper per invocation since converter could modify the entry in it
            listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
         }
      }
   }

   @Override
   public void notifyCacheEntryLoaded(K key, V value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryLoadedListeners)) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_LOADED);
         configureEvent(e, key, value, pre, ctx);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (CacheEntryListenerInvocation<K, V> listener : cacheEntryLoadedListeners) {
            // Need a wrapper per invocation since converter could modify the entry in it
            listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
         }
      }
   }

   @Override
   public void notifyCacheEntryActivated(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryActivatedListeners)) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_ACTIVATED);
         configureEvent(e, key, value, pre, ctx);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (CacheEntryListenerInvocation<K, V> listener : cacheEntryActivatedListeners) {
            // Need a wrapper per invocation since converter could modify the entry in it
            listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
         }
      }
   }

   private void setTx(InvocationContext ctx, EventImpl<K, V> e) {
      if (ctx != null && ctx.isInTxScope()) {
         GlobalTransaction tx = ((TxInvocationContext) ctx).getGlobalTransaction();
         e.setTransactionId(tx);
      }
   }

   @Override
   public void notifyCacheEntryPassivated(K key, V value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryPassivatedListeners)) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, CACHE_ENTRY_PASSIVATED);
         if (typeConverter != null) {
            key = (K) typeConverter.unboxKey(key);
            value = (V) typeConverter.unboxValue(value);
         }
         if (cacheConverter != null) {
            key = (K) cacheConverter.unboxKey(key);
            value = (V) cacheConverter.unboxValue(value);
         }
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (CacheEntryListenerInvocation<K, V> listener : cacheEntryPassivatedListeners) {
            // Need a wrapper per invocation since converter could modify the entry in it
            listener.invoke(new EventWrapper<>(e.getKey(), e), isLocalNodePrimaryOwner);
         }
      }
   }

   @Override
   public void notifyTransactionCompleted(GlobalTransaction transaction, boolean successful, InvocationContext ctx) {
      if (!transactionCompletedListeners.isEmpty()) {
         boolean isOriginLocal = ctx.isOriginLocal();
         EventImpl<K, V> e = EventImpl.createEvent(cache, TRANSACTION_COMPLETED);
         e.setOriginLocal(isOriginLocal);
         e.setTransactionId(transaction);
         e.setTransactionSuccessful(successful);
         for (CacheEntryListenerInvocation<K, V> listener : transactionCompletedListeners) listener.invoke(e);
         if (ctx.isInTxScope()) {
            if (successful) {
               eventManager.sendEvents();
            } else {
               eventManager.dropEvents();
            }
         }
      }
   }

   @Override
   public void notifyTransactionRegistered(GlobalTransaction globalTransaction, boolean isOriginLocal) {
      if (!transactionRegisteredListeners.isEmpty()) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, TRANSACTION_REGISTERED);
         e.setOriginLocal(isOriginLocal);
         e.setTransactionId(globalTransaction);
         for (CacheEntryListenerInvocation<K, V> listener : transactionRegisteredListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyDataRehashed(ConsistentHash oldCH, ConsistentHash newCH, ConsistentHash unionCH, int newTopologyId, boolean pre) {
      if (!dataRehashedListeners.isEmpty()) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, DATA_REHASHED);
         e.setPre(pre);
         e.setReadConsistentHashAtStart(oldCH);
         e.setWriteConsistentHashAtStart(oldCH);
         e.setReadConsistentHashAtEnd(newCH);
         e.setWriteConsistentHashAtEnd(newCH);
         e.setUnionConsistentHash(unionCH);
         e.setNewTopologyId(newTopologyId);
         for (CacheEntryListenerInvocation<K, V> listener : dataRehashedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyTopologyChanged(CacheTopology oldTopology, CacheTopology newTopology, int newTopologyId, boolean pre) {
      if (!topologyChangedListeners.isEmpty()) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, TOPOLOGY_CHANGED);
         e.setPre(pre);
         if (oldTopology != null) {
            e.setReadConsistentHashAtStart(oldTopology.getReadConsistentHash());
            e.setWriteConsistentHashAtStart(oldTopology.getWriteConsistentHash());
         }
         e.setReadConsistentHashAtEnd(newTopology.getReadConsistentHash());
         e.setWriteConsistentHashAtEnd(newTopology.getWriteConsistentHash());
         e.setNewTopologyId(newTopologyId);
         for (CacheEntryListenerInvocation<K, V> listener : topologyChangedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyPartitionStatusChanged(AvailabilityMode mode, boolean pre) {
      if (!partitionChangedListeners.isEmpty()) {
         EventImpl<K, V> e = EventImpl.createEvent(cache, PARTITION_STATUS_CHANGED);
         e.setPre(pre);
         e.setAvailabilityMode(mode);
         for (CacheEntryListenerInvocation<K, V> listener : partitionChangedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyClusterListeners(Collection<? extends CacheEntryEvent<K, V>> events, UUID uuid) {
      // We don't need to unwrap key or value as the node where the event originated did this already
      for (CacheEntryEvent<K, V> event : events) {
         if (event.isPre()) {
            throw new IllegalArgumentException("Events for cluster listener should never be pre change");
         }
         switch (event.getType()) {
            case CACHE_ENTRY_MODIFIED:
               for (CacheEntryListenerInvocation<K, V> listener : cacheEntryModifiedListeners) {
                  if (listener.isClustered() && uuid.equals(listener.getIdentifier())) {
                     // We force invocation, since it means the owning node passed filters already and they
                     // already converted so don't run converter either
                     listener.invokeNoChecks(new EventWrapper<>(event.getKey(),event), false, true);
                  }
               }
               break;
            case CACHE_ENTRY_CREATED:
               for (CacheEntryListenerInvocation<K, V> listener : cacheEntryCreatedListeners) {
                  if (listener.isClustered() && uuid.equals(listener.getIdentifier())) {
                     // We force invocation, since it means the owning node passed filters already and they
                     // already converted so don't run converter either
                     listener.invokeNoChecks(new EventWrapper<>(event.getKey(),event), false, true);
                  }
               }
               break;
            case CACHE_ENTRY_REMOVED:
               for (CacheEntryListenerInvocation<K, V> listener : cacheEntryRemovedListeners) {
                  if (listener.isClustered() && uuid.equals(listener.getIdentifier())) {
                     // We force invocation, since it means the owning node passed filters already and they
                     // already converted so don't run converter either
                     listener.invokeNoChecks(new EventWrapper<>(event.getKey(),event), false, true);
                  }
               }
               break;
            case CACHE_ENTRY_EXPIRED:
               cacheEntryExpiredListeners.forEach(listener -> {
                  if (listener.isClustered() && uuid.equals(listener.getIdentifier())) {
                     // We force invocation, since it means the owning node passed filters already and they
                     // already converted so don't run converter either
                     listener.invokeNoChecks(new EventWrapper<>(event.getKey(),event), false, true);
                  }
               });
               break;
            default:
               throw new IllegalArgumentException("Unexpected event type encountered!");
         }
      }
   }

   @Override
   public Collection<DistributedCallable> retrieveClusterListenerCallablesToInstall() {
      Set<Object> enlistedAlready = new HashSet<>();
      Set<DistributedCallable> callables = new HashSet<>();

      if (trace) {
         log.tracef("Request received to get cluster listeners currently registered");
      }

      registerClusterListenerCallablesToInstall(enlistedAlready, callables, cacheEntryModifiedListeners);
      registerClusterListenerCallablesToInstall(enlistedAlready, callables, cacheEntryCreatedListeners);
      registerClusterListenerCallablesToInstall(enlistedAlready, callables, cacheEntryRemovedListeners);

      if (trace) {
         log.tracef("Cluster listeners found %s", callables);
      }

      return callables;
   }

   private void registerClusterListenerCallablesToInstall(Set<Object> enlistedAlready,
                                                          Set<DistributedCallable> callables,
                                                          List<CacheEntryListenerInvocation<K, V>> listenerInvocations) {
      for (CacheEntryListenerInvocation<K, V> listener : listenerInvocations) {
         if (!enlistedAlready.contains(listener.getTarget())) {
            // If clustered means it is local - so use our address
            if (listener.isClustered()) {
               Set<Class<? extends Annotation>> filterAnnotations = listener.getFilterAnnotations();
               callables.add(new ClusterListenerReplicateCallable(listener.getIdentifier(),
                                                                  cache.getCacheManager().getAddress(), listener.getFilter(),
                                                                  listener.getConverter(), listener.isSync(),
                                                                  filterAnnotations));
               enlistedAlready.add(listener.getTarget());
            }
            else if (listener.getTarget() instanceof RemoteClusterListener) {
               RemoteClusterListener lcl = (RemoteClusterListener)listener.getTarget();
               Set<Class<? extends Annotation>> filterAnnotations = listener.getFilterAnnotations();
               callables.add(new ClusterListenerReplicateCallable(lcl.getId(), lcl.getOwnerAddress(), listener.getFilter(),
                                                                  listener.getConverter(), listener.isSync(),
                                                                  filterAnnotations));
               enlistedAlready.add(listener.getTarget());
            }
         }
      }
   }

   public boolean isNotificationAllowed(FlagAffectedCommand cmd, List<CacheEntryListenerInvocation<K, V>> listeners) {
      return !listeners.isEmpty() && (cmd == null || !cmd.hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION));
   }

   @Override
   public void addListener(Object listener) {
      addListener(listener, null, null, null);
   }

   @Override
   public void addListener(Object listener, ClassLoader classLoader) {
      addListener(listener, null, null, classLoader);
   }

   @Override
   public void addListener(Object listener, KeyFilter<? super K> filter, ClassLoader classLoader) {
      addListener(listener, new KeyFilterAsCacheEventFilter<>(filter), null, classLoader);
   }

   /**
    * Adds the listener using the provided filter converter and class loader.  The provided builder is used to add
    * additional configuration including (clustered, onlyPrimary & identifier) which can be used after this method
    * is completed to see what values were used in the addition of this listener
    * @param listener
    * @param filter
    * @param converter
    * @param classLoader
    * @param <C>
    * @return
    */
   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter,
                                           CacheEventConverter<? super K, ? super V, C> converter, ClassLoader classLoader) {
      final Listener l = testListenerClassValidity(listener.getClass());
      final UUID generatedId = UUID.randomUUID();
      final CacheMode cacheMode = config.clustering().cacheMode();

      FilterIndexingServiceProvider indexingProvider = null;
      boolean foundMethods = false;
      if (filter instanceof IndexedFilter) {
         IndexedFilter indexedFilter = (IndexedFilter) filter;
         indexingProvider = findIndexingServiceProvider(indexedFilter);
         if (indexingProvider != null) {
            DelegatingCacheInvocationBuilder builder = new DelegatingCacheInvocationBuilder(indexingProvider);
            builder
                  .setIncludeCurrentState(l.includeCurrentState())
                  .setClustered(l.clustered())
                  .setOnlyPrimary(l.clustered() ? cacheMode.isDistributed() : l.primaryOnly())
                  .setObservation(l.clustered() ? Listener.Observation.POST : l.observation())
                  .setFilter(filter)
                  .setConverter(converter)
                  .setIdentifier(generatedId)
                  .setClassLoader(classLoader);
            foundMethods = validateAndAddListenerInvocations(listener, builder);
            builder.registerListenerInvocations();
         }
      }
      if (indexingProvider == null) {
         CacheInvocationBuilder builder = new CacheInvocationBuilder();
         builder
               .setIncludeCurrentState(l.includeCurrentState())
               .setClustered(l.clustered())
               .setOnlyPrimary(l.clustered() ? cacheMode.isDistributed() : l.primaryOnly())
               .setObservation(l.clustered() ? Listener.Observation.POST : l.observation())
               .setFilter(filter)
               .setConverter(converter)
               .setIdentifier(generatedId)
               .setClassLoader(classLoader);

         if (l.clustered()) {
            builder.setFilterAnnotations(findListenerCallbacks(listener));
         }

         foundMethods = validateAndAddListenerInvocations(listener, builder);
      }

      if (foundMethods && l.clustered()) {
         if (l.observation() == Listener.Observation.PRE) {
            throw log.clusterListenerRegisteredWithOnlyPreEvents(listener.getClass());
         } else if (cacheMode.isInvalidation()) {
            throw new UnsupportedOperationException("Cluster listeners cannot be used with Invalidation Caches!");
         } else if (cacheMode.isDistributed()) {
            clusterListenerIDs.put(listener, generatedId);
            EmbeddedCacheManager manager = cache.getCacheManager();
            Address ourAddress = manager.getAddress();

            List<Address> members = manager.getMembers();
            // If we are the only member don't even worry about sending listeners
            if (members != null && members.size() > 1) {
               DistributedExecutionCompletionService decs = new DistributedExecutionCompletionService(distExecutorService);

               if (trace) {
                  log.tracef("Replicating cluster listener to other nodes %s for cluster listener with id %s",
                             members, generatedId);
               }
               Callable callable = new ClusterListenerReplicateCallable(
                     generatedId, ourAddress, filter, converter, l.sync(),
                     findListenerCallbacks(listener));
               for (Address member : members) {
                  if (!member.equals(ourAddress)) {
                     decs.submit(member, callable);
                  }
               }

               for (int i = 0; i < members.size() - 1; ++i) {
                  try {
                     decs.take().get();
                  } catch (InterruptedException e) {
                     throw new CacheListenerException(e);
                  } catch (ExecutionException e) {
                     Throwable cause = e.getCause();
                     // If we got a SuspectException it means the remote node hasn't started this cache yet.
                     // Just ignore, when it joins it will retrieve the listener
                     if (!(cause instanceof SuspectException)) {
                        throw new CacheListenerException(cause);
                     }
                  }
               }

               int extraCount = 0;
               // If anyone else joined since we sent these we have to send the listeners again, since they may have queried
               // before the other nodes got the new listener
               List<Address> membersAfter = manager.getMembers();
               for (Address member : membersAfter) {
                  if (!members.contains(member) && !member.equals(ourAddress)) {
                     if (trace) {
                        log.tracef("Found additional node %s that joined during replication of cluster listener with id %s",
                                   member, generatedId);
                     }
                     extraCount++;
                     decs.submit(member, callable);
                  }
               }

               for (int i = 0; i < extraCount; ++i) {
                  try {
                     decs.take().get();
                  } catch (InterruptedException e) {
                     throw new CacheListenerException(e);
                  } catch (ExecutionException e) {
                     throw new CacheListenerException(e);
                  }
               }
            }
         }
      }

      // If we have a segment listener handler, it means we have to do initial state
      QueueingSegmentListener handler = segmentHandler.remove(generatedId);
      if (handler != null) {
         if (trace) {
            log.tracef("Listener %s requests initial state for cache", generatedId);
         }

         try (CacheStream<CacheEntry<K, V>> entryStream = cache.getAdvancedCache().cacheEntrySet().stream()) {
            Stream<CacheEntry<K, V>> usedStream = entryStream.segmentCompletionListener(handler);

            if (filter instanceof CacheEventFilterConverter && (filter == converter || converter == null)) {
               // Hacky cast to prevent other casts
               usedStream = CacheFilters.filterAndConvert(usedStream,
                       new CacheEventFilterConverterAsKeyValueFilterConverter<>((CacheEventFilterConverter<K, V, V>) filter));
            } else {
               usedStream = filter == null ? usedStream : usedStream.filter(CacheFilters.predicate(
                       new CacheEventFilterAsKeyValueFilter<>(filter)));
               usedStream = converter == null ? usedStream : usedStream.map(CacheFilters.function(
                       new CacheEventConverterAsConverter(converter)));
            }

            Iterator<CacheEntry<K, V>> iterator = usedStream.iterator();
            while (iterator.hasNext()) {
               CacheEntry<K, V> entry = iterator.next();
               // Mark the key as processed and see if we had a concurrent update
               Object value = handler.markKeyAsProcessing(entry.getKey());
               if (value == BaseQueueingSegmentListener.REMOVED) {
                  // Don't process this value if we had a concurrent remove
                  continue;
               }
               raiseEventForInitialTransfer(generatedId, entry, l.clustered());

               handler.notifiedKey(entry.getKey());
            }
         }

         Set<CacheEntry> entries = handler.findCreatedEntries();

         for (CacheEntry entry : entries) {
            raiseEventForInitialTransfer(generatedId, entry, l.clustered());
         }

         if (trace) {
            log.tracef("Listener %s initial state for cache completed", generatedId);
         }

         handler.transferComplete();
      }
   }

   private FilterIndexingServiceProvider findIndexingServiceProvider(IndexedFilter indexedFilter) {
      for (FilterIndexingServiceProvider provider : filterIndexingServiceProviders) {
         if (provider.supportsFilter(indexedFilter)) {
            return provider;
         }
      }
      log.noFilterIndexingServiceProviderFound(indexedFilter.getClass().getName());
      return null;
   }

   @Override
   public List<CacheEntryListenerInvocation<K, V>> getListenerCollectionForAnnotation(Class<? extends Annotation> annotation) {
      return super.getListenerCollectionForAnnotation(annotation);
   }

   private void raiseEventForInitialTransfer(UUID identifier, CacheEntry entry, boolean clustered) {
      EventImpl preEvent;
      if (clustered) {
         // In clustered mode we only send post event
         preEvent = null;
      } else {
         preEvent = EventImpl.createEvent(cache, CACHE_ENTRY_CREATED);
         preEvent.setKey(entry.getKey());
         preEvent.setPre(true);
      }

      EventImpl postEvent = EventImpl.createEvent(cache, CACHE_ENTRY_CREATED);
      postEvent.setKey(entry.getKey());
      postEvent.setValue(entry.getValue());
      postEvent.setMetadata(entry.getMetadata());
      postEvent.setPre(false);

      for (CacheEntryListenerInvocation<K, V> invocation : cacheEntryCreatedListeners) {
         // Now notify all our methods of the creates
         if (invocation.getIdentifier() == identifier) {
            if (preEvent != null) {
               // Non clustered notifications are done twice
               invocation.invokeNoChecks(new EventWrapper<>(null, preEvent), true, true);
            }
            invocation.invokeNoChecks(new EventWrapper<>(null, postEvent), true, true);
         }
      }
   }

   @Override
   public void addListener(Object listener, KeyFilter<? super K> filter) {
      addListener(listener, filter, null);
   }

   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
      addListener(listener, filter, converter, null);
   }

   @Override
   public <C> void addFilteredListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
         Set<Class<? extends Annotation>> filterAnnotations) {
      final Listener l = testListenerClassValidity(listener.getClass());
      final UUID generatedId = UUID.randomUUID();
      final CacheMode cacheMode = config.clustering().cacheMode();

      FilterIndexingServiceProvider indexingProvider = null;
      boolean foundMethods = false;
      if (filter instanceof IndexedFilter) {
         IndexedFilter indexedFilter = (IndexedFilter) filter;
         indexingProvider = findIndexingServiceProvider(indexedFilter);
         if (indexingProvider != null) {
            DelegatingCacheInvocationBuilder builder = new DelegatingCacheInvocationBuilder(indexingProvider);
            builder
                  .setFilterAnnotations(filterAnnotations)
                  .setIncludeCurrentState(l.includeCurrentState())
                  .setClustered(l.clustered())
                  .setOnlyPrimary(l.clustered() ? cacheMode.isDistributed() : l.primaryOnly())
                  .setObservation(l.clustered() ? Listener.Observation.POST : l.observation())
                  .setFilter(filter)
                  .setConverter(converter)
                  .setIdentifier(generatedId)
                  .setClassLoader(null);
            foundMethods = validateAndAddFilterListenerInvocations(listener, builder, filterAnnotations);
            builder.registerListenerInvocations();
         }
      }
      if (indexingProvider == null) {
         CacheInvocationBuilder builder = new CacheInvocationBuilder();
         builder
               .setFilterAnnotations(filterAnnotations)
               .setIncludeCurrentState(l.includeCurrentState())
               .setClustered(l.clustered())
               .setOnlyPrimary(l.clustered() ? cacheMode.isDistributed() : l.primaryOnly())
               .setObservation(l.clustered() ? Listener.Observation.POST : l.observation())
               .setFilter(filter)
               .setConverter(converter)
               .setIdentifier(generatedId)
               .setClassLoader(null);

         if (l.clustered()) {
            builder.setFilterAnnotations(findListenerCallbacks(listener));
         }

         foundMethods = validateAndAddFilterListenerInvocations(listener, builder, filterAnnotations);
      }

      if (foundMethods && l.clustered()) {
         if (l.observation() == Listener.Observation.PRE) {
            throw log.clusterListenerRegisteredWithOnlyPreEvents(listener.getClass());
         } else if (cacheMode.isInvalidation()) {
            throw new UnsupportedOperationException("Cluster listeners cannot be used with Invalidation Caches!");
         } else if (cacheMode.isDistributed()) {
            clusterListenerIDs.put(listener, generatedId);
            EmbeddedCacheManager manager = cache.getCacheManager();
            Address ourAddress = manager.getAddress();

            List<Address> members = manager.getMembers();
            // If we are the only member don't even worry about sending listeners
            if (members != null && members.size() > 1) {
               DistributedExecutionCompletionService decs = new DistributedExecutionCompletionService(distExecutorService);

               if (trace) {
                  log.tracef("Replicating cluster listener to other nodes %s for cluster listener with id %s",
                        members, generatedId);
               }
               Callable callable = new ClusterListenerReplicateCallable(
                     generatedId, ourAddress, filter, converter, l.sync(),
                     filterAnnotations);
               for (Address member : members) {
                  if (!member.equals(ourAddress)) {
                     decs.submit(member, callable);
                  }
               }

               for (int i = 0; i < members.size() - 1; ++i) {
                  try {
                     decs.take().get();
                  } catch (InterruptedException e) {
                     throw new CacheListenerException(e);
                  } catch (ExecutionException e) {
                     Throwable cause = e.getCause();
                     // If we got a SuspectException it means the remote node hasn't started this cache yet.
                     // Just ignore, when it joins it will retrieve the listener
                     if (!(cause instanceof SuspectException)) {
                        throw new CacheListenerException(cause);
                     }
                  }
               }

               int extraCount = 0;
               // If anyone else joined since we sent these we have to send the listeners again, since they may have queried
               // before the other nodes got the new listener
               List<Address> membersAfter = manager.getMembers();
               for (Address member : membersAfter) {
                  if (!members.contains(member) && !member.equals(ourAddress)) {
                     if (trace) {
                        log.tracef("Found additional node %s that joined during replication of cluster listener with id %s",
                              member, generatedId);
                     }
                     extraCount++;
                     decs.submit(member, callable);
                  }
               }

               for (int i = 0; i < extraCount; ++i) {
                  try {
                     decs.take().get();
                  } catch (InterruptedException e) {
                     throw new CacheListenerException(e);
                  } catch (ExecutionException e) {
                     throw new CacheListenerException(e);
                  }
               }
            }
         }
      }

      // If we have a segment listener handler, it means we have to do initial state
      QueueingSegmentListener handler = segmentHandler.remove(generatedId);
      if (handler != null) {
         if (trace) {
            log.tracef("Listener %s requests initial state for cache", generatedId);
         }

         try (CacheStream<CacheEntry<K, V>> entryStream = cache.getAdvancedCache().cacheEntrySet().stream()) {
            Stream<CacheEntry<K, V>> usedStream = entryStream.segmentCompletionListener(handler);

            if (filter instanceof CacheEventFilterConverter && (filter == converter || converter == null)) {
               // Hacky cast to prevent other casts
               usedStream = CacheFilters.filterAndConvert(usedStream,
                     new CacheEventFilterConverterAsKeyValueFilterConverter<>((CacheEventFilterConverter<K, V, V>) filter));
            } else {
               usedStream = filter == null ? usedStream : usedStream.filter(CacheFilters.predicate(
                     new CacheEventFilterAsKeyValueFilter<>(filter)));
               usedStream = converter == null ? usedStream : usedStream.map(CacheFilters.function(
                     new CacheEventConverterAsConverter(converter)));
            }

            Iterator<CacheEntry<K, V>> iterator = usedStream.iterator();
            while (iterator.hasNext()) {
               CacheEntry<K, V> entry = iterator.next();
               // Mark the key as processed and see if we had a concurrent update
               Object value = handler.markKeyAsProcessing(entry.getKey());
               if (value == BaseQueueingSegmentListener.REMOVED) {
                  // Don't process this value if we had a concurrent remove
                  continue;
               }
               raiseEventForInitialTransfer(generatedId, entry, l.clustered());

               handler.notifiedKey(entry.getKey());
            }
         }

         Set<CacheEntry> entries = handler.findCreatedEntries();

         for (CacheEntry entry : entries) {
            raiseEventForInitialTransfer(generatedId, entry, l.clustered());
         }

         if (trace) {
            log.tracef("Listener %s initial state for cache completed", generatedId);
         }

         handler.transferComplete();
      }
   }

   protected class CacheInvocationBuilder extends AbstractInvocationBuilder {
      CacheEventFilter<? super K, ? super V> filter;
      CacheEventConverter<? super K, ? super V, ?> converter;
      boolean onlyPrimary;
      boolean clustered;
      boolean includeCurrentState;
      UUID identifier;
      Listener.Observation observation;
      Set<Class<? extends Annotation>> filterAnnotations;

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
         ListenerInvocation<Event<K, V>> invocation = new ListenerInvocationImpl(target, method, sync, classLoader,
                                                                                 subject);

         wireFilterAndConverterDependencies(filter, converter);

         // If we are dealing with clustered events that forces the cluster listener to only use primary only else we would
         // have duplicate events
         CacheEntryListenerInvocation<K, V> returnValue;

         if (includeCurrentState) {
            // If it is a clustered listener and distributed cache we can do some extra optimizations
            if (clustered) {
               QueueingSegmentListener handler = segmentHandler.get(identifier);
               if (handler == null) {
                  if (config.clustering().cacheMode().isDistributed()) {
                     handler = new DistributedQueueingSegmentListener(entryFactory, distributionManager);
                  } else {
                     handler = new QueueingAllSegmentListener(entryFactory);
                  }
                  QueueingSegmentListener currentQueue = segmentHandler.putIfAbsent(identifier, handler);
                  if (currentQueue != null) {
                     handler = currentQueue;
                  }
               }
               returnValue = new ClusteredListenerInvocation<>(invocation, handler, filter, converter, annotation,
                                                                   onlyPrimary, identifier, sync, observation, filterAnnotations);
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
               returnValue = new BaseCacheEntryListenerInvocation(invocation, filter, converter, annotation,
                                                                  onlyPrimary, clustered, identifier, sync, observation, filterAnnotations);
            }
         } else {
            // If no includeCurrentState just use the base listener invocation which immediately passes all notifications
            // off
            returnValue = new BaseCacheEntryListenerInvocation(invocation, filter, converter, annotation, onlyPrimary,
                                                               clustered, identifier, sync, observation, filterAnnotations);
         }
         return returnValue;
      }

      private <C> void wireFilterAndConverterDependencies(CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
         if (filter != null) {
            componentRegistry.wireDependencies(filter);
         }
         if (converter != null && converter != filter) {
            componentRegistry.wireDependencies(converter);
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
            provider.registerListenerInvocations(clustered, onlyPrimary, filterAndConvert, (IndexedFilter<?, ?, ?>) filter, listeners);
         }
      }
   }

   protected class NonClusteredListenerInvocation extends BaseCacheEntryListenerInvocation<K, V> {

      private final QueueingSegmentListener<K, V, Event<K, V>> handler;

      protected NonClusteredListenerInvocation(ListenerInvocation<Event<K, V>> invocation,
                                               QueueingSegmentListener<K, V, Event<K, V>> handler,
                                               CacheEventFilter<? super K, ? super V> filter,
                                               CacheEventConverter<? super K, ? super V, ?> converter,
                                               Class<? extends Annotation> annotation, boolean onlyPrimary,
                                               UUID identifier, boolean sync, Listener.Observation observation,
                                               Set<Class<? extends Annotation>> filterAnnotations) {
         super(invocation, filter, converter, annotation, onlyPrimary, false, identifier, sync, observation, filterAnnotations);
         this.handler = handler;
      }

      @Override
      protected void doRealInvocation(Event<K, V> event) {
         if (!handler.handleEvent(new EventWrapper<>(null, event), invocation)) {
            super.doRealInvocation(event);
         }
      }
   }

   /**
    * This class is to be used with cluster listener invocations only when they have included current state.  Thus
    * we can assume all types are CacheEntryEvent, since it doesn't allow other types.
    */
   protected static class ClusteredListenerInvocation<K, V> extends BaseCacheEntryListenerInvocation<K, V> {

      private final QueueingSegmentListener<K, V, CacheEntryEvent<K, V>> handler;

      public ClusteredListenerInvocation(ListenerInvocation<Event<K, V>> invocation, QueueingSegmentListener<K, V, CacheEntryEvent<K, V>> handler,
                                         CacheEventFilter<? super K, ? super V> filter,
                                         CacheEventConverter<? super K, ? super V, ?> converter,
                                         Class<? extends Annotation> annotation, boolean onlyPrimary,
                                         UUID identifier, boolean sync, Listener.Observation observation,
                                         Set<Class<? extends Annotation>> filterAnnotations) {
         super(invocation, filter, converter, annotation, onlyPrimary, true, identifier, sync, observation, filterAnnotations);
         this.handler = handler;
      }

      @Override
      public void invoke(Event<K, V> event) {
         throw new UnsupportedOperationException("Clustered initial transfer don't support regular events!");
      }

      @Override
      protected void doRealInvocation(EventWrapper<K, V, CacheEntryEvent<K, V>> wrapped) {
         // This is only used with clusters and such we can safely cast this here
         if (!handler.handleEvent(wrapped, invocation)) {
            super.doRealInvocation(wrapped.getEvent());
         }
      }
   }

   protected static class BaseCacheEntryListenerInvocation<K, V> implements CacheEntryListenerInvocation<K, V> {

      protected final ListenerInvocation<Event<K, V>> invocation;
      protected final CacheEventFilter<? super K, ? super V> filter;
      protected final CacheEventConverter<? super K, ? super V, ?> converter;
      protected final boolean onlyPrimary;
      protected final boolean clustered;
      protected final UUID identifier;
      protected final Class<? extends Annotation> annotation;
      protected final boolean sync;
      protected final boolean filterAndConvert;
      protected final Listener.Observation observation;
      protected final Set<Class<? extends Annotation>> filterAnnotations;


      protected BaseCacheEntryListenerInvocation(ListenerInvocation<Event<K, V>> invocation,
                                                 CacheEventFilter<? super K, ? super V> filter,
                                                 CacheEventConverter<? super K, ? super V, ?> converter,
                                                 Class<? extends Annotation> annotation, boolean onlyPrimary,
                                                 boolean clustered, UUID identifier, boolean sync,
                                                 Listener.Observation observation,
                                                 Set<Class<? extends Annotation>> filterAnnotations)  {
         this.invocation = invocation;
         this.filter = filter;
         this.converter = converter;
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
      public void invoke(Event<K, V> event) {
         if (shouldInvoke(event)) {
            doRealInvocation(event);
         }
      }

      /**
       * This is the entry point for local listeners firing events
       * @param wrapped
       * @param isLocalNodePrimaryOwner
       */
      @Override
      public void invoke(EventWrapper<K, V, CacheEntryEvent<K, V>> wrapped, boolean isLocalNodePrimaryOwner) {
         // See if this should be filtered first before evaluating
         CacheEntryEvent<K, V> resultingEvent = shouldInvoke(wrapped.getEvent(), isLocalNodePrimaryOwner);
         if (resultingEvent != null) {
            wrapped.setEvent(resultingEvent);
            invokeNoChecks(wrapped, false, filterAndConvert);
         }
      }

      /**
       * This is the entry point for remote listener events being fired
       * @param wrapped
       * @param skipQueue
       */
      @Override
      public void invokeNoChecks(EventWrapper<K, V, CacheEntryEvent<K, V>> wrapped, boolean skipQueue, boolean skipConverter) {
         // We run the converter first, this way the converter doesn't have to run serialized when enqueued and also
         // the handler doesn't have to worry about it
         if (!skipConverter) {
            wrapped.setEvent(convertValue(converter, wrapped.getEvent()));
         }

         if (skipQueue) {
            invocation.invoke(wrapped.getEvent());
         } else {
            doRealInvocation(wrapped);
         }
      }

      protected void doRealInvocation(EventWrapper<K, V, CacheEntryEvent<K, V>> event) {
         doRealInvocation(event.getEvent());
      }

      protected void doRealInvocation(Event<K, V> event) {
         invocation.invoke(event);
      }

      protected boolean shouldInvoke(Event<K, V> event) {
         return observation.shouldInvoke(event.isPre());
      }

      protected CacheEntryEvent<K, V> shouldInvoke(CacheEntryEvent<K, V> event, boolean isLocalNodePrimaryOwner) {
         if (trace) {
            log.tracef("Should invoke %s (filter %s)? (onlyPrimary=%s, isPrimary=%s)", event, filter, onlyPrimary, isLocalNodePrimaryOwner);
         }
         if (onlyPrimary && !isLocalNodePrimaryOwner) return null;
         if (event instanceof EventImpl) {
            EventImpl<K, V> eventImpl = (EventImpl<K, V>)event;
            if (!shouldInvoke(event)) return null;
            EventType eventType;
            // Only use the filter if it was provided and we have an event that we can filter properly
            if (filter != null && (eventType = getEvent(eventImpl)) != null) {
               if (filterAndConvert) {
                  Object newValue = ((CacheEventFilterConverter)filter).filterAndConvert(eventImpl.getKey(),
                        eventImpl.getOldValue(), eventImpl.getOldMetadata(), eventImpl.getValue(),
                        eventImpl.getMetadata(), eventType);
                  if (newValue != null) {
                     EventImpl<K, V> clone = eventImpl.clone();
                     clone.setValue((V) newValue);
                     return clone;
                  } else {
                     return null;
                  }
               } else if (!filter.accept(eventImpl.getKey(), eventImpl.getOldValue(), eventImpl.getOldMetadata(),
                                 eventImpl.getValue(), eventImpl.getMetadata(), eventType)) {
                  return null;
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
               EventImpl<K, V> eventImpl = (EventImpl<K, V>)event;
               Object newValue = converter.convert(eventImpl.getKey(), (V) eventImpl.getOldValue(),
                                 eventImpl.getOldMetadata(), (V) eventImpl.getValue(),
                                 eventImpl.getMetadata(), null);
               if (newValue != eventImpl.getValue()) {
                  EventImpl<K, V> clone = eventImpl.clone();
                  clone.setValue((V) newValue);
                  returnedEvent = clone;
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

      @Override
      public boolean isSync() {
         return sync;
      }
   }

   @Override
   public void removeListener(Object listener) {
      super.removeListener(listener);
      UUID id = clusterListenerIDs.remove(listener);
      if (id != null) {
         List<Future<?>> futures = distExecutorService.submitEverywhere(new ClusterListenerRemoveCallable(id));
         for (Future<?> future : futures) {
            try {
               future.get();
            } catch (InterruptedException e) {
               throw new CacheListenerException(e);
            } catch (ExecutionException e) {
               throw new CacheListenerException(e);
            }
         }
      }
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
