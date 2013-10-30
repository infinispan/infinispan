package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutionCompletionService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.AbstractListenerImpl;
import org.infinispan.notifications.Converter;
import org.infinispan.notifications.KeyFilter;
import org.infinispan.notifications.KeyValueFilter;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerRemoveCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.notifications.cachelistener.cluster.RemoteClusterListener;
import org.infinispan.notifications.cachelistener.event.*;
import org.infinispan.notifications.cachelistener.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.security.auth.Subject;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.infinispan.commons.util.InfinispanCollections.transformCollectionToMap;
import static org.infinispan.notifications.cachelistener.event.Event.Type.*;

/**
 * Helper class that handles all notifications to registered listeners.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public final class CacheNotifierImpl extends AbstractListenerImpl implements ClusterCacheNotifier {

   private static final Log log = LogFactory.getLog(CacheNotifierImpl.class);

   private static final Map<Class<? extends Annotation>, Class<?>> allowedListeners = new HashMap<Class<? extends Annotation>, Class<?>>(16);
   private static final Map<Class<? extends Annotation>, Class<?>> clusterAllowedListeners =
         new HashMap<Class<? extends Annotation>, Class<?>>();

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
      allowedListeners.put(DataRehashed.class, DataRehashedEvent.class);
      allowedListeners.put(TopologyChanged.class, TopologyChangedEvent.class);

      // For backward compat
      allowedListeners.put(CacheEntryEvicted.class, CacheEntryEvictedEvent.class);
   }

   static {
      clusterAllowedListeners.put(CacheEntryCreated.class, CacheEntryCreatedEvent.class);
      clusterAllowedListeners.put(CacheEntryModified.class, CacheEntryModifiedEvent.class);
      clusterAllowedListeners.put(CacheEntryRemoved.class, CacheEntryRemovedEvent.class);
   }

   final List<ListenerInvocation> cacheEntryCreatedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryRemovedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryVisitedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryModifiedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryActivatedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryPassivatedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryLoadedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryInvalidatedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntriesEvictedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> transactionRegisteredListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> transactionCompletedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> dataRehashedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> topologyChangedListeners = new CopyOnWriteArrayList<ListenerInvocation>();

   // For backward compat
   final List<ListenerInvocation> cacheEntryEvictedListeners = new CopyOnWriteArrayList<ListenerInvocation>();

   private Cache<Object, Object> cache;
   private ClusteringDependentLogic clusteringDependentLogic;
   private TransactionManager transactionManager;
   private DistributedExecutorService distExecutorService;
   private Configuration config;

   private final Map<Object, UUID> clusterListenerIDs = new ConcurrentHashMap<Object, UUID>();

   public CacheNotifierImpl() {

      listenersMap.put(CacheEntryCreated.class, cacheEntryCreatedListeners);
      listenersMap.put(CacheEntryRemoved.class, cacheEntryRemovedListeners);
      listenersMap.put(CacheEntryVisited.class, cacheEntryVisitedListeners);
      listenersMap.put(CacheEntryModified.class, cacheEntryModifiedListeners);
      listenersMap.put(CacheEntryActivated.class, cacheEntryActivatedListeners);
      listenersMap.put(CacheEntryPassivated.class, cacheEntryPassivatedListeners);
      listenersMap.put(CacheEntryLoaded.class, cacheEntryLoadedListeners);
      listenersMap.put(CacheEntriesEvicted.class, cacheEntriesEvictedListeners);
      listenersMap.put(TransactionRegistered.class, transactionRegisteredListeners);
      listenersMap.put(TransactionCompleted.class, transactionCompletedListeners);
      listenersMap.put(CacheEntryInvalidated.class, cacheEntryInvalidatedListeners);
      listenersMap.put(DataRehashed.class, dataRehashedListeners);
      listenersMap.put(TopologyChanged.class, topologyChangedListeners);

      // For backward compat
      listenersMap.put(CacheEntryEvicted.class, cacheEntryEvictedListeners);
   }

   @Inject
   void injectDependencies(Cache<Object, Object> cache, ClusteringDependentLogic clusteringDependentLogic,
                           TransactionManager transactionManager, Configuration config) {
      this.cache = cache;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.transactionManager = transactionManager;
      this.config = config;
   }

   @Override
   public void start() {
      super.start();
      this.distExecutorService = new DefaultExecutorService(cache, new WithinThreadExecutor());
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected Map<Class<? extends Annotation>, Class<?>> getAllowedMethodAnnotations() {
      return allowedListeners;
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

   @Override
   public void notifyCacheEntryCreated(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command) {
      if (!cacheEntryCreatedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_CREATED);
         configureEvent(e, key, value, pre, ctx, command);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryCreatedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyCacheEntryModified(Object key, Object value,
         boolean created, boolean pre, InvocationContext ctx,
         FlagAffectedCommand command) {
      if (!cacheEntryModifiedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_MODIFIED);
         configureEvent(e, key, value, pre, ctx, command);
         // Even if CacheEntryCreatedEvent.getValue() has been added, to
         // avoid breaking old behaviour and make it easy to comply with
         // JSR-107 specification TCK, it's necessary to find out whether a
         // modification is the result of a cache entry being created or not.
         // This is needed because on JSR-107, a modification is only fired
         // when the entry is updated, and only one event is fired, so you
         // want to fire it when isPre=false.
         e.setCreated(created);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryModifiedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyCacheEntryRemoved(Object key, Object value, Object oldValue,
         boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryRemovedListeners)) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_REMOVED);
         configureEvent(e, key, value, pre, ctx, command);
         e.setOldValue(oldValue);
         setTx(ctx, e);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryRemovedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   private void configureEvent(EventImpl<Object, Object> e, Object key, Object value, boolean pre,
                               InvocationContext ctx, FlagAffectedCommand command) {
      boolean originLocal = ctx.isOriginLocal();

      e.setOriginLocal(originLocal);
      e.setValue(value);
      e.setPre(pre);
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry != null) {
         e.setMetadata(entry.getMetadata());
      }
      Set<Flag> flags;
      if (command != null && (flags = command.getFlags()) != null && flags.contains(Flag.COMMAND_RETRY)) {
         e.setCommandRetried(true);
      }
      e.setKey(key);
      setTx(ctx, e);
   }

   @Override
   public void notifyCacheEntryVisited(Object key, Object value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryVisitedListeners)) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_VISITED);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         setTx(ctx, e);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryVisitedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyCacheEntriesEvicted(Collection<InternalCacheEntry> entries, InvocationContext ctx, FlagAffectedCommand command) {
      if (!entries.isEmpty()) {
         if (isNotificationAllowed(command, cacheEntriesEvictedListeners)) {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
            Map<Object, Object> evictedKeysAndValues = transformCollectionToMap(entries,
               new InfinispanCollections.MapMakerFunction<Object, Object, InternalCacheEntry>() {
                  @Override
                  public Map.Entry<Object, Object> transform(final InternalCacheEntry input) {
                     return new Map.Entry<Object, Object>() {
                        @Override
                        public Object getKey() {
                          return input.getKey();
                        }

                        @Override
                        public Object getValue() {
                          return input.getValue();
                        }

                        @Override
                        public Object setValue(Object value) {
                          throw new UnsupportedOperationException();
                        }
                     };
                  }
               }
            );

            e.setEntries(evictedKeysAndValues);
            for (ListenerInvocation listener : cacheEntriesEvictedListeners) listener.invoke(e);
         }

         // For backward compat
         if (isNotificationAllowed(command, cacheEntryEvictedListeners)) {
            for (InternalCacheEntry ice : entries) {
               EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
               e.setKey(ice.getKey());
               e.setValue(ice.getValue());
               boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(ice.getKey());
               for (ListenerInvocation listener : cacheEntryEvictedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
            }
         }
      }
   }

   @Override
   public void notifyCacheEntryEvicted(Object key, Object value,
         InvocationContext ctx, FlagAffectedCommand command) {
      boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
      if (isNotificationAllowed(command, cacheEntriesEvictedListeners)) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
         e.setEntries(Collections.singletonMap(key, value));
         for (ListenerInvocation listener : cacheEntriesEvictedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }

      // For backward compat
      if (isNotificationAllowed(command, cacheEntryEvictedListeners)) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
         e.setKey(key);
         e.setValue(value);
         for (ListenerInvocation listener : cacheEntryEvictedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyCacheEntryInvalidated(final Object key, Object value, final boolean pre,
         InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryInvalidatedListeners)) {
         final boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_INVALIDATED);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         setTx(ctx, e);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryInvalidatedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyCacheEntryLoaded(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryLoadedListeners)) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_LOADED);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         setTx(ctx, e);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryLoadedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyCacheEntryActivated(Object key, Object value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryActivatedListeners)) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_ACTIVATED);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         setTx(ctx, e);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryActivatedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   private void setTx(InvocationContext ctx, EventImpl<Object, Object> e) {
      if (ctx != null && ctx.isInTxScope()) {
         GlobalTransaction tx = ((TxInvocationContext) ctx).getGlobalTransaction();
         e.setTransactionId(tx);
      }
   }

   @Override
   public void notifyCacheEntryPassivated(Object key, Object value, boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryPassivatedListeners)) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_PASSIVATED);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryPassivatedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyTransactionCompleted(GlobalTransaction transaction, boolean successful, InvocationContext ctx) {
      if (!transactionCompletedListeners.isEmpty()) {
         boolean isOriginLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, TRANSACTION_COMPLETED);
         e.setOriginLocal(isOriginLocal);
         e.setTransactionId(transaction);
         e.setTransactionSuccessful(successful);
         for (ListenerInvocation listener : transactionCompletedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyTransactionRegistered(GlobalTransaction globalTransaction, boolean isOriginLocal) {
      if (!transactionRegisteredListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, TRANSACTION_REGISTERED);
         e.setOriginLocal(isOriginLocal);
         e.setTransactionId(globalTransaction);
         for (ListenerInvocation listener : transactionRegisteredListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyDataRehashed(ConsistentHash oldCH, ConsistentHash newCH, int newTopologyId, boolean pre) {
      if (!dataRehashedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, DATA_REHASHED);
         e.setPre(pre);
         e.setConsistentHashAtStart(oldCH);
         e.setConsistentHashAtEnd(newCH);
         e.setNewTopologyId(newTopologyId);
         for (ListenerInvocation listener : dataRehashedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyTopologyChanged(ConsistentHash oldConsistentHash, ConsistentHash newConsistentHash, int newTopologyId, boolean pre) {
      if (!topologyChangedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, TOPOLOGY_CHANGED);
         e.setPre(pre);
         e.setConsistentHashAtStart(oldConsistentHash);
         e.setConsistentHashAtEnd(newConsistentHash);
         e.setNewTopologyId(newTopologyId);
         for (ListenerInvocation listener : topologyChangedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyClusterListeners(Collection<? extends Event> events, UUID uuid) {
      for (Event event : events) {
         if (event.isPre()) {
            throw new IllegalArgumentException("Events for cluster listener should never be pre change");
         }
         switch (event.getType()) {
            case CACHE_ENTRY_MODIFIED:
               for (ListenerInvocation listener : cacheEntryModifiedListeners) {
                  if (listener.clustered && uuid.equals(listener.generatedId)) {
                     // We bypass any check to force invocation
                     listener.invoke(event, true, true);
                  }
               }
               break;
            case CACHE_ENTRY_CREATED:
               for (ListenerInvocation listener : cacheEntryCreatedListeners) {
                  if (listener.clustered && uuid.equals(listener.generatedId)) {
                     // We bypass any check to force invocation
                     listener.invoke(event, true, true);
                  }
               }
               break;
            case CACHE_ENTRY_REMOVED:
               for (ListenerInvocation listener : cacheEntryRemovedListeners) {
                  if (listener.clustered && uuid.equals(listener.generatedId)) {
                     // We bypass any check to force invocation
                     listener.invoke(event, true, true);
                  }
               }
               break;
            default:
               throw new IllegalArgumentException("Unexpected event type encountered!");
         }
      }
   }

   @Override
   public Collection<DistributedCallable> retrieveClusterListenerCallablesToInstall() {
      Set<Object> enlistedAlready = new HashSet<Object>();
      Set<DistributedCallable> callables = new HashSet<DistributedCallable>();

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
                                                          Set<DistributedCallable> callables,
                                                          List<ListenerInvocation> listenerInvocations) {
      for (ListenerInvocation listener : listenerInvocations) {
         if (!enlistedAlready.contains(listener.target)) {
            // If clustered means it is local - so use our address
            if (listener.clustered) {
               callables.add(new ClusterListenerReplicateCallable(listener.generatedId,
                                                                  cache.getCacheManager().getAddress(), listener.filter,
                                                                  listener.converter));
               enlistedAlready.add(listener.target);
            }
            else if (listener.target instanceof RemoteClusterListener) {
               RemoteClusterListener lcl = (RemoteClusterListener)listener.target;
               callables.add(new ClusterListenerReplicateCallable(lcl.getId(), lcl.getOwnerAddress(), listener.filter,
                                                                  listener.converter));
               enlistedAlready.add(listener.target);
            }
         }
      }
   }

   public boolean isNotificationAllowed(
         FlagAffectedCommand cmd, List<ListenerInvocation> listeners) {
      return (cmd == null || !cmd.hasFlag(Flag.SKIP_LISTENER_NOTIFICATION))
            && !listeners.isEmpty();
   }

   @Override
   public void addListener(Object listener, KeyFilter filter, ClassLoader classLoader) {
      validateAndAddListenerInvocation(listener, new KeyFilterAsKeyValueFilter(filter), null, classLoader);
   }

   @Override
   public void addListener(Object listener, KeyFilter filter) {
      addListener(listener, filter, null);
   }

   @Override
   public <K, V, C> void addListener(Object listener, KeyValueFilter<K, V> filter, Converter<K, V, C> converter) {
      validateAndAddListenerInvocation(listener, filter, converter, null);
   }

   @Override
   protected void addedListener(Object listener, UUID generatedId, boolean hasClusteredMethods, KeyValueFilter filter,
                                Converter converter) {
      if (hasClusteredMethods) {
         if (config.clustering().cacheMode().isInvalidation()) {
            throw new UnsupportedOperationException("Cluster listeners cannot be used with Invalidation Caches!");
         }
         clusterListenerIDs.put(listener, generatedId);
         EmbeddedCacheManager manager = cache.getCacheManager();
         Address ourAddress = manager.getAddress();

         List<Address> members = manager.getMembers();
         // If we are the only member don't even worry about sending listeners
         if (members.size() > 1) {
            DistributedExecutionCompletionService decs = new DistributedExecutionCompletionService(distExecutorService);

            log.tracef("Replicating cluster listener to other nodes %s for cluster listener with id %s",
                       members, generatedId);
            Callable callable = new ClusterListenerReplicateCallable(generatedId, ourAddress, filter, converter);
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
                  throw new CacheListenerException(e);
               }
            }

            int extraCount = 0;
            // If anyone else joined since we sent these we have to send the listeners again, since they may have queried
            // before the other nodes got the new listener
            List<Address> membersAfter = manager.getMembers();
            for (Address member : membersAfter) {
               if (!members.contains(member) && !member.equals(ourAddress)) {
                  log.tracef("Found additional node %s that joined during replication of cluster listener with id %s",
                             member, generatedId);
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

   @Override
   protected ListenerInvocation createListenerInvocation(Object listener, Method m, Listener l, KeyValueFilter filter,
                                                         Converter converter, ClassLoader classLoader, UUID generatedId, Subject subject) {
      // If we are dealing with clustered events that forces the cluster listener to only use primary only else we would
      // have duplicate events
      return new ListenerInvocation(listener, m, l.sync(), l.clustered() ? true : l.primaryOnly(), l.clustered(),
                                    filter, converter, classLoader, generatedId, subject);
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
}
