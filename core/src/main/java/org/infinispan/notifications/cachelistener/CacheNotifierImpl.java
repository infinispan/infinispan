package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.AbstractListenerImpl;
import org.infinispan.notifications.KeyFilter;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.event.*;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.InvalidTransactionException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.infinispan.commons.util.InfinispanCollections.transformCollectionToMap;
import static org.infinispan.notifications.cachelistener.event.Event.Type.*;

/**
 * Helper class that handles all notifications to registered listeners.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public final class CacheNotifierImpl extends AbstractListenerImpl implements CacheNotifier {

   private static final Log log = LogFactory.getLog(CacheNotifierImpl.class);

   private static final Map<Class<? extends Annotation>, Class<?>> allowedListeners = new HashMap<Class<? extends Annotation>, Class<?>>(16);

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
                           TransactionManager transactionManager) {
      this.cache = cache;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.transactionManager = transactionManager;
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
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_CREATED);
         e.setOriginLocal(originLocal);
         // Added capability to set cache entry created value in order
         // to avoid breaking behaviour of CacheEntryModifiedEvent.getValue()
         // when isPre=false.
         e.setValue(value);
         e.setPre(pre);
         e.setKey(key);
         setTx(ctx, e);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryCreatedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyCacheEntryModified(Object key, Object value,
         boolean created, boolean pre, InvocationContext ctx,
         FlagAffectedCommand command) {
      if (!cacheEntryModifiedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_MODIFIED);
         e.setOriginLocal(originLocal);
         e.setValue(value);
         e.setPre(pre);
         e.setKey(key);
         // Even if CacheEntryCreatedEvent.getValue() has been added, to
         // avoid breaking old behaviour and make it easy to comply with
         // JSR-107 specification TCK, it's necessary to find out whether a
         // modification is the result of a cache entry being created or not.
         // This is needed because on JSR-107, a modification is only fired
         // when the entry is updated, and only one event is fired, so you
         // want to fire it when isPre=false.
         e.setCreated(created);
         setTx(ctx, e);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryModifiedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
   }

   @Override
   public void notifyCacheEntryRemoved(Object key, Object value, Object oldValue,
         boolean pre, InvocationContext ctx, FlagAffectedCommand command) {
      if (isNotificationAllowed(command, cacheEntryRemovedListeners)) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_REMOVED);
         e.setOriginLocal(originLocal);
         e.setValue(value);
         e.setOldValue(oldValue);
         e.setPre(pre);
         e.setKey(key);
         setTx(ctx, e);
         boolean isLocalNodePrimaryOwner = clusteringDependentLogic.localNodeIsPrimaryOwner(key);
         for (ListenerInvocation listener : cacheEntryRemovedListeners) listener.invoke(e, isLocalNodePrimaryOwner);
      }
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
   public void notifyTransactionRegistered(GlobalTransaction globalTransaction, InvocationContext ctx) {
      if (!transactionRegisteredListeners.isEmpty()) {
         boolean isOriginLocal = ctx.isOriginLocal();
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

   public boolean isNotificationAllowed(
         FlagAffectedCommand cmd, List<ListenerInvocation> listeners) {
      return (cmd == null || !cmd.hasFlag(Flag.SKIP_LISTENER_NOTIFICATION))
            && !listeners.isEmpty();
   }

   @Override
   public void addListener(Object listener, KeyFilter filter, ClassLoader classLoader) {
      validateAndAddListenerInvocation(listener, filter, classLoader);
   }

   @Override
   public void addListener(Object listener, KeyFilter filter) {
      validateAndAddListenerInvocation(listener, filter, null);
   }
}
