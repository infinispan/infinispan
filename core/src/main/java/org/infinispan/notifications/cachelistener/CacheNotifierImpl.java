/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.AbstractListenerImpl;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.event.*;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.infinispan.notifications.cachelistener.event.Event.Type.*;
import static org.infinispan.util.InfinispanCollections.transformCollectionToMap;

/**
 * Helper class that handles all notifications to registered listeners.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class CacheNotifierImpl extends AbstractListenerImpl implements CacheNotifier {
   private static final Log log = LogFactory.getLog(CacheNotifierImpl.class);

   private static final Map<Class<? extends Annotation>, Class> allowedListeners = new HashMap<Class<? extends Annotation>, Class>();

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
   @SuppressWarnings("unchecked")
   void injectDependencies(Cache cache) {
      this.cache = cache;
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected Map<Class<? extends Annotation>, Class> getAllowedMethodAnnotations() {
      return allowedListeners;
   }

   @Override
   public void notifyCacheEntryCreated(Object key, boolean pre, InvocationContext ctx) {
      if (!cacheEntryCreatedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_CREATED);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         setTx(ctx, e);
         for (ListenerInvocation listener : cacheEntryCreatedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheEntryModified(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryModifiedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_MODIFIED);
         e.setOriginLocal(originLocal);
         e.setValue(value);
         e.setPre(pre);
         e.setKey(key);
         setTx(ctx, e);
         for (ListenerInvocation listener : cacheEntryModifiedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheEntryRemoved(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryRemovedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_REMOVED);
         e.setOriginLocal(originLocal);
         e.setValue(value);
         e.setPre(pre);
         e.setKey(key);
         setTx(ctx, e);
         for (ListenerInvocation listener : cacheEntryRemovedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheEntryVisited(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryVisitedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_VISITED);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         setTx(ctx, e);
         for (ListenerInvocation listener : cacheEntryVisitedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheEntriesEvicted(Collection<InternalCacheEntry> entries, InvocationContext ctx) {
      if (!entries.isEmpty()) {
         if (!cacheEntriesEvictedListeners.isEmpty()) {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
            Map<Object, Object> evictedKeysAndValues = transformCollectionToMap(entries,
                                                                                new InfinispanCollections.MapMakerFunction<Object, Object, InternalCacheEntry>() {
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
         if (!cacheEntryEvictedListeners.isEmpty()) {
            for (InternalCacheEntry ice : entries) {
               EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
               e.setKey(ice.getKey());
               e.setValue(ice.getValue());
               for (ListenerInvocation listener : cacheEntryEvictedListeners) listener.invoke(e);
            }
         }
      }
   }

   @Override
   public void notifyCacheEntryEvicted(Object key, Object value, InvocationContext ctx) {
      if (!cacheEntriesEvictedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
         e.setEntries(Collections.singletonMap(key, value));
         for (ListenerInvocation listener : cacheEntriesEvictedListeners) listener.invoke(e);
      }

      // For backward compat
      if (!cacheEntryEvictedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
         e.setKey(key);
         e.setValue(value);
         for (ListenerInvocation listener : cacheEntryEvictedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheEntryInvalidated(final Object key, Object value, final boolean pre, InvocationContext ctx) {
      if (!cacheEntryInvalidatedListeners.isEmpty()) {
         final boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_INVALIDATED);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         setTx(ctx, e);
         for (ListenerInvocation listener : cacheEntryInvalidatedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheEntryLoaded(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryLoadedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_LOADED);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         setTx(ctx, e);
         for (ListenerInvocation listener : cacheEntryLoadedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheEntryActivated(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryActivatedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_ACTIVATED);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         setTx(ctx, e);
         for (ListenerInvocation listener : cacheEntryActivatedListeners) listener.invoke(e);
      }
   }

   private void setTx(InvocationContext ctx, EventImpl e) {
      if (ctx != null && ctx.isInTxScope()) {
         GlobalTransaction tx = ((TxInvocationContext) ctx).getGlobalTransaction();
         e.setTransactionId(tx);
      }
   }

   @Override
   public void notifyCacheEntryPassivated(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryPassivatedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_PASSIVATED);
         e.setPre(pre);
         e.setKey(key);
         e.setValue(value);
         for (ListenerInvocation listener : cacheEntryPassivatedListeners) listener.invoke(e);
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
   public void notifyDataRehashed(Collection<Address> oldView, Collection<Address> newView, long newViewId, boolean pre) {
      if (!dataRehashedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, DATA_REHASHED);
         e.setPre(pre);
         e.setMembersAtStart(oldView);
         e.setMembersAtEnd(newView);
         e.setNewViewId(newViewId);
         for (ListenerInvocation listener : dataRehashedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyTopologyChanged(ConsistentHash oldConsistentHash, ConsistentHash newConsistentHash, boolean pre) {
      if (!topologyChangedListeners.isEmpty()) {
         EventImpl<Object, Object> e = EventImpl.createEvent(cache, TOPOLOGY_CHANGED);
         e.setPre(pre);
         e.setConsistentHashAtStart(oldConsistentHash);
         e.setConsistentHashAtEnd(newConsistentHash);
         for (ListenerInvocation listener : topologyChangedListeners) listener.invoke(e);
      }
   }
}
