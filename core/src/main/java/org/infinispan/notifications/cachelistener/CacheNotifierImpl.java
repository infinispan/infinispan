/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import static org.infinispan.notifications.cachelistener.event.Event.Type.*;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.Cache;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.AbstractListenerImpl;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.notifications.cachelistener.event.EventImpl;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Helper class that handles all notifications to registered listeners.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
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
      allowedListeners.put(CacheEntryEvicted.class, CacheEntryEvictedEvent.class);
      allowedListeners.put(TransactionRegistered.class, TransactionRegisteredEvent.class);
      allowedListeners.put(TransactionCompleted.class, TransactionCompletedEvent.class);
      allowedListeners.put(CacheEntryInvalidated.class, CacheEntryInvalidatedEvent.class);

   }

   final List<ListenerInvocation> cacheEntryCreatedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryRemovedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryVisitedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryModifiedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryActivatedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryPassivatedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryLoadedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryInvalidatedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheEntryEvictedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> transactionRegisteredListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> transactionCompletedListeners = new CopyOnWriteArrayList<ListenerInvocation>();

   private InvocationContextContainer icc;
   private Cache<Object, Object> cache;

   public CacheNotifierImpl() {

      listenersMap.put(CacheEntryCreated.class, cacheEntryCreatedListeners);
      listenersMap.put(CacheEntryRemoved.class, cacheEntryRemovedListeners);
      listenersMap.put(CacheEntryVisited.class, cacheEntryVisitedListeners);
      listenersMap.put(CacheEntryModified.class, cacheEntryModifiedListeners);
      listenersMap.put(CacheEntryActivated.class, cacheEntryActivatedListeners);
      listenersMap.put(CacheEntryPassivated.class, cacheEntryPassivatedListeners);
      listenersMap.put(CacheEntryLoaded.class, cacheEntryLoadedListeners);
      listenersMap.put(CacheEntryEvicted.class, cacheEntryEvictedListeners);
      listenersMap.put(TransactionRegistered.class, transactionRegisteredListeners);
      listenersMap.put(TransactionCompleted.class, transactionCompletedListeners);
      listenersMap.put(CacheEntryInvalidated.class, cacheEntryInvalidatedListeners);
   }

   @Inject
   @SuppressWarnings("unchecked")
   void injectDependencies(InvocationContextContainer icc, Cache cache) {
      this.icc = icc;
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
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_CREATED);
            e.setOriginLocal(originLocal);
            e.setPre(pre);
            e.setKey(key);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryCreatedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyCacheEntryModified(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryModifiedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_MODIFIED);
            e.setOriginLocal(originLocal);
            e.setValue(value);
            e.setPre(pre);
            e.setKey(key);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryModifiedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyCacheEntryRemoved(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryRemovedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_REMOVED);
            e.setOriginLocal(originLocal);
            e.setValue(value);
            e.setPre(pre);
            e.setKey(key);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryRemovedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyCacheEntryVisited(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryVisitedListeners.isEmpty()) {
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_VISITED);
            e.setPre(pre);
            e.setKey(key);
            e.setValue(value);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryVisitedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyCacheEntryEvicted(final Object key, Object value, final boolean pre, InvocationContext ctx) {
      if (!cacheEntryEvictedListeners.isEmpty()) {
         final boolean originLocal = ctx.isOriginLocal();
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_EVICTED);
            e.setOriginLocal(originLocal);
            e.setPre(pre);
            e.setKey(key);
            e.setValue(value);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryEvictedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyCacheEntryInvalidated(final Object key, Object value, final boolean pre, InvocationContext ctx) {
      if (!cacheEntryInvalidatedListeners.isEmpty()) {
         final boolean originLocal = ctx.isOriginLocal();
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_INVALIDATED);
            e.setOriginLocal(originLocal);
            e.setPre(pre);
            e.setKey(key);
            e.setValue(value);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryInvalidatedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyCacheEntryLoaded(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryLoadedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_LOADED);
            e.setOriginLocal(originLocal);
            e.setPre(pre);
            e.setKey(key);
            e.setValue(value);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryLoadedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyCacheEntryActivated(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryActivatedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_ACTIVATED);
            e.setOriginLocal(originLocal);
            e.setPre(pre);
            e.setKey(key);
            e.setValue(value);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryActivatedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
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
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, CACHE_ENTRY_PASSIVATED);
            e.setPre(pre);
            e.setKey(key);
            e.setValue(value);
            setTx(ctx, e);
            for (ListenerInvocation listener : cacheEntryPassivatedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyTransactionCompleted(GlobalTransaction transaction, boolean successful, InvocationContext ctx) {
      if (!transactionCompletedListeners.isEmpty()) {
         boolean isOriginLocal = ctx.isOriginLocal();
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, TRANSACTION_COMPLETED);
            e.setOriginLocal(isOriginLocal);
            e.setTransactionId(transaction);
            e.setTransactionSuccessful(successful);
            for (ListenerInvocation listener : transactionCompletedListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }

   @Override
   public void notifyTransactionRegistered(GlobalTransaction globalTransaction, InvocationContext ctx) {
      if (!transactionRegisteredListeners.isEmpty()) {
         boolean isOriginLocal = ctx.isOriginLocal();
         InvocationContext contexts = icc.suspend();
         try {
            EventImpl<Object, Object> e = EventImpl.createEvent(cache, TRANSACTION_REGISTERED);
            e.setOriginLocal(isOriginLocal);
            e.setTransactionId(globalTransaction);
            for (ListenerInvocation listener : transactionRegisteredListeners) listener.invoke(e);
         } finally {
            icc.resume(contexts);
         }
      }
   }
}
