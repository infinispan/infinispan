/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.invocation.InvocationContextContainer;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.notifications.AbstractListenerImpl;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.event.*;
import static org.infinispan.notifications.cachelistener.event.Event.Type.*;

import javax.transaction.Transaction;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Helper class that handles all notifications to registered listeners.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
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
   private Cache cache;

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
   void injectDependencies(InvocationContextContainer icc, Cache cache) {
      this.icc = icc;
      this.cache = cache;
   }

   protected Log getLog() {
      return log;
   }

   protected Map<Class<? extends Annotation>, Class> getAllowedMethodAnnotations() {
      return allowedListeners;
   }

   public void notifyCacheEntryCreated(Object key, boolean pre, InvocationContext ctx) {
      if (!cacheEntryCreatedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_CREATED);
         for (ListenerInvocation listener : cacheEntryCreatedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyCacheEntryModified(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryModifiedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(originLocal);
         e.setValue(value);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_MODIFIED);
         for (ListenerInvocation listener : cacheEntryModifiedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyCacheEntryRemoved(Object key, Object value, boolean pre, InvocationContext ctx) {
      if (!cacheEntryRemovedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(originLocal);
         e.setValue(value);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_REMOVED);
         for (ListenerInvocation listener : cacheEntryRemovedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyCacheEntryVisited(Object key, boolean pre, InvocationContext ctx) {
      if (!cacheEntryVisitedListeners.isEmpty()) {
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_VISITED);
         for (ListenerInvocation listener : cacheEntryVisitedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyCacheEntryEvicted(final Object key, final boolean pre, InvocationContext ctx) {
      if (!cacheEntryEvictedListeners.isEmpty()) {
         final boolean originLocal = ctx.isOriginLocal();
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_EVICTED);
         for (ListenerInvocation listener : cacheEntryEvictedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyCacheEntryInvalidated(final Object key, final boolean pre, InvocationContext ctx) {
      if (!cacheEntryInvalidatedListeners.isEmpty()) {
         final boolean originLocal = ctx.isOriginLocal();
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_INVALIDATED);
         for (ListenerInvocation listener : cacheEntryInvalidatedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyCacheEntryLoaded(Object key, boolean pre, InvocationContext ctx) {
      if (!cacheEntryLoadedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_LOADED);
         for (ListenerInvocation listener : cacheEntryLoadedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyCacheEntryActivated(Object key, boolean pre, InvocationContext ctx) {
      if (!cacheEntryActivatedListeners.isEmpty()) {
         boolean originLocal = ctx.isOriginLocal();
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(originLocal);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_ACTIVATED);
         for (ListenerInvocation listener : cacheEntryActivatedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyCacheEntryPassivated(Object key, boolean pre, InvocationContext ctx) {
      if (!cacheEntryPassivatedListeners.isEmpty()) {
         Transaction tx = ctx.getTransaction();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setPre(pre);
         e.setKey(key);
         e.setTransaction(tx);
         e.setType(CACHE_ENTRY_PASSIVATED);
         for (ListenerInvocation listener : cacheEntryPassivatedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyTransactionCompleted(Transaction transaction, boolean successful, InvocationContext ctx) {
      if (!transactionCompletedListeners.isEmpty()) {
         boolean isOriginLocal = ctx.isOriginLocal();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(isOriginLocal);
         e.setTransaction(transaction);
         e.setTransactionSuccessful(successful);
         e.setType(TRANSACTION_COMPLETED);
         for (ListenerInvocation listener : transactionCompletedListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   public void notifyTransactionRegistered(Transaction transaction, InvocationContext ctx) {
      if (!transactionRegisteredListeners.isEmpty()) {
         boolean isOriginLocal = ctx.isOriginLocal();
         InvocationContext backup = resetInvocationContext(ctx);
         EventImpl e = new EventImpl();
         e.setCache(cache);
         e.setOriginLocal(isOriginLocal);
         e.setTransaction(transaction);
         e.setType(TRANSACTION_REGISTERED);
         for (ListenerInvocation listener : transactionRegisteredListeners) listener.invoke(e);
         restoreInvocationContext(backup);
      }
   }

   private void restoreInvocationContext(InvocationContext backup) {
      icc.set(backup);
   }

   /**
    * Resets the current (passed-in) invocation, and returns a temp InvocationContext containing its state so it can be
    * restored later using {@link #restoreInvocationContext(InvocationContext)}
    *
    * @param ctx the current context to be reset
    * @return a clone of ctx, before it was reset
    */
   private InvocationContext resetInvocationContext(InvocationContext ctx) {
      // wipe current context.
      icc.reset();
      // get a new Invocation Context
      InvocationContext newContext = icc.get();
      newContext.putLookedUpEntries(ctx.getLookedUpEntries());
//      newContext.addAllKeysLocked(ctx.getKeysLocked());
      return ctx;
   }
}
