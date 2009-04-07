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

import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listenable;

import javax.transaction.Transaction;

/**
 * Public interface with all allowed notifications.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@NonVolatile
@Scope(Scopes.NAMED_CACHE)
public interface CacheNotifier extends Listenable {
   /**
    * Notifies all registered listeners of a CacheEntryCreated event.
    */
   void notifyCacheEntryCreated(Object key, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a CacheEntryModified event.
    */
   void notifyCacheEntryModified(Object key, Object value, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a CacheEntryRemoved event.
    */
   void notifyCacheEntryRemoved(Object key, Object value, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a CacheEntryVisited event.
    */
   void notifyCacheEntryVisited(Object key, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a CacheEntryEvicted event.
    */
   void notifyCacheEntryEvicted(Object key, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a CacheEntryInvalidated event.
    */
   void notifyCacheEntryInvalidated(Object key, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a CacheEntryLoaded event.
    */
   void notifyCacheEntryLoaded(Object key, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a CacheEntryActivated event.
    */
   void notifyCacheEntryActivated(Object key, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a CacheEntryPassivated event.
    */
   void notifyCacheEntryPassivated(Object key, boolean pre, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a transaction completion event.
    *
    * @param transaction the transaction that has just completed
    * @param successful  if true, the transaction committed.  If false, this is a rollback event
    */
   void notifyTransactionCompleted(Transaction transaction, boolean successful, InvocationContext ctx);

   /**
    * Notifies all registered listeners of a transaction registration event.
    *
    * @param transaction the transaction that has just completed
    */
   void notifyTransactionRegistered(Transaction transaction, InvocationContext ctx);
}