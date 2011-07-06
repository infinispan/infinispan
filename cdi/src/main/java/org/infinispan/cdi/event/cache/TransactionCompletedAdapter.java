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
package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class TransactionCompletedAdapter<K, V> extends AbstractAdapter<TransactionCompletedEvent<K, V>> {

   public static final TransactionCompletedEvent<?, ?> EMPTY = new TransactionCompletedEvent<Object, Object>() {

      public Type getType() {
         return null;
      }

      public GlobalTransaction getGlobalTransaction() {
         return null;
      }

      public boolean isOriginLocal() {
         return false;
      }

      public boolean isPre() {
         return false;
      }

      public Cache<Object, Object> getCache() {
         return null;
      }

      public boolean isTransactionSuccessful() {
         return false;
      }

   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<TransactionCompletedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<TransactionCompletedEvent<?, ?>>() {
   };

   public TransactionCompletedAdapter(Event<TransactionCompletedEvent<K, V>> event) {
      super(event);
   }

   @TransactionCompleted
   public void fire(TransactionCompletedEvent<K, V> payload) {
      super.fire(payload);
   }
}
