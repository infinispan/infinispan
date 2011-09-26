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
package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.InfinispanCollections;

import java.util.Map;

/**
 * Context to be used for non transactional calls, both remote and local.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class NonTxInvocationContext extends AbstractInvocationContext {

   protected BidirectionalLinkedHashMap<Object, CacheEntry> lookedUpEntries = null;

   public CacheEntry lookupEntry(Object k) {
      return lookedUpEntries == null ? null : lookedUpEntries.get(k);
   }

   public void removeLookedUpEntry(Object key) {
      if (lookedUpEntries != null) lookedUpEntries.remove(key);
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      initLookedUpEntries();
      lookedUpEntries.put(key, e);
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> newLookedUpEntries) {
      initLookedUpEntries();
      for (Map.Entry<Object, CacheEntry> ce: newLookedUpEntries.entrySet()) {
         lookedUpEntries.put(ce.getKey(), ce.getValue());
      }
   }

   public void clearLookedUpEntries() {
      lookedUpEntries = null;
   }

   @SuppressWarnings("unchecked")
   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return (BidirectionalMap<Object, CacheEntry>)
            (lookedUpEntries == null ? InfinispanCollections.emptyBidirectionalMap() : lookedUpEntries);
   }

   public boolean isOriginLocal() {
      return isContextFlagSet(ContextFlag.ORIGIN_LOCAL);
   }

   public void setOriginLocal(boolean originLocal) {
      setContextFlag(ContextFlag.ORIGIN_LOCAL, originLocal);
   }

   public boolean isInTxScope() {
      return false;
   }

   public Object getLockOwner() {
      return Thread.currentThread();
   }

   private void initLookedUpEntries() {
      if (lookedUpEntries == null) lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(4);
   }

   @Override
   public void reset() {
      super.reset();
      clearLookedUpEntries();
   }

   @Override
   public NonTxInvocationContext clone() {
      NonTxInvocationContext dolly = (NonTxInvocationContext) super.clone();
      if (lookedUpEntries != null) {
         dolly.lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(lookedUpEntries);
      }
      return dolly;
   }
}
