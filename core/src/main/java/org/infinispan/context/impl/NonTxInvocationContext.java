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
import org.infinispan.util.AnyEquivalence;
import org.infinispan.util.Equivalence;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.CollectionFactory;

import java.util.Map;
import java.util.Set;

/**
 * Context to be used for non transactional calls, both remote and local.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class NonTxInvocationContext extends AbstractInvocationContext {

   private static final int INITIAL_CAPACITY = 4;

   protected final Map<Object, CacheEntry> lookedUpEntries;

   protected Set<Object> lockedKeys;

   private final Equivalence<Object> keyEq;

   public NonTxInvocationContext(int numEntries, boolean local, Equivalence<Object> keyEq) {
      lookedUpEntries = CollectionFactory.makeMap(numEntries, keyEq, AnyEquivalence.<CacheEntry>getInstance());
      setOriginLocal(local);
      this.keyEq = keyEq;
   }

   public NonTxInvocationContext(Equivalence<Object> keyEq) {
      lookedUpEntries = CollectionFactory.makeMap(INITIAL_CAPACITY, keyEq, AnyEquivalence.<CacheEntry>getInstance());
      this.keyEq = keyEq;
   }

   @Override
   public CacheEntry lookupEntry(Object k) {
      return lookedUpEntries.get(k);
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      lookedUpEntries.remove(key);
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> newLookedUpEntries) {
      lookedUpEntries.putAll(newLookedUpEntries);
   }

   @Override
   public void clearLookedUpEntries() {
      lookedUpEntries.clear();
   }

   @Override
   @SuppressWarnings("unchecked")
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return (Map<Object, CacheEntry>)
            (lookedUpEntries == null ?
                   InfinispanCollections.emptyMap() : lookedUpEntries);
   }

   @Override
   public boolean isOriginLocal() {
      return isContextFlagSet(ContextFlag.ORIGIN_LOCAL);
   }

   public void setOriginLocal(boolean originLocal) {
      setContextFlag(ContextFlag.ORIGIN_LOCAL, originLocal);
   }

   @Override
   public boolean isInTxScope() {
      return false;
   }

   @Override
   public Object getLockOwner() {
      return Thread.currentThread();
   }

   @Override
   public NonTxInvocationContext clone() {
      NonTxInvocationContext dolly = (NonTxInvocationContext) super.clone();
      dolly.lookedUpEntries.putAll(lookedUpEntries);
      return dolly;
   }

   @Override
   public void addLockedKey(Object key) {
      if (lockedKeys == null)
         lockedKeys = CollectionFactory.makeSet(INITIAL_CAPACITY, keyEq);

      lockedKeys.add(key);
   }

   @Override
   public Set<Object> getLockedKeys() {
      return lockedKeys == null ? InfinispanCollections.emptySet() : lockedKeys;
   }

   @Override
   public void clearLockedKeys() {
      lockedKeys = null;
   }
}
