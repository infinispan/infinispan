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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

   public NonTxInvocationContext(int numEntries, boolean local) {
      lookedUpEntries = new HashMap<Object, CacheEntry>(numEntries);
      setOriginLocal(local);
   }

   public NonTxInvocationContext() {
      lookedUpEntries = new HashMap<Object, CacheEntry>(INITIAL_CAPACITY);
   }

   public CacheEntry lookupEntry(Object k) {
      return lookedUpEntries.get(k);
   }

   public void removeLookedUpEntry(Object key) {
      lookedUpEntries.remove(key);
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      lookedUpEntries.put(key, e);
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> newLookedUpEntries) {
      lookedUpEntries.putAll(newLookedUpEntries);
   }

   public void clearLookedUpEntries() {
      lookedUpEntries.clear();
   }

   @SuppressWarnings("unchecked")
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return (Map<Object, CacheEntry>)
            (lookedUpEntries == null ? Collections.emptyMap() : lookedUpEntries);
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

   @Override
   public void reset() {
      super.reset();
      clearLookedUpEntries();
      if (lockedKeys != null) lockedKeys.clear();
   }

   @Override
   public NonTxInvocationContext clone() {
      NonTxInvocationContext dolly = (NonTxInvocationContext) super.clone();
      dolly.lookedUpEntries.putAll(lookedUpEntries);
      return dolly;
   }

   @Override
   public void addLockedKey(Object key) {
      if (lockedKeys == null) lockedKeys = new HashSet<Object>(INITIAL_CAPACITY);
      lockedKeys.add(key);
   }

   @Override
   public Set<Object> getLockedKeys() {
      return lockedKeys == null ? Collections.emptySet() : lockedKeys;
   }

   @Override
   public void clearLockedKeys() {
      lockedKeys = null;
   }
}
