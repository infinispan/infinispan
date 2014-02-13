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

package org.infinispan.context;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.InfinispanCollections;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Mircea Markus
 * @author Sanne Grinovero
 * @since 5.1
 */
public final class SingleKeyNonTxInvocationContext implements InvocationContext {

   /**
    * It is possible for the key to only be wrapped but not locked, e.g. when a get takes place.
    */
   private boolean isLocked;
   private final boolean isOriginLocal;

   private Object key;

   private CacheEntry cacheEntry;

   //TODO move reference to ClassLoader to InvocationContextFactory (Memory allocation cost)
   private ClassLoader classLoader;
   //TODO move the Origin's address to the InvocationContextFactory when isOriginLocal=true -> all addresses are the same  (Memory allocation cost)
   //(verify if this is worth it by looking at object alignment - would need a different implementation as pointing to null wouldn't help)
   private Address origin;

   public SingleKeyNonTxInvocationContext(final boolean originLocal) {
      this.isOriginLocal = originLocal;
   }

   @Override
   public boolean isOriginLocal() {
      return isOriginLocal;
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
   public Set<Object> getLockedKeys() {
      return isLocked && key != null ?
            Collections.singleton(key) : InfinispanCollections.emptySet();
   }

   @Override
   public void clearLockedKeys() {
      key = null;
      cacheEntry = null;
      isLocked = false;
   }

   @Override
   public void addLockedKey(Object key) {
      if (cacheEntry != null && !key.equals(this.key))
         throw illegalStateException();
      isLocked = true;
   }

   private IllegalStateException illegalStateException() {
      return new IllegalStateException("This is a single key invocation context, using multiple keys shouldn't be possible");
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      if (key != null && this.key !=null && key.equals(this.key)) return cacheEntry;
      return null;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return key == null ? InfinispanCollections.<Object, CacheEntry>emptyMap() : Collections.singletonMap(key, cacheEntry);
   }

   @Override
   public void putLookedUpEntry(final Object key, final CacheEntry e) {
      this.key = key;
      this.cacheEntry = e;
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      if (key.equals(this.key))
         clearLockedKeys();
   }

   public Object getKey() {
      return key;
   }

   public CacheEntry getCacheEntry() {
      return cacheEntry;
   }

   @Override
   public Address getOrigin() {
      return origin;
   }

   @Override
   public ClassLoader getClassLoader() {
      return classLoader;
   }

   @Override
   public void setClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   public boolean hasLockedKey(final Object key) {
      return isLocked && key.equals(this.key);
   }

   @Override
   public boolean replaceValue(Object key, Object value) {
      CacheEntry ce = lookupEntry(key);
      if (ce == null || ce.isNull() || ce.isLockPlaceholder() || ce.getValue() == null) {
         if (ce != null && ce.isChanged()) {
            ce.setValue(value);
         } else {
            return false;
         }
      }
      return true;
   }

   @Override
   public SingleKeyNonTxInvocationContext clone() {
      try {
         return (SingleKeyNonTxInvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
   }

}
