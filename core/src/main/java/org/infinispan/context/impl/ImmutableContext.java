/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.context.impl;

import org.infinispan.CacheException;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.InfinispanCollections;

import java.util.Map;
import java.util.Set;

/**
 * This context is a non-context for operations such as eviction which are not related
 * to the method invocation which caused them.
 * 
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
public final class ImmutableContext implements InvocationContext {
   
   public static final ImmutableContext INSTANCE = new ImmutableContext();
   
   private ImmutableContext() {
      //don't create multiple instances
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      throw newUnsupportedMethod();
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return InfinispanCollections.emptyMap();
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      throw newUnsupportedMethod();
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      throw newUnsupportedMethod();
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      throw newUnsupportedMethod();
   }

   @Override
   public void clearLookedUpEntries() {
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return false;
   }

   @Override
   public boolean isOriginLocal() {
      return true;
   }

   @Override
   public Address getOrigin() {
      return null;
   }

   @Override
   public boolean isInTxScope() {
      return false;
   }

   @Override
   public Object getLockOwner() {
      return null;
   }

   @Override
   public boolean isUseFutureReturnType() {
      return false;
   }

   @Override
   public void setUseFutureReturnType(boolean useFutureReturnType) {
      throw newUnsupportedMethod();
   }

   @Override
   public Set<Object> getLockedKeys() {
      return InfinispanCollections.emptySet();
   }

   @Override
   public InvocationContext clone() {
      return this;
   }

   @Override
   public ClassLoader getClassLoader() {
      return null;
   }

   @Override
   public void setClassLoader(ClassLoader classLoader) {
      throw newUnsupportedMethod();
   }

   /**
    * @return an exception to state this context is read only
    */
   private static CacheException newUnsupportedMethod() {
      throw newUnsupportedMethod();
   }

   @Override
   public void addLockedKey(Object key) {
      throw newUnsupportedMethod();
   }

   @Override
   public void clearLockedKeys() {
      throw newUnsupportedMethod();
   }

   @Override
   public boolean replaceValue(Object key, InternalCacheEntry cacheEntry) {
      throw newUnsupportedMethod();
   }

   @Override
   public boolean isEntryRemovedInContext(Object key) {
      return false;
   }
}
