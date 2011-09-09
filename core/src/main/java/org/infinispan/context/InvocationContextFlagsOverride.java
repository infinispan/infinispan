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

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.BidirectionalMap;


/**
 * Wraps an existing {@link InvocationContext} without changing the context directly
 * but making sure the specified flags are considered enabled.
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 5.0
 */
public class InvocationContextFlagsOverride implements InvocationContext {
   
   private final InvocationContext delegate;
   private final Set<Flag> flags;
   
   /**
    * Wraps an existing {@link InvocationContext} without changing the context directly
    * but making sure the specified flags are considered enabled.
    * @param delegate
    * @param flags
    */
   public InvocationContextFlagsOverride(InvocationContext delegate, Set<Flag> flags) {
      if (delegate == null || flags == null) {
         throw new IllegalArgumentException("parameters shall not be null");
      }
      this.delegate = delegate;
      this.flags = Flag.copyWithouthRemotableFlags(flags);
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      return delegate.lookupEntry(key);
   }

   @Override
   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return delegate.getLookedUpEntries();
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      delegate.putLookedUpEntry(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      delegate.putLookedUpEntries(lookedUpEntries);
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      delegate.removeLookedUpEntry(key);
   }

   @Override
   public void clearLookedUpEntries() {
      delegate.clearLookedUpEntries();
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return delegate.hasLockedKey(key);
   }

   @Override
   public boolean hasFlag(Flag o) {
      if (flags.contains(o)) {
         return true;
      }
      return delegate.hasFlag(o);
   }

   @Override
   public Set<Flag> getFlags() {
      Set<Flag> flagsInDelegate = delegate.getFlags();
      if (flagsInDelegate == null || flagsInDelegate.isEmpty()) {
         return flags;
      }
      else {
         Set<Flag> merged = EnumSet.copyOf(flagsInDelegate);
         merged.addAll(flags);
         return merged;
      }
   }

   @Override
   public void setFlags(Flag... newFlags) {
      throw new IllegalStateException("Flags can't be changed after creating an InvocationContextFlagsOverride wrapper");
   }

   @Override
   public void setFlags(Collection<Flag> newFlags) {
      throw new IllegalStateException("Flags can't be changed after creating an InvocationContextFlagsOverride wrapper");
   }

   @Override
   public void reset() {
      delegate.reset();
   }

   @Override
   public boolean isOriginLocal() {
      return delegate.isOriginLocal();
   }

   @Override
   public boolean isInTxScope() {
      return delegate.isInTxScope();
   }

   @Override
   public Object getLockOwner() {
      return delegate.getLockOwner();
   }

   @Override
   public boolean isUseFutureReturnType() {
      return delegate.isUseFutureReturnType();
   }

   @Override
   public void setUseFutureReturnType(boolean useFutureReturnType) {
      delegate.setUseFutureReturnType(useFutureReturnType);
   }

   @Override
   public Set<Object> getLockedKeys() {
      return delegate.getLockedKeys();
   }
   
   @Override
   public Address getOrigin() {
      return delegate.getOrigin();
   }
   
   @Override
   public InvocationContextFlagsOverride clone() {
      return new InvocationContextFlagsOverride(delegate, flags);
   }

   @Override
   public ClassLoader getClassLoader() {
      return delegate.getClassLoader();
   }

   @Override
   public void setClassLoader(ClassLoader classLoader) {
      delegate.setClassLoader(classLoader);
   }

   @Override
   public void registerLockedKey(Object key) {
      delegate.registerLockedKey(key);
   }
}
