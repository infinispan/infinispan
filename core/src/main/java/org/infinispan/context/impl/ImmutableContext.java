package org.infinispan.context.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;

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
   public void removeLookedUpEntry(Object key) {
      throw newUnsupportedMethod();
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
