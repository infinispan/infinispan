package org.infinispan.context.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.commons.CacheException;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * This context is a non-context for operations such as eviction which are not related
 * to the method invocation which caused them.
 *
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
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
      return Collections.emptyMap();
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
   public void setLockOwner(Object lockOwner) {
      throw newUnsupportedMethod();
   }

   @Override
   public Set<Object> getLockedKeys() {
      return Collections.emptySet();
   }

   @Override
   public InvocationContext clone() {
      return this;
   }

   /**
    * @return an exception to state this context is read only
    */
   private static CacheException newUnsupportedMethod() {
      throw new UnsupportedOperationException();
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
   public boolean isEntryRemovedInContext(Object key) {
      return false;
   }

   @Override
   public void forEachEntry(BiConsumer<Object, CacheEntry> action) { }

   @Override
   public void forEachValue(BiConsumer<Object, CacheEntry> action) { }

   @Override
   public <K, V> Publisher<CacheEntry<K, V>> publisher() {
      return Flowable.empty();
   }
}
