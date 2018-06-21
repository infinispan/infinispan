package org.infinispan.context.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;

/**
 * Context to be used for non transactional calls, both remote and local.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class NonTxInvocationContext extends AbstractInvocationContext {

   private static final int INITIAL_CAPACITY = 4;

   private final Map<Object, CacheEntry> lookedUpEntries;
   private Set<Object> lockedKeys;
   private Object lockOwner;


   public NonTxInvocationContext(int numEntries, Address origin) {
      super(origin);
      lookedUpEntries = CollectionFactory.makeMap(CollectionFactory.computeCapacity(numEntries));
   }

   public NonTxInvocationContext(Address origin) {
      super(origin);
      lookedUpEntries = CollectionFactory.makeMap(INITIAL_CAPACITY);
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
   @SuppressWarnings("unchecked")
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return (Map<Object, CacheEntry>)
            (lookedUpEntries == null ?
                   Collections.emptyMap() : lookedUpEntries);
   }

   @Override
   public void forEachEntry(BiConsumer<Object, CacheEntry> action) {
      if (lookedUpEntries != null) {
         lookedUpEntries.forEach(action);
      }
   }

   @Override
   public int lookedUpEntriesCount() {
      return lookedUpEntries != null ? lookedUpEntries.size() : 0;
   }

   @Override
   public boolean isInTxScope() {
      return false;
   }

   @Override
   public Object getLockOwner() {
      return lockOwner;
   }

   @Override
   public void setLockOwner(Object lockOwner) {
      this.lockOwner = lockOwner;
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
         lockedKeys = CollectionFactory.makeSet(INITIAL_CAPACITY);

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
