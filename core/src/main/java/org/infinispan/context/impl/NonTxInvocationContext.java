package org.infinispan.context.impl;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;

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

   public NonTxInvocationContext(int numEntries, Address origin, Equivalence<Object> keyEq) {
      super(origin);
      lookedUpEntries = CollectionFactory.makeMap(numEntries, keyEq, AnyEquivalence.<CacheEntry>getInstance());
      this.keyEq = keyEq;
   }

   public NonTxInvocationContext(Address origin, Equivalence<Object> keyEq) {
      super(origin);
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
   @SuppressWarnings("unchecked")
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return (Map<Object, CacheEntry>)
            (lookedUpEntries == null ?
                   InfinispanCollections.emptyMap() : lookedUpEntries);
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
