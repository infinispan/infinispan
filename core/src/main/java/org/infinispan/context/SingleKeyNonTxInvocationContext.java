package org.infinispan.context;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.AbstractInvocationContext;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Mircea Markus
 * @since 5.1
 */
public class SingleKeyNonTxInvocationContext extends AbstractInvocationContext {

   private final boolean isOriginLocal;

   private Object key;

   /**
    * It is possible for the key to only be wrapped but not locked, e.g. when a get takes place.
    */
   private boolean isLocked;

   private CacheEntry cacheEntry;

   private final Equivalence keyEquivalence;

   public SingleKeyNonTxInvocationContext(
         boolean originLocal, Equivalence keyEquivalence) {
      isOriginLocal = originLocal;
      this.keyEquivalence = keyEquivalence;
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
   }

   @Override
   public void addLockedKey(Object key) {
      if (cacheEntry != null && !keyEquivalence.equals(key, this.key))
         throw illegalStateException();
      isLocked = true;
   }

   private IllegalStateException illegalStateException() {
      return new IllegalStateException("This is a single key invocation context, using multiple keys shouldn't be possible");
   }

   @Override
   @SuppressWarnings("unchecked")
   public CacheEntry lookupEntry(Object key) {
      if (key != null && this.key !=null && keyEquivalence.equals(key, this.key))
         return cacheEntry;

      return null;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return key == null ? InfinispanCollections.<Object, CacheEntry>emptyMap() : Collections.singletonMap(key, cacheEntry);
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      this.key = key;
      this.cacheEntry = e;
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      if (lookedUpEntries.size() > 1) throw illegalStateException();
      Map.Entry<Object, CacheEntry> e = lookedUpEntries.entrySet().iterator().next();
      this.key = e.getKey();
      this.cacheEntry = e.getValue();
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      if (keyEquivalence.equals(key, this.key))
         clearLockedKeys();
   }

   @Override
   public void clearLookedUpEntries() {
      clearLockedKeys();
   }

   public Object getKey() {
      return key;
   }

   public CacheEntry getCacheEntry() {
      return cacheEntry;
   }
}
