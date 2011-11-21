package org.infinispan.context;

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

   public SingleKeyNonTxInvocationContext(boolean originLocal) {
      isOriginLocal = originLocal;
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
      return isLocked && key != null ? Collections.singleton(key) : Collections.emptySet();
   }

   @Override
   public void clearLockedKeys() {
      key = null;
      cacheEntry = null;
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
      if (key != null && key.equals(this.key)) return cacheEntry;
      return null;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return key == null ? Collections.<Object, CacheEntry>emptyMap() : Collections.singletonMap(key, cacheEntry);
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      this.key = key;
      this.cacheEntry = e;
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      if (lookedUpEntries.size() > 1) throw illegalStateException();
      this.key = lookedUpEntries.entrySet().iterator().next();
      this.cacheEntry = lookedUpEntries.get(this.key);
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      if (key.equals(this.key))
         clearLockedKeys();
   }

   @Override
   public void clearLookedUpEntries() {
      clearLockedKeys();
   }
}
