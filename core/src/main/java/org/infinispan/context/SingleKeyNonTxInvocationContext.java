package org.infinispan.context;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.interceptors.impl.BaseAsyncInvocationContext;
import org.infinispan.remoting.transport.Address;

/**
 * @author Mircea Markus
 * @author Sanne Grinovero
 * @deprecated Since 9.0, this class is going to be moved to an internal package.
 */
@Deprecated
public final class SingleKeyNonTxInvocationContext extends BaseAsyncInvocationContext implements InvocationContext {

   /**
    * It is possible for the key to only be wrapped but not locked, e.g. when a get takes place.
    */
   private boolean isLocked;

   private Object key;

   private CacheEntry cacheEntry;

   //TODO move reference to Equivalence to InvocationContextFactory (Memory allocation cost)
   private final Equivalence keyEquivalence;
   //TODO move the Origin's address to the InvocationContextFactory when isOriginLocal=true -> all addresses are the same  (Memory allocation cost)
   //(verify if this is worth it by looking at object alignment - would need a different implementation as pointing to null wouldn't help)
   private final Address origin;

   private Object lockOwner;

   public SingleKeyNonTxInvocationContext(final Address origin, final Equivalence<Object> keyEquivalence) {
      this.origin = origin;
      this.keyEquivalence = keyEquivalence;
   }

   @Override
   public boolean isOriginLocal() {
      return origin == null;
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
   public Set<Object> getLockedKeys() {
      return isLocked ? Collections.singleton(key) : Collections.emptySet();
   }

   @Override
   public void clearLockedKeys() {
      isLocked = false;
      // TODO Dan: this shouldn't be necessary, but we don't clear the looked up keys
      // when retrying a non-tx command because of a topology change
      cacheEntry = null;
   }

   @Override
   public void addLockedKey(final Object key) {
      if (this.key == null) {
         // Set the key here
         this.key = key;
      } else if (!keyEquivalence.equals(key, this.key)) {
         throw illegalStateException();
      }

      isLocked = true;
   }

   private IllegalStateException illegalStateException() {
      return new IllegalStateException("This is a single key invocation context, using multiple keys shouldn't be possible");
   }

   @Override
   public CacheEntry lookupEntry(final Object key) {
      if (key != null && this.key != null && keyEquivalence.equals(key, this.key))
         return cacheEntry;

      return null;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return cacheEntry == null ? Collections.emptyMap() : Collections.singletonMap(key, cacheEntry);
   }

   @Override
   public void putLookedUpEntry(final Object key, final CacheEntry e) {
      if (this.key == null) {
         // Set the key here
         this.key = key;
      } else if (!keyEquivalence.equals(key, this.key)) {
         throw illegalStateException();
      }

      this.cacheEntry = e;
   }

   @Override
   public void removeLookedUpEntry(final Object key) {
      if (keyEquivalence.equals(key, this.key)) {
         this.cacheEntry = null;
      }
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
      return null;
   }

   @Override
   public void setClassLoader(ClassLoader classLoader) {
      // No-op
   }

   @Override
   public boolean hasLockedKey(final Object key) {
      return isLocked && keyEquivalence.equals(this.key, key);
   }

   @Override
   public boolean isEntryRemovedInContext(final Object key) {
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   public void resetState() {
      this.key = null;
      this.cacheEntry = null;
      this.isLocked = false;
   }

}
