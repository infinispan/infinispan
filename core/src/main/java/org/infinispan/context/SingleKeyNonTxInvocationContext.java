package org.infinispan.context;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.remoting.transport.Address;

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

   //TODO move reference to Equivalence to InvocationContextFactory (Memory allocation cost)
   private final Equivalence keyEquivalence;
   //TODO move reference to ClassLoader to InvocationContextFactory (Memory allocation cost)
   private ClassLoader classLoader;
   //TODO move the Origin's address to the InvocationContextFactory when isOriginLocal=true -> all addresses are the same  (Memory allocation cost)
   //(verify if this is worth it by looking at object alignment - would need a different implementation as pointing to null wouldn't help)
   private Address origin;

   public SingleKeyNonTxInvocationContext(final boolean originLocal, final Equivalence keyEquivalence) {
      this.isOriginLocal = originLocal;
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
      return isLocked ? Collections.singleton(key) : InfinispanCollections.emptySet();
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
      return cacheEntry == null ? InfinispanCollections.<Object, CacheEntry>emptyMap() : Collections.singletonMap(key, cacheEntry);
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
      return classLoader;
   }

   @Override
   public void setClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   public boolean hasLockedKey(final Object key) {
      return isLocked && keyEquivalence.equals(this.key, key);
   }

   @Override
   public boolean replaceValue(final Object key, final InternalCacheEntry cacheEntry) {
      CacheEntry ce = lookupEntry(key);
      if (ce == null || ce.isNull() || ce.getValue() == null) {
         if (ce != null) {
            ce.setValue(cacheEntry.getValue());
            ce.setMetadata(cacheEntry.getMetadata());
         } else {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean isEntryRemovedInContext(final Object key) {
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   @Override
   public SingleKeyNonTxInvocationContext clone() {
      try {
         return (SingleKeyNonTxInvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
   }

   public void resetState() {
      this.key = null;
      this.cacheEntry = null;
      this.isLocked = false;
   }

}
