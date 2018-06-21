package org.infinispan.hibernate.cache.commons.access;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;

/**
 * This is a copy of {@link org.infinispan.context.SingleKeyNonTxInvocationContext}
 * which carries extra info about the session.
 */
class SessionInvocationContext implements InvocationContext {
   private boolean isLocked;
   private Object key;
   private CacheEntry cacheEntry;
   private final Object lockOwner;
   private final Object session;

   public SessionInvocationContext(Object session, Object lockOwner) {
      this.session = session;
      this.lockOwner = lockOwner;
   }

   @Override
   public boolean isOriginLocal() {
      return true;
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
      throw new UnsupportedOperationException();
   }

   @Override
   public InvocationContext clone() {
      try {
         return (InvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!", e);
      }
   }

   @Override
   public Set<Object> getLockedKeys() {
      return isLocked ? Collections.singleton(key) : Collections.emptySet();
   }

   @Override
   public void clearLockedKeys() {
      isLocked = false;
   }

   @Override
   public void addLockedKey(final Object key) {
      if (this.key == null) {
         // Set the key here
         this.key = key;
      } else if (!isKeyEquals(key)) {
         throw illegalStateException();
      }

      isLocked = true;
   }

   private IllegalStateException illegalStateException() {
      return new IllegalStateException("This is a single key invocation context, using multiple keys shouldn't be possible");
   }

   @Override
   public CacheEntry lookupEntry(final Object key) {
      if (this.key != null && isKeyEquals(key))
         return cacheEntry;

      return null;
   }

   public boolean isKeyEquals(Object key) {
      return this.key == key || this.key.equals(key);
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return cacheEntry == null ? Collections.emptyMap() : Collections.singletonMap(key, cacheEntry);
   }

   @Override
   public void forEachEntry(BiConsumer<Object, CacheEntry> action) {
      if (cacheEntry != null) {
         action.accept(key, cacheEntry);
      }
   }

   @Override
   public int lookedUpEntriesCount() {
      return cacheEntry != null ? 1 : 0;
   }

   @Override
   public void putLookedUpEntry(final Object key, final CacheEntry e) {
      if (this.key == null) {
         // Set the key here
         this.key = key;
      } else if (!isKeyEquals(key)) {
         throw illegalStateException();
      }

      this.cacheEntry = e;
   }

   @Override
   public void removeLookedUpEntry(final Object key) {
      if (this.key != null && isKeyEquals(key)) {
         this.cacheEntry = null;
      }
   }

   public Object getKey() {
      return key;
   }

   @Override
   public Address getOrigin() {
      return LocalModeAddress.INSTANCE;
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
      return isLocked && isKeyEquals(key);
   }

   @Override
   public boolean isEntryRemovedInContext(final Object key) {
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   public Object getSession() {
      return session;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("SingleKeyNonTxInvocationContext{");
      sb.append("isLocked=").append(isLocked);
      sb.append(", key=").append(key);
      sb.append(", cacheEntry=").append(cacheEntry);
      sb.append(", lockOwner=").append(lockOwner);
      sb.append(", session=").append(session);
      sb.append('}');
      return sb.toString();
   }
}
