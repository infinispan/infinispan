package org.infinispan.context;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.ContextLock;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea Markus
 * @author Sanne Grinovero
 * @deprecated Since 9.0, this class is going to be moved to an internal package.
 */
@Deprecated
public final class SingleKeyNonTxInvocationContext implements InvocationContext {
   private static Log log = LogFactory.getLog(SingleKeyNonTxInvocationContext.class);
   private static boolean trace = log.isTraceEnabled();

   private static AtomicReferenceFieldUpdater<SingleKeyNonTxInvocationContext, Object> contextLockUpdater =
         AtomicReferenceFieldUpdater.newUpdater(SingleKeyNonTxInvocationContext.class, Object.class, "contextLock");

   /**
    * It is possible for the key to only be wrapped but not locked, e.g. when a get takes place.
    */
   private boolean isLocked;

   private Object key;

   private CacheEntry cacheEntry;

   //TODO move the Origin's address to the InvocationContextFactory when isOriginLocal=true -> all addresses are the same  (Memory allocation cost)
   //(verify if this is worth it by looking at object alignment - would need a different implementation as pointing to null wouldn't help)
   private final Address origin;

   private Object lockOwner;

   // Let the creating thread automatically acquire it.
   private volatile Object contextLock = ContextLock.init();

   public SingleKeyNonTxInvocationContext(final Address origin) {
      this.origin = origin;
   }

   @Override
   public boolean isOriginLocal() {
      assertContextLock();
      return origin == null;
   }

   @Override
   public boolean isInTxScope() {
      assertContextLock();
      return false;
   }

   @Override
   public Object getLockOwner() {
      assertContextLock();
      return lockOwner;
   }

   @Override
   public void setLockOwner(Object lockOwner) {
      this.lockOwner = lockOwner;
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
      assertContextLock();
      return isLocked ? Collections.singleton(key) : Collections.emptySet();
   }

   @Override
   public void clearLockedKeys() {
      assertContextLock();
      isLocked = false;
   }

   @Override
   public void addLockedKey(final Object key) {
      assertContextLock();
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
      assertContextLock();
      if (this.key != null && isKeyEquals(key))
         return cacheEntry;

      return null;
   }

   public boolean isKeyEquals(Object key) {
      assertContextLock();
      return this.key == key || this.key.equals(key);
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      assertContextLock();
      return cacheEntry == null ? Collections.emptyMap() : Collections.singletonMap(key, cacheEntry);
   }

   @Override
   public void forEachEntry(BiConsumer<Object, CacheEntry> action) {
      assertContextLock();
      if (cacheEntry != null) {
         action.accept(key, cacheEntry);
      }
   }

   @Override
   public int lookedUpEntriesCount() {
      assertContextLock();
      return cacheEntry != null ? 1 : 0;
   }

   @Override
   public void putLookedUpEntry(final Object key, final CacheEntry e) {
      assertContextLock();
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
      assertContextLock();
      if (this.key != null && isKeyEquals(key)) {
         this.cacheEntry = null;
      }
   }

   public Object getKey() {
      assertContextLock();
      return key;
   }

   public CacheEntry getCacheEntry() {
      assertContextLock();
      return cacheEntry;
   }

   @Override
   public Address getOrigin() {
      assertContextLock();
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
      return isLocked && isKeyEquals(key);
   }

   @Override
   public boolean isEntryRemovedInContext(final Object key) {
      assertContextLock();
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   @Override
   public CompletionStage<Void> enter() {
      if (trace) log.tracef("Entering context %08X", System.identityHashCode(this));
      CompletionStage<Void> cs = ContextLock.enter(this, contextLockUpdater);
      if (trace) {
         log.tracef("Current context lock %s", cs);
      }
      return cs;
   }

   @Override
   public void exit() {
      if (trace) log.tracef("Leaving context %08X", System.identityHashCode(this));
      ContextLock.exit(this, contextLockUpdater);
   }

   private void assertContextLock() {
      Object contextLock = this.contextLock;
      assert ContextLock.isOwned(contextLock) : "Context lock is " + contextLock;
   }


   public void resetState() {
      this.key = null;
      this.cacheEntry = null;
      this.isLocked = false;
      CompletionStage<Void> cs = enter();
      assert cs == null;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("SingleKeyNonTxInvocationContext{");
      sb.append("isLocked=").append(isLocked);
      sb.append(", key=").append(key);
      sb.append(", cacheEntry=").append(cacheEntry);
      sb.append(", origin=").append(origin);
      sb.append(", lockOwner=").append(lockOwner);
      sb.append('}');
      return sb.toString();
   }
}
