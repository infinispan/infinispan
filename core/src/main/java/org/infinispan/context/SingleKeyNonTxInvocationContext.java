package org.infinispan.context;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import org.infinispan.container.entries.CacheEntry;
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
   private static AtomicReferenceFieldUpdater currentThreadUpdater =
         AtomicReferenceFieldUpdater.newUpdater(SingleKeyNonTxInvocationContext.class, Thread.class, "currentThread");

   private static Log log = LogFactory.getLog(SingleKeyNonTxInvocationContext.class);
   private static boolean trace = log.isTraceEnabled();
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
   private volatile Thread currentThread = Thread.currentThread();

   public SingleKeyNonTxInvocationContext(final Address origin) {
      this.origin = origin;
   }

   @Override
   public boolean isOriginLocal() {
      assert currentThread == Thread.currentThread();
      return origin == null;
   }

   @Override
   public boolean isInTxScope() {
      assert currentThread == Thread.currentThread();
      return false;
   }

   @Override
   public Object getLockOwner() {
      assert currentThread == Thread.currentThread();
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
      assert currentThread == Thread.currentThread();
      return isLocked ? Collections.singleton(key) : Collections.emptySet();
   }

   @Override
   public void clearLockedKeys() {
      assert currentThread == Thread.currentThread();
      isLocked = false;
   }

   @Override
   public void addLockedKey(final Object key) {
      assert currentThread == Thread.currentThread();
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
      assert currentThread == Thread.currentThread();
      if (this.key != null && isKeyEquals(key))
         return cacheEntry;

      return null;
   }

   public boolean isKeyEquals(Object key) {
      assert currentThread == Thread.currentThread();
      return this.key == key || this.key.equals(key);
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      assert currentThread == Thread.currentThread();
      return cacheEntry == null ? Collections.emptyMap() : Collections.singletonMap(key, cacheEntry);
   }

   @Override
   public void forEachEntry(BiConsumer<Object, CacheEntry> action) {
      assert currentThread == Thread.currentThread();
      if (cacheEntry != null) {
         action.accept(key, cacheEntry);
      }
   }

   @Override
   public int lookedUpEntriesCount() {
      assert currentThread == Thread.currentThread();
      return cacheEntry != null ? 1 : 0;
   }

   @Override
   public void putLookedUpEntry(final Object key, final CacheEntry e) {
      assert currentThread == Thread.currentThread();
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
      assert currentThread == Thread.currentThread();
      if (this.key != null && isKeyEquals(key)) {
         this.cacheEntry = null;
      }
   }

   public Object getKey() {
      assert currentThread == Thread.currentThread();
      return key;
   }

   public CacheEntry getCacheEntry() {
      assert currentThread == Thread.currentThread();
      return cacheEntry;
   }

   @Override
   public Address getOrigin() {
      assert currentThread == Thread.currentThread();
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
      assert currentThread == Thread.currentThread();
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   @Override
   public CompletionStage<Void> enter() {
      if (trace) log.trace("Entering context");
      if (currentThreadUpdater.compareAndSet(this, null, Thread.currentThread())) {
         return null;
      } else {
         // we might miss the thread when doing the log...
         throw new IllegalStateException("Concurrent access by " + currentThread + ", we are " + Thread.currentThread());
      }
   }

   @Override
   public void exit() {
      if (trace) log.trace("Leaving context");
      if (!currentThreadUpdater.compareAndSet(this, Thread.currentThread(), null)) {
         throw new IllegalStateException("Unexpected locking thread: " + currentThread + ", we are " + Thread.currentThread());
      }
   }

   public void resetState() {
      assert currentThread == Thread.currentThread();
      this.key = null;
      this.cacheEntry = null;
      this.isLocked = false;
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
