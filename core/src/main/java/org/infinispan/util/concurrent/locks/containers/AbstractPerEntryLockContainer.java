package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.util.concurrent.locks.RefCountingLock;
import org.infinispan.util.logging.Log;

import java.util.concurrent.TimeUnit;

import static org.infinispan.commons.util.Util.toStr;

/**
 * An abstract lock container that creates and maintains a new lock per entry
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractPerEntryLockContainer<L extends RefCountingLock> extends AbstractLockContainer<L> {

   // We specifically need a CHMV8, to be able to use methods like computeIfAbsent, computeIfPresent and compute
   protected final EquivalentConcurrentHashMapV8<Object, L> locks;

   protected AbstractPerEntryLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      locks = new EquivalentConcurrentHashMapV8<Object, L>(
            16, concurrencyLevel, keyEquivalence, AnyEquivalence.getInstance());
   }

   protected abstract L newLock();

   @Override
   public final L getLock(Object key) {
      return locks.get(key);
   }

   @Override
   public int getNumLocksHeld() {
      return locks.size();
   }

   @Override
   public int size() {
      return locks.size();
   }

   @Override
   public L acquireLock(final Object lockOwner, final Object key, final long timeout, final TimeUnit unit) throws InterruptedException {
      final ByRef<Boolean> lockAcquired = ByRef.create(Boolean.FALSE);
      L lock = locks.compute(key, new EquivalentConcurrentHashMapV8.BiFun<Object, L, L>() {
         @Override
         public L apply(Object key, L lock) {
            // This happens atomically in the CHM
            if (lock == null) {
               Log log = getLog();
               if (log.isTraceEnabled())
                  log.tracef("Creating and acquiring new lock instance for key %s", toStr(key));

               lock = newLock();
               // Since this is a new lock, it is certainly uncontended.
               lock(lock, lockOwner);
               lockAcquired.set(Boolean.TRUE);
               return lock;
            }

            // No need to worry about concurrent updates - there can't be a release in progress at the same time
            int refCount = lock.getReferenceCounter().incrementAndGet();
            if (refCount <= 1) {
               throw new IllegalStateException("Lock " + key + " acquired although it should have been removed: " + lock);
            }
            return lock;
         }
      });

      if (!lockAcquired.get()) {
         // We retrieved a lock that was already present,
         lockAcquired.set(tryLock(lock, timeout, unit, lockOwner));
      }

      if (lockAcquired.get())
         return lock;
      else {
         getLog().tracef("Timed out attempting to acquire lock for key %s after %s", key, Util.prettyPrintTime(timeout, unit));

         // We didn't acquire the lock, but we still incremented the reference count.
         // We may need to delete the entry if the owner thread released it just after we timed out.
         // We use an atomic operation here as another thread might be trying to increment the ref count
         // at the same time (otherwise it would make the acquire function at the beginning more complicated).
         locks.computeIfPresent(key, new EquivalentConcurrentHashMapV8.BiFun<Object, L, L>() {
            @Override
            public L apply(Object key, L lock) {
               // This will happen atomically in the CHM
               // We have a reference, so value can't be null
               boolean remove = lock.getReferenceCounter().decrementAndGet() == 0;
               return remove ? null : lock;
            }
         });
         return null;
      }
   }

   @Override
   public void releaseLock(final Object lockOwner, Object key) {
      locks.computeIfPresent(key, new EquivalentConcurrentHashMapV8.BiFun<Object, L, L>() {
         @Override
         public L apply(Object key, L lock) {
            // This will happen atomically in the CHM
            // We have a reference, so value can't be null
            Log log = getLog();
            if (log.isTraceEnabled())
               log.tracef("Unlocking lock instance for key %s", toStr(key));

            unlock(lock, lockOwner);

            int refCount = lock.getReferenceCounter().decrementAndGet();
            boolean remove = refCount == 0;
            if (refCount < 0) {
               throw new IllegalStateException("Negative reference count for lock " + key + ": " + lock);
            }

            // Ok, unlock was successful.  If the unlock was not successful, an exception will propagate and the entry will not be changed.
            return remove ? null : lock;
         }
      });
   }

   @Override
   public int getLockId(Object key) {
      L lock = getLock(key);
      return lock == null ? -1 : System.identityHashCode(lock);
   }

   @Override
   public String toString() {
      return "AbstractPerEntryLockContainer{" +
            "locks=" + locks +
            '}';
   }
}
