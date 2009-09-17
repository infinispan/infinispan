package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;

/**
 * A data container that exposes an iterator that is ordered based on least recently used (visited) entries first.
 * <p/>
 * This builds on the {@link SpinLockBasedFIFODataContainer} by calling {@link
 * SpinLockBasedLRUDataContainer#updateLinks(org.infinispan.container.SpinLockBasedFIFODataContainer.LinkedEntry)} even for
 * {@link #get(Object)} invocations to make sure ordering is intact, as per LRU.
 * <p/>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@ThreadSafe
public class SpinLockBasedLRUDataContainer extends SpinLockBasedFIFODataContainer {

   @Override
   public InternalCacheEntry get(Object k) {
      int h = hash(k.hashCode());
      Segment s = segmentFor(h);
      LinkedEntry le = s.get(k, h);
      InternalCacheEntry ice = le == null ? null : le.entry;
      if (ice != null) {
         if (ice.isExpired()) {
            remove(k);
            ice = null;
         } else {
            ice.touch();
            updateLinks(le);
         }
      }

      return ice;
   }
   
   @Override
   public void put(Object k, Object v, long lifespan, long maxIdle) {
      // do a normal put first.
      int h = hash(k.hashCode());
      Segment s = segmentFor(h);
      s.lock();
      LinkedEntry le = null;
      Aux before = null, after = null;
      boolean newEntry = false;
      try {
         le = s.get(k, h);
         InternalCacheEntry ice = le == null ? null : le.entry;
         if (ice == null) {
            newEntry = true;
            ice = InternalEntryFactory.create(k, v, lifespan, maxIdle);
            // only update linking if this is a new entry
            le = new LinkedEntry();
            le.lock();
            after = new Aux();
            after.lock();
            le.next = after;
            after.next = dummyEntry;
         } else {
            ice.setValue(v);
            ice = ice.setLifespan(lifespan).setMaxIdle(maxIdle);
            updateLinks(le);
         }

         le.entry = ice;
         s.locklessPut(k, h, le);

         if (newEntry) {
            dummyEntry.lock();
            (before = dummyEntry.prev).lock();
            before.next = le;
            le.prev = before;
            dummyEntry.prev = after;
         }
      } finally {
         if (newEntry) {
            if (le != null) {
               before.unlock();
               dummyEntry.unlock();
               after.unlock();
               le.unlock();
            }
         }
         s.unlock();
      }
   }

   /**
    * Updates links on this entry, moving it to the end of the linked list
    *
    * @param l linked entry to update
    */
   protected final void updateLinks(LinkedEntry l) {
      if (l.next != dummyEntry.prev) {

         // if we cannot lock l it means it is being updated by another process, either removing it or updating it anyway
         // so we can skip updating links in that case.
         if (l.tryLock()) {
            try {
               Aux before = l.prev;
               before.lock();
               Aux after = l.next;
               after.lock();

               LinkedEntry nextEntry = after.next;
               nextEntry.lock();
               dummyEntry.lock();
               Aux last = dummyEntry.prev;
               last.lock();

               try {
                  last.next = l;
                  l.prev = last;
                  after.next = dummyEntry;
                  dummyEntry.prev = after;
                  nextEntry.prev = before;
                  before.next = nextEntry;
               } finally {
                  last.unlock();
                  dummyEntry.unlock();
                  nextEntry.unlock();
                  after.unlock();
                  before.unlock();
               }
            } finally {
               l.unlock();
            }
         }
      }
   }
}
