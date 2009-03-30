package org.horizon.container;

import net.jcip.annotations.ThreadSafe;
import org.horizon.container.entries.InternalCacheEntry;

/**
 * A data container that exposes an iterator that is ordered based on least recently used (visited) entries first.
 * <p/>
 * This builds on the {@link org.horizon.container.FIFODataContainer} by calling {@link
 * org.horizon.container.LRUDataContainer#updateLinks(org.horizon.container.FIFODataContainer.LinkedEntry)} even for
 * {@link #get(Object)} invocations to make sure ordering is intact, as per LRU.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
public class LRUDataContainer extends FIFODataContainer {

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

   // TODO make sure even a put() on an existing entry updates links  

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
