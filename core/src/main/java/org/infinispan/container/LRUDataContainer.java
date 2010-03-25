package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;

/**
 * Based on the same techniques outlined in the {@link org.infinispan.container.FIFODataContainer}, this implementation
 * additionally unlinks and re-links entries at the tail whenever entries are visited (using a get()) or are updated (a
 * put() on an existing key).
 * <p/>
 * Again, these are constant-time operations.
 * <p/>
 * Note though that this implementation does have a far lesser degree of concurrency when compared with its FIFO variant
 * due to the segment locking necessary even when doing a get() (since gets reorder links).  This has a knock-on effect
 * not just on get() but even on other write() operations since they all compete for the same segment lock (when working
 * on keys mapped to the same segment, of course).
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
@Deprecated
public class LRUDataContainer extends FIFODataContainer {

   public LRUDataContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   @Override
   public InternalCacheEntry get(Object k) {
      int h = hash(k.hashCode());
      Segment s = segmentFor(h);

      LinkedEntry le = s.get(k, h);
      InternalCacheEntry ice = null;
      if (le != null) ice = le.e;
      if (ice != null) {
         if (ice.isExpired()) {
            remove(k);
            ice = null;
         } else {
            ice.touch();
            boolean needToUnlockSegment = false;
            try {
               s.lock(); // we need to lock this segment to safely update links
               needToUnlockSegment = true;
               updateLinks(le);
            } finally {
               if (needToUnlockSegment) s.unlock();
            }
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
      LinkedEntry le;
      boolean newEntry = false;
      try {
         le = s.get(k, h);
         InternalCacheEntry ice = le == null ? null : le.e;
         if (ice == null) {
            newEntry = true;
            ice = InternalEntryFactory.create(k, v, lifespan, maxIdle);
            le = new LinkedEntry(ice);
         } else {
            ice.setValue(v);
            ice = entryFactory.update(ice, lifespan, maxIdle);
            // need to do this anyway since the ICE impl may have changed
            le.e = ice;
         }

         s.locklessPut(k, h, le);

         if (newEntry) {
            linkAtEnd(le);
         } else {
            updateLinks(le);
         }

      } finally {
         s.unlock();
      }
   }

   protected final void updateLinks(LinkedEntry le) {
      unlink(le);
      linkAtEnd(le);
   }
}