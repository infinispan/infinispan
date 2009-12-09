package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;

import java.util.Comparator;

/**
 * Based on the same techniques outlined in the {@link SimpleDataContainer}, this implementation always forces the
 * collection of last used timestamps for entries.  This means that {@link org.infinispan.container.entries.ImmortalCacheEntry}
 * and {@link org.infinispan.container.entries.MortalCacheEntry} are never used, since only {@link org.infinispan.container.entries.TransientCacheEntry}
 * and {@link org.infinispan.container.entries.TransientMortalCacheEntry} instances capture timestamps.
 * <p/>
 * All gets, puts, etc are constant time operations.
 * <p/>
 * Iteration incurs a O(N log(N)) cost since the timestamps are sorted first, and there is an added memory overhead in
 * temporary space to hold sorted references.  When sorting, this implementation does not use the millisecond granularity
 * when ordering timestamps; instead it defaults to a 1-second granularity since the LRU ordering does not need to be
 * strict and the TimSort implementation used for sorting performs significantly better with minimal reordering offered
 * by a coarser granularity.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
public class LRUSimpleDataContainer extends FIFOSimpleDataContainer {
   public LRUSimpleDataContainer(int concurrencyLevel) {
      super(concurrencyLevel, false, true, new LRUComparator(DEFAULT_TIMESTAMP_GRANULARITY));
   }

   public LRUSimpleDataContainer(int concurrencyLevel, int timestampGranularity) {
      super(concurrencyLevel, false, true, new LRUComparator(timestampGranularity));
   }

   private static final class LRUComparator implements Comparator<InternalCacheEntry> {
      int timestampGranularity;

      private LRUComparator(int timestampGranularity) {
         this.timestampGranularity = timestampGranularity;
      }

      @Override
      public int compare(InternalCacheEntry o1, InternalCacheEntry o2) {
         return (int) o1.getLastUsed() / timestampGranularity - (int) o2.getLastUsed() / timestampGranularity;
      }
   }
}