package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.util.TimSort;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Based on the same techniques outlined in the {@link SimpleDataContainer}, this implementation always forces the
 * collection of creation timestamps for entries.  This means that {@link org.infinispan.container.entries.ImmortalCacheEntry}
 * and {@link org.infinispan.container.entries.TransientCacheEntry} are never used, since only {@link org.infinispan.container.entries.MortalCacheEntry}
 * and {@link org.infinispan.container.entries.TransientMortalCacheEntry} instances capture timestamps.
 * <p/>
 * All gets, puts, etc are constant time operations.
 * <p/>
 * Iteration incurs a O(N log(N)) cost since the timestamps are sorted first, and there is an added memory overhead in
 * temporary space to hold sorted references.  When sorting, this implementation does not use the millisecond granularity
 * when ordering timestamps; instead it defaults to a 1-second granularity since the FIFO ordering does not need to be
 * strict and the TimSort implementation used for sorting performs significantly better with minimal reordering offered
 * by a coarser granularity.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
public class FIFOSimpleDataContainer extends SimpleDataContainer {
   // This is to facilitate faster sorting.  DO we really care about millisecond accuracy when ordering the collection?
   final static int DEFAULT_TIMESTAMP_GRANULARITY = 1000;

   private final Comparator<InternalCacheEntry> COMPARATOR;

   public FIFOSimpleDataContainer(int concurrencyLevel) {
      this(concurrencyLevel, true, false, new FIFOComparator(DEFAULT_TIMESTAMP_GRANULARITY));
   }

   public FIFOSimpleDataContainer(int concurrencyLevel, int timestampGranularity) {
      this(concurrencyLevel, true, false, new FIFOComparator(timestampGranularity));
   }

   FIFOSimpleDataContainer(int concurrencyLevel, boolean recordCreated, boolean recordLastUsed, Comparator<InternalCacheEntry> c) {
      super(concurrencyLevel, recordCreated, recordLastUsed);
      COMPARATOR = c;
   }


   @Override
   public Iterator<InternalCacheEntry> iterator() {
      InternalCacheEntry[] sortedEntries = new InternalCacheEntry[immortalEntries.size() + mortalEntries.size()];
      int i=0;
      for (InternalCacheEntry ice: immortalEntries.values()){
         if (i == sortedEntries.length) break;
         sortedEntries[i++] = ice;
      }

      for (InternalCacheEntry ice: mortalEntries.values()){
         if (i == sortedEntries.length) break;
         sortedEntries[i++] = ice;
      }

      TimSort.sort(sortedEntries, COMPARATOR);
      return Arrays.asList(sortedEntries).iterator();
   }

   private static final class FIFOComparator implements Comparator<InternalCacheEntry> {
      int timestampGranularity;

      private FIFOComparator(int timestampGranularity) {
         this.timestampGranularity = timestampGranularity;
      }

      @Override
      public int compare(InternalCacheEntry o1, InternalCacheEntry o2) {
         return (int) o1.getCreated() / timestampGranularity - (int) o2.getCreated() / timestampGranularity;
      }
   }
}