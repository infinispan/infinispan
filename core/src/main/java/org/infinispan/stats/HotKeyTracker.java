package org.infinispan.stats;

import java.util.List;

import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.stats.impl.DefaultHotKeyTracker;
import org.infinispan.stats.impl.DisabledHotKeyTracker;

/**
 * Tracks per-cache hot key frequency for reads and writes independently.
 * <p>
 * When enabled, every origin-local cache operation contributes to a probabilistic
 * frequency sketch. The top-N most frequently accessed keys can be queried at any
 * time. When disabled, all operations are no-ops with zero overhead.
 *
 * @since 16.3
 */
public interface HotKeyTracker {

   static HotKeyTracker create(int k, int numSegments) {
      return k > 0 ? new DefaultHotKeyTracker(k, numSegments) : DisabledHotKeyTracker.instance();
   }

   /**
    * Records a read access for the given key.
    *
    * @param key the key that was read
    * @param segment the segment the key belongs to
    */
   void recordRead(Object key, int segment);

   /**
    * Records a write access for the given key.
    *
    * @param key the key that was written
    * @param segment the segment the key belongs to
    */
   void recordWrite(Object key, int segment);

   /**
    * Returns the most frequently read keys in descending order of frequency.
    *
    * @param n maximum number of entries to return
    * @return top read keys with their approximate counts, at most {@code n} entries
    */
   List<HeavyKeeper.KeyFrequency<Object>> getTopReads(int n);

   /**
    * Returns the most frequently written keys in descending order of frequency.
    *
    * @param n maximum number of entries to return
    * @return top written keys with their approximate counts, at most {@code n} entries
    */
   List<HeavyKeeper.KeyFrequency<Object>> getTopWrites(int n);

   /**
    * Returns the total number of read accesses recorded since the last reset.
    *
    * @return total read count
    */
   long totalReads();

   /**
    * Returns the total number of write accesses recorded since the last reset.
    *
    * @return total write count
    */
   long totalWrites();

   /**
    * Returns the total read count for a specific segment.
    *
    * @param segment the segment index
    * @return total reads recorded for that segment
    */
   long segmentReads(int segment);

   /**
    * Returns the total write count for a specific segment.
    *
    * @param segment the segment index
    * @return total writes recorded for that segment
    */
   long segmentWrites(int segment);

   /**
    * Resets all tracked frequencies and total counts to zero.
    */
   void reset();
}
