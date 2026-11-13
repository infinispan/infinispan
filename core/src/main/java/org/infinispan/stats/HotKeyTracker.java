package org.infinispan.stats;

import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.configuration.cache.HotKeysConfiguration;
import org.infinispan.stats.impl.DefaultHotKeyTracker;
import org.infinispan.stats.impl.DisabledHotKeyTracker;

/**
 * Tracks per-cache hot key frequency for reads and writes independently.
 * <p>
 * When enabled, every origin-local cache operation contributes to a probabilistic
 * frequency sketch. The top-N most frequently accessed keys can be queried at any
 * time. When disabled, all operations are no-ops with zero overhead.
 * </p>
 *
 * @since 16.4
 * @author José Bolina
 */
public interface HotKeyTracker {

   /**
    * Creates the tracker for a cache.
    *
    * @param k           the number of keys to track per operation type; zero or less disables tracking
    * @param numSegments the number of segments of the cache
    * @return a tracker that records accesses when {@code k} is greater than zero, or a shared tracker that ignores
    *         them otherwise
    * @throws CacheConfigurationException if {@code k} is greater than {@link HotKeysConfiguration#MAX_TOP_K}
    */
   static HotKeyTracker create(int k, int numSegments) {
      return k > 0 ? new DefaultHotKeyTracker(k, numSegments) : DisabledHotKeyTracker.instance();
   }

   /**
    * Records a read access for the given key.
    * <p>
    * The key always counts towards the top read keys. The access counts towards the segment and total read counts only
    * when {@code segment} is a valid segment of the cache; an invalid segment is ignored and never makes this method
    * throw.
    * </p>
    *
    * @param key the key that was read
    * @param segment the segment the key belongs to
    */
   void recordRead(Object key, int segment);

   /**
    * Records a write access for the given key.
    * <p>
    * The key always counts towards the top written keys. The access counts towards the segment and total write counts
    * only when {@code segment} is a valid segment of the cache; an invalid segment is ignored and never makes this
    * method throw.
    * </p>
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
    * @param segment the segment index, between zero (inclusive) and the cache's number of segments (exclusive)
    * @return total reads recorded for that segment since the last reset
    * @throws IllegalArgumentException if {@code segment} is out of range
    */
   long segmentReads(int segment);

   /**
    * Returns the total write count for a specific segment.
    *
    * @param segment the segment index, between zero (inclusive) and the cache's number of segments (exclusive)
    * @return total writes recorded for that segment since the last reset
    * @throws IllegalArgumentException if {@code segment} is out of range
    */
   long segmentWrites(int segment);

   /**
    * Resets all tracked frequencies and total counts to zero.
    */
   void reset();
}
