package org.infinispan.conflict;

import java.util.List;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.partitionhandling.PartitionHandling;

/**
 * @author Ryan Emerson
 * @since 9.1
 */
public interface EntryMergePolicy<K, V> {

   /**
    * This method is called by {@link ConflictManager#resolveConflicts()} for each conflict discovered to determine
    * which {@link CacheEntry} should be utilised. This merge policy is used when a user explicitly calls {@link ConflictManager#resolveConflicts()}
    * as well as when a partition merge occurs with {@link PartitionHandling#ALLOW_READ_WRITES} set.
    *
    * In the event of a partition merge, we define the preferred partition as the partition whom's coordinator is coordinating
    * the current merge.
    *
    * @param preferredEntry During a partition merge, the preferred entry is the entry that belongs to the preferred partition.
    *                       If multiple entries exist in the preferred partition, then the preferred entry is the primary
    *                       owner in the preferred partition. If no entry exists in the preferred partition, then the
    *                       preferredEntry is null.
    *
    *                       During a non-merge call to {@link ConflictManager#resolveConflicts()}, the preferredEntry is
    *                       simply the primary owner of an entry
    *
    * @param otherEntries a {@link List} of all other {@link CacheEntry} associated with a given Key.
    * @return the winning {@link CacheEntry} to be utilised across the cluster, or null if all entries for a key should be
    * removed.
    */
   CacheEntry<K, V> merge(CacheEntry<K, V> preferredEntry, List<CacheEntry<K, V>> otherEntries);
}
