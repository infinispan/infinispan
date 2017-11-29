package org.infinispan.stats.impl;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class StatKeys {
   static final String TIME_SINCE_RESET = "timeSinceReset";
   static final String TIME_SINCE_START = "timeSinceStart";
   static final String REMOVE_MISSES = "removeMisses";
   static final String REMOVE_HITS = "removeHits";
   static final String AVERAGE_WRITE_TIME = "averageWriteTime";
   static final String AVERAGE_READ_TIME = "averageReadTime";
   static final String AVERAGE_REMOVE_TIME = "averageRemoveTime";
   static final String EVICTIONS = "evictions";
   static final String HITS = "hits";
   static final String MISSES = "misses";
   static final String NUMBER_OF_ENTRIES = "numberOfEntries";
   static final String NUMBER_OF_ENTRIES_IN_MEMORY = "numberOfEntriesInMemory";
   static final String OFF_HEAP_MEMORY_USED = "offHeapMemoryUsed";
   static final String RETRIEVALS = "retrievals";
   static final String STORES = "stores";
   static final String REQUIRED_MIN_NODES = "minRequiredNodes";

   //LockManager
   static final String NUMBER_OF_LOCKS_HELD = "numberOfLocksHeld";
   static final String NUMBER_OF_LOCKS_AVAILABLE = "numberOfLocksAvailable";

   //Invalidation/passivation/activation
   static final String INVALIDATIONS = "invalidations";
   static final String PASSIVATIONS = "passivations";
   static final String ACTIVATIONS = "activations";

   //cache loaders
   static final String CACHE_LOADER_LOADS = "cacheLoaderLoads";
   static final String CACHE_LOADER_MISSES = "cacheLoaderMisses";
   static final String CACHE_WRITER_STORES = "cacheWriterStores";
}
