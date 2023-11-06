package org.infinispan.functional.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * An internal interface for implementation of {@link FunctionalMap} for a simple cache.
 *
 * @since 15.0
 * @param <K>: Map key type.
 * @param <V>: Map value type.
 */
interface SimpleFunctionalMap<K, V> extends FunctionalMap<K, V> {

   @SuppressWarnings("unchecked")
   default K toStorageKey(K key) {
      return (K) cache().getAdvancedCache()
            .getKeyDataConversion()
            .toStorage(key);
   }

   default <R> R toLocalExecution(CompletableFuture<R> cf) {
      assert cf.isDone() : "CompletableFuture was not done!";
      return CompletionStages.join(cf);
   }

   default MVCCEntry<K, V> readCacheEntry(K key, InternalCacheEntry<K, V> ice) {
      if (ice == null) {
         return new ReadCommittedEntry<>(key, null, null);
      }

      V value;
      Metadata metadata;
      PrivateMetadata internalMetadata;
      synchronized (ice) {
         value = ice.getValue();
         metadata = ice.getMetadata();
         internalMetadata = ice.getInternalMetadata();
      }

      MVCCEntry<K, V> mvccEntry = new ReadCommittedEntry<>(key, value, metadata);
      mvccEntry.setInternalMetadata(internalMetadata);
      mvccEntry.setCreated(ice.getCreated());
      mvccEntry.setLastUsed(ice.getLastUsed());
      return mvccEntry;
   }
}
