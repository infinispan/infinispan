package org.infinispan.query.clustered;

import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Sort;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.remoting.transport.Address;

/**
 * Iterates on the results of a distributed query returning the whole cache entry, both key and value.
 *
 * @author anistor@redhat.com
 * @since 13.0
 */
class DistributedEntryIterator<K, V> extends DistributedIterator<EntityEntry<K, V>> {

   private final boolean withMetadata;

   DistributedEntryIterator(LocalQueryStatistics queryStatistics, Sort sort, int resultSize,
                            int maxResults, int firstResult, Map<Address, NodeTopDocs> topDocsResponses,
                            AdvancedCache<?, ?> cache, boolean withMetadata) {
      super(queryStatistics, sort, resultSize, maxResults, firstResult, topDocsResponses, cache);
      this.withMetadata = withMetadata;
   }

   @Override
   protected EntityEntry<K, V> decorate(Object key, Object value, float score, Metadata metadata) {
      return new EntityEntry<>((K) key, (V) value, score, metadata);
   }

   @Override
   protected void getAllAndStore(List<KeyAndScore> keysAndScores) {
      if (!withMetadata) {
         super.getAllAndStore(keysAndScores);
         return;
      }
      var map = cache.getAllCacheEntries(toKeySet(keysAndScores));
      keysAndScores.stream()
            .map(keyAndScore -> {
               var cacheEntry = map.get(keyAndScore.key());
               if (cacheEntry == null) {
                  return null;
               }
               return decorate(keyAndScore.key(), cacheEntry.getValue(), keyAndScore.score(), cacheEntry.getMetadata());
            })
            .forEach(values::add);
   }
}
