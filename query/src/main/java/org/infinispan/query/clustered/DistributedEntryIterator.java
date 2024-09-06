package org.infinispan.query.clustered;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.search.Sort;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.impl.QueryKeyConverter;
import org.infinispan.remoting.transport.Address;

/**
 * Iterates on the results of a distributed query returning the whole cache entry, both key and value.
 *
 * @author anistor@redhat.com
 * @since 13.0
 */
class DistributedEntryIterator<K, V> extends DistributedIterator<EntityEntry<K, V>> {

   private final boolean withMetadata;
   private final QueryKeyConverter queryKeyConverter;

   DistributedEntryIterator(LocalQueryStatistics queryStatistics, Sort sort, int resultSize,
                            int maxResults, int firstResult, Map<Address, NodeTopDocs> topDocsResponses,
                            AdvancedCache<?, ?> cache, boolean withMetadata) {
      super(queryStatistics, sort, resultSize, maxResults, firstResult, topDocsResponses, cache);
      this.withMetadata = withMetadata;
      queryKeyConverter = new QueryKeyConverter(cache);
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

      Set<Object> keySet = keysAndScores.stream()
            .map(keyAndScore -> keyAndScore.key)
            .collect(Collectors.toSet());
      Map<?, CacheEntry<Object, Object>> convertedMap =
            queryKeyConverter.convertEntries(cache.getAllCacheEntries(keySet));
      keysAndScores.forEach(bla -> bla.covertedKey = queryKeyConverter.convert(bla.key));
      keysAndScores.stream()
            .map(keyAndScore -> {
               CacheEntry<Object, Object> cacheEntry = convertedMap.get(keyAndScore.covertedKey);
               return decorate(keyAndScore.key,
                     cacheEntry.getValue(), keyAndScore.score, cacheEntry.getMetadata());
            })
            .forEach(values::add);
   }
}
