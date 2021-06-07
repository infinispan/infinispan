package org.infinispan.query.clustered;

import java.util.Map;

import org.apache.lucene.search.Sort;
import org.infinispan.AdvancedCache;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.remoting.transport.Address;

/**
 * Iterates on the results of a distributed query returning the whole cache entry, both key and value.
 *
 * @author anistor@redhat.com
 * @since 13.0
 */
class DistributedEntryIterator<K, V> extends DistributedIterator<Map.Entry<K, V>> {

   DistributedEntryIterator(LocalQueryStatistics queryStatistics, Sort sort, int fetchSize, int resultSize,
                            int maxResults, int firstResult, Map<Address, NodeTopDocs> topDocsResponses,
                            AdvancedCache<?, ?> cache) {
      super(queryStatistics, sort, fetchSize, resultSize, maxResults, firstResult, topDocsResponses, cache);
   }

   @Override
   protected Map.Entry<K, V> decorate(Object key, Object value) {
      return new Map.Entry<K, V>() {

         @Override
         public K getKey() {
            return (K) key;
         }

         @Override
         public V getValue() {
            return (V) value;
         }

         @Override
         public V setValue(V value) {
            throw new UnsupportedOperationException("Entry is immutable");
         }
      };
   }
}
