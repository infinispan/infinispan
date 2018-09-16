package org.infinispan.query.clustered.commandworkers;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.infinispan.commons.util.CollectionFactory;

/**
 * Each indexed cache in the cluster has a QueryBox instance. The QueryBox keeps the active lazy iterators
 * (actually it keeps the DocumentExtractor of the searches) on the cluster, so it can return values
 * for the queries in a "lazy" way.
 *
 * When a DistributedLazyIterator is created, every nodes creates a DocumentExtractor and register
 * it in your own QueryBox. So, the LazyIterator can fetch the values in a lazy way.
 *
 * EVICTION: NOT IMPLEMENTED!
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
public final class QueryBox {

   // TODO [anistor] see https://issues.jboss.org/browse/ISPN-9466
   // <query UUID, ISPNQuery>
   private final ConcurrentMap<UUID, DocumentExtractor> queries = CollectionFactory.makeConcurrentMap();

   /**
    * Kill the query (DocumentExtractor).
    *
    * @param queryId The id of the query
    */
   void kill(UUID queryId) {
      DocumentExtractor extractor = queries.remove(queryId);
      if (extractor != null) {
         extractor.close();
      }
   }

   /**
    * Get the query (DocumentExtractor) by id..
    *
    * @param queryId The id of the query
    * @return the DocumentExtractor
    */
   DocumentExtractor get(UUID queryId) {
      DocumentExtractor extractor = queries.get(queryId);
      if (extractor == null) {
         throw new IllegalStateException("Query not found: " + queryId);
      }
      return extractor;
   }

   /**
    * Register a query (DocumentExtractor), so we can lazily load the results.
    *
    * @param queryId   The id of the query
    * @param extractor The query
    */
   void put(UUID queryId, DocumentExtractor extractor) {
      queries.put(queryId, extractor);
   }
}
