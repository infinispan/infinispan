package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;
import java.util.UUID;

import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.QueryDefinition;

/**
 * Add specific behavior for ClusteredQueryCommand. Each ClusteredQueryCommandType links to a CQWorker
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
abstract class CQWorker {

   protected AdvancedCache<?, ?> cache;

   private KeyTransformationHandler keyTransformationHandler;

   // the query
   protected QueryDefinition queryDefinition;
   protected UUID queryId;
   protected int docIndex;

   void initialize(AdvancedCache<?, ?> cache, QueryDefinition queryDefinition, UUID queryId, int docIndex) {
      this.cache = cache;
      this.keyTransformationHandler = ComponentRegistryUtils.getQueryInterceptor(cache).getKeyTransformationHandler();
      if (queryDefinition != null) {
         this.queryDefinition = queryDefinition;
         this.queryDefinition.initialize(cache);
      }
      this.queryId = queryId;
      this.docIndex = docIndex;
   }

   abstract QueryResponse perform(BitSet segments);

   void setFilter(BitSet segments) {
      SearchQueryBuilder searchQuery = queryDefinition.getSearchQuery();
      if (segments.cardinality() != cache.getCacheConfiguration().clustering().hash().numSegments()) {
         searchQuery.routeOnSegments(segments);
      } else {
         searchQuery.noRouting();
      }
   }

   /**
    * Utility to extract the cache key of a DocumentExtractor and use the KeyTransformationHandler to turn the string
    * into the actual key object.
    *
    * @param documentId
    * @return
    */
   Object stringToKey(String documentId) {
      return keyTransformationHandler.stringToKey(documentId);
   }
}
