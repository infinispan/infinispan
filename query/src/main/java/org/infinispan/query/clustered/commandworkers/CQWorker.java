package org.infinispan.query.clustered.commandworkers;

import java.io.IOException;
import java.util.UUID;

import org.hibernate.search.exception.SearchException;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.impl.QueryDefinition;

/**
 * Add specific behavior for ClusteredQueryCommand. Each ClusteredQueryCommandType links to a CQWorker
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
abstract class CQWorker {

   protected AdvancedCache<?, ?> cache;

   private KeyTransformationHandler keyTransformationHandler;

   private QueryBox queryBox;

   private SearchIntegrator searchFactory;

   // the query
   protected QueryDefinition queryDefinition;
   protected UUID queryId;
   protected int docIndex;

   void init(AdvancedCache<?, ?> cache, QueryDefinition queryDefinition, UUID queryId, int docIndex) {
      this.cache = cache;
      this.keyTransformationHandler = KeyTransformationHandler.getInstance(cache);
      if (queryDefinition != null) {
         this.queryDefinition = queryDefinition;
         this.queryDefinition.initialize(cache);
      }
      this.queryId = queryId;
      this.docIndex = docIndex;
   }

   abstract QueryResponse perform();

   QueryBox getQueryBox() {
      if (queryBox == null) {
         queryBox = cache.getComponentRegistry().getLocalComponent(QueryBox.class);
      }
      return queryBox;
   }

   SearchIntegrator getSearchFactory() {
      if (searchFactory == null) {
         searchFactory = cache.getComponentRegistry().getComponent(SearchIntegrator.class);
      }
      return searchFactory;
   }

   /**
    * Utility to extract the cache key of a DocumentExtractor and use the KeyTransformationHandler to turn the string
    * into the actual key object.
    *
    * @param extractor
    * @param docIndex
    * @return
    */
   Object extractKey(DocumentExtractor extractor, int docIndex) {
      String strKey;
      try {
         strKey = (String) extractor.extract(docIndex).getId();
      } catch (IOException e) {
         throw new SearchException("Error while extracting key", e);
      }

      return keyTransformationHandler.stringToKey(strKey, cache.getClassLoader());
   }
}
