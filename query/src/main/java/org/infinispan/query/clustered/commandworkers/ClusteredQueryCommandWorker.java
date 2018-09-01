package org.infinispan.query.clustered.commandworkers;

import java.util.UUID;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.clustered.QueryBox;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.impl.QueryDefinition;

/**
 * ClusteredQueryCommandWorker.
 *
 * Add specific behavior for ClusteredQueryCommand. Each ClusteredQueryCommandType links to a
 * ClusteredQueryCommandWorker
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public abstract class ClusteredQueryCommandWorker {

   protected Cache<?, ?> cache;

   private QueryBox queryBox;

   private SearchIntegrator searchFactory;

   // the query
   protected QueryDefinition queryDefinition;
   protected UUID lazyQueryId;
   protected int docIndex;

   public void init(Cache<?, ?> cache, QueryDefinition queryDefinition, UUID lazyQueryId, int docIndex) {
      this.cache = cache;
      if (queryDefinition != null) {
         this.queryDefinition = queryDefinition;
         this.queryDefinition.initialize(cache.getAdvancedCache());
      }
      this.lazyQueryId = lazyQueryId;
      this.docIndex = docIndex;
   }

   public abstract QueryResponse perform();

   protected QueryBox getQueryBox() {
      if (queryBox == null) {
         ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
         queryBox = cr.getLocalComponent(QueryBox.class);
      }

      return queryBox;
   }

   protected SearchIntegrator getSearchFactory() {
      if (searchFactory == null) {
         ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
         searchFactory = cr.getComponent(SearchIntegrator.class);
      }

      return searchFactory;
   }

}
