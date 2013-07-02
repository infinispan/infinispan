package org.infinispan.query.clustered.commandworkers;

import java.util.UUID;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.clustered.QueryBox;
import org.infinispan.query.clustered.QueryResponse;

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

   private SearchFactoryIntegrator searchFactory;

   // the query
   protected HSQuery query;
   protected UUID lazyQueryId;
   protected int docIndex;

   public void init(Cache<?, ?> cache, HSQuery query, UUID lazyQueryId, int docIndex) {
      this.cache = cache;
      this.query = query;
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

   protected SearchFactoryIntegrator getSearchFactory() {
      if (searchFactory == null) {
         ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
         searchFactory = cr.getComponent(SearchFactoryIntegrator.class);
      }

      return searchFactory;
   }

}
