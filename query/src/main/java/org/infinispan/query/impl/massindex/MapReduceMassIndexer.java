package org.infinispan.query.impl.massindex;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class MapReduceMassIndexer implements MassIndexer {

   private final AdvancedCache<Object, Object> cache;

   public MapReduceMassIndexer(AdvancedCache cache, SearchFactoryIntegrator searchFactory) {
      this.cache = cache;
   }

   @Override
   public void start() {
      wipeExistingIndexes();
      new MapReduceTask<Object, Object, Object, LuceneWork>(cache)
         .mappedWith(new IndexingMapper())
         .reducedWith(new IndexingReducer())
         .execute();
   }

   private void wipeExistingIndexes() {
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      queryInterceptor.purgeAllIndexes();
   }

}
