package org.infinispan.query.impl.massindex;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.impl.batch.DefaultBatchBackend;
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
   private final SearchFactoryIntegrator searchFactory;

   public MapReduceMassIndexer(AdvancedCache cache, SearchFactoryIntegrator searchFactory) {
      this.cache = cache;
      this.searchFactory = searchFactory;
   }

   @Override
   public void start() {
      wipeExistingIndexes();
      new MapReduceTask<Object, Object, Object, LuceneWork>(cache)
            .mappedWith(new IndexingMapper())
            .reducedWith(new IndexingReducer())
            .execute();
      flush();
   }

   private void wipeExistingIndexes() {
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      queryInterceptor.purgeAllIndexes();
   }

   private void flush() {
      DefaultMassIndexerProgressMonitor progressMonitor = new DefaultMassIndexerProgressMonitor(cache.getAdvancedCache().getComponentRegistry().getTimeService());
      new DefaultBatchBackend(searchFactory, progressMonitor).flush(searchFactory.getIndexedTypes());
   }

}
