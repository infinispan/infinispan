package org.infinispan.query.impl.massindex;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.impl.batch.DefaultBatchBackend;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.query.MassIndexer;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class MapReduceMassIndexer implements MassIndexer {

   private final AdvancedCache<Object, Object> cache;
   private final SearchFactoryIntegrator searchFactory;
   private boolean flushPerNodeEnabled = false;  

   public MapReduceMassIndexer(AdvancedCache cache, SearchFactoryIntegrator searchFactory) {
      this.cache = cache;
      this.searchFactory = searchFactory;
   }

   @Override
   public void start() {
      wipeExistingIndexes();
      new MapReduceTask<Object, Object, Object, LuceneWork>(cache)
            .mappedWith(new IndexingMapper(flushPerNodeEnabled))
            .reducedWith(new IndexingReducer())
            .execute();
      flush();
   }


   @Override
   public boolean isFlushPerNodeEnabled() {
      return flushPerNodeEnabled;
   }

   @Override
   public void setFlushPerNodeEnabled(boolean enabled) {
      this.flushPerNodeEnabled = enabled;
   }

   private void wipeExistingIndexes() {
      if (flushPerNodeEnabled) {
         new PerNodeIndexPurger(cache).purge();
      } else {
         new SingleNodeIndexPurger(cache).purge();
      }
   }

   private void flush() {
      if (!flushPerNodeEnabled) {
         DefaultMassIndexerProgressMonitor progressMonitor = new DefaultMassIndexerProgressMonitor(cache.getAdvancedCache().getComponentRegistry().getTimeService());
         new DefaultBatchBackend(searchFactory, progressMonitor).flush(searchFactory.getIndexedTypes());
      }
   }

}
