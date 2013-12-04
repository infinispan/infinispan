package org.infinispan.query.impl.massindex;

import java.util.Iterator;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.impl.batch.DefaultBatchBackend;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;

/**
 * This Reduce doesn't really index the entries but forwards them to the
 * appropriate index master; the backend knows how to deal with sharding
 * and this way we avoid unnecessary round trips.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public final class IndexingReducer implements Reducer<Object, LuceneWork> {

   private SearchFactoryIntegrator searchFactory;
   private DefaultMassIndexerProgressMonitor progressMonitor;
   private DefaultBatchBackend defaultBatchBackend;

   public void initialize(Cache<?, ?> inputCache) {
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(inputCache);
      searchFactory = queryInterceptor.getSearchFactory();
      progressMonitor = new DefaultMassIndexerProgressMonitor(inputCache.getAdvancedCache().getComponentRegistry()
                                                                         .getTimeService());
      defaultBatchBackend = new DefaultBatchBackend(searchFactory, progressMonitor);
   }

   @Override
   public LuceneWork reduce(Object reducedKey, Iterator<LuceneWork> iter) {
      try {
         while (iter.hasNext()) {
            LuceneWork work = iter.next();
            defaultBatchBackend.enqueueAsyncWork(work);
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return null;
   }

   /**
    * Since indexing work is done asynchronously in the backend, we need to flush at the end to
    * make sure we don't return control to user before all work was processed and flushed.
    */
   public void flush() {
      defaultBatchBackend.flush(searchFactory.getIndexedTypes());
   }

}
