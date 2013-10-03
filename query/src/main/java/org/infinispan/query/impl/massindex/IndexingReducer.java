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

   private DefaultMassIndexerProgressMonitor progressMonitor;
   private DefaultBatchBackend defaultBatchBackend;

   public void initialize(Cache<?, ?> inputCache) {
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(inputCache);
      SearchFactoryIntegrator searchFactory = queryInterceptor.getSearchFactory();
      this.progressMonitor = new DefaultMassIndexerProgressMonitor(inputCache.getAdvancedCache().getComponentRegistry()
                                                                         .getTimeService());
      this.defaultBatchBackend = new DefaultBatchBackend(searchFactory, progressMonitor);
   }

   @Override
   public LuceneWork reduce(Object reducedKey, Iterator<LuceneWork> iter) {
      try {
         while (iter.hasNext()) {
            defaultBatchBackend.enqueueAsyncWork(iter.next());
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return null;
   }

}
