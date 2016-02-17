package org.infinispan.query.impl.massindex;

import org.hibernate.search.backend.FlushLuceneWork;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.PurgeAllLuceneWork;
import org.hibernate.search.backend.impl.batch.DefaultBatchBackend;
import org.hibernate.search.backend.spi.BatchBackend;
import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.SearchIntegrator;

import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Arrays.stream;

/**
 * Decorates {@link org.hibernate.search.backend.impl.batch.DefaultBatchBackend} adding capacity of doing
 * synchronous purges and flushes.
 *
 * @author gustavonalle
 * @since 7.2
 */
public class ExtendedBatchBackend implements BatchBackend {
   private final DefaultBatchBackend defaultBatchBackend;
   private final SearchIntegrator integrator;
   private final MassIndexerProgressMonitor progressMonitor;

   public ExtendedBatchBackend(SearchIntegrator integrator, MassIndexerProgressMonitor progressMonitor) {
      this.integrator = integrator;
      this.progressMonitor = progressMonitor;
      this.defaultBatchBackend = new DefaultBatchBackend(integrator, progressMonitor);
   }

   public void purge(Set<Class<?>> entityTypes) {
      performShardAwareOperation(entityTypes, (im, type) -> {
         im.performStreamOperation(new PurgeAllLuceneWork(type), progressMonitor, false);
      });
      flush(entityTypes);
   }

   @Override
   public void enqueueAsyncWork(LuceneWork work) throws InterruptedException {
      defaultBatchBackend.enqueueAsyncWork(work);
   }

   @Override
   public void doWorkInSync(LuceneWork work) {
      defaultBatchBackend.doWorkInSync(work);
   }

   @Override
   public void flush(Set<Class<?>> indexedRootTypes) {
      performShardAwareOperation(indexedRootTypes, (im, type) -> {
         im.performStreamOperation(new FlushLuceneWork(null, type), progressMonitor, false);
      });
   }

   @Override
   public void optimize(Set<Class<?>> targetedClasses) {
      defaultBatchBackend.optimize(targetedClasses);
   }

   private void performShardAwareOperation(Set<Class<?>> entityTypes, BiConsumer<IndexManager, Class<?>> operation) {
      for (Class<?> type : entityTypes) {
         EntityIndexBinding indexBindingForEntity = integrator.getIndexBinding(type);
         if (indexBindingForEntity != null) {
            IndexManager[] indexManagers = indexBindingForEntity.getSelectionStrategy().getIndexManagersForDeletion(type, null, null);
            stream(indexManagers).forEach(im -> operation.accept(im, type));
         }
      }
   }
}
