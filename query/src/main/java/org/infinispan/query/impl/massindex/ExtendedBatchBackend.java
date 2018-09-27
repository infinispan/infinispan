package org.infinispan.query.impl.massindex;

import java.util.Set;
import java.util.function.BiConsumer;

import org.hibernate.search.backend.FlushLuceneWork;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.PurgeAllLuceneWork;
import org.hibernate.search.backend.impl.batch.DefaultBatchBackend;
import org.hibernate.search.backend.spi.BatchBackend;
import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.engine.integration.impl.ExtendedSearchIntegrator;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.IndexedTypeSet;
import org.hibernate.search.spi.SearchIntegrator;

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
      this.defaultBatchBackend = new DefaultBatchBackend(integrator.unwrap(ExtendedSearchIntegrator.class), progressMonitor);
   }

   public void purge(IndexedTypeSet entityTypes) {
      performShardAwareOperation(entityTypes, (im, type) -> im.performStreamOperation(new PurgeAllLuceneWork(type), progressMonitor, false));
      flush(entityTypes);
   }

   @Override
   public void enqueueAsyncWork(LuceneWork work) throws InterruptedException {
      defaultBatchBackend.enqueueAsyncWork(work);
   }

   @Override
   public void awaitAsyncProcessingCompletion() {
      defaultBatchBackend.awaitAsyncProcessingCompletion();
   }

   @Override
   public void doWorkInSync(LuceneWork work) {
      defaultBatchBackend.doWorkInSync(work);
   }

   @Override
   public void flush(IndexedTypeSet entityTypes) {
      performShardAwareOperation(entityTypes, (im, type) -> im.performStreamOperation(new FlushLuceneWork(null, type), progressMonitor, false));
   }

   @Override
   public void optimize(IndexedTypeSet entityTypes) {
      defaultBatchBackend.optimize(entityTypes);
   }

   private void performShardAwareOperation(IndexedTypeSet entityTypes, BiConsumer<IndexManager, IndexedTypeIdentifier> operation) {
      for (IndexedTypeIdentifier type : entityTypes) {
         EntityIndexBinding indexBindingForEntity = integrator.getIndexBinding(type);
         if (indexBindingForEntity != null) {
            Set<IndexManager> indexManagers = indexBindingForEntity.getIndexManagerSelector().forExisting(type, null, null);
            indexManagers.forEach(im -> operation.accept(im, type));
         }
      }
   }
}
