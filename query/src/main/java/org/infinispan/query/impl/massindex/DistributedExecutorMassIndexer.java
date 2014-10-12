package org.infinispan.query.impl.massindex;

import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author gustavonalle
 * @since 7.1
 */
public class DistributedExecutorMassIndexer implements MassIndexer {

   private static final Log LOG = LogFactory.getLog(DistributedExecutorMassIndexer.class, Log.class);

   private final AdvancedCache cache;
   private final SearchIntegrator searchIntegrator;
   private final IndexUpdater indexUpdater;

   public DistributedExecutorMassIndexer(AdvancedCache cache, SearchIntegrator searchIntegrator) {
      this.cache = cache;
      this.searchIntegrator = searchIntegrator;
      this.indexUpdater = new IndexUpdater(cache);
   }

   @Override
   @SuppressWarnings("unchecked")
   public void start() {
      DistributedExecutorService executor = new DefaultExecutorService(cache);
      ArrayList<Future<Void>> futures = new ArrayList<>();
      Deque<Class<?>> toFlush = new LinkedList<>();
      boolean replicated = cache.getAdvancedCache().getCacheConfiguration().clustering().cacheMode().isReplicated();

      for (Class<?> indexedType : searchIntegrator.getIndexedTypes()) {
         EntityIndexBinding indexBinding = searchIntegrator.getIndexBinding(indexedType);
         IndexManager[] indexManagers = indexBinding.getIndexManagers();
         for (IndexManager indexManager : indexManagers) {
            String indexName = indexManager.getIndexName();
            IndexWorker indexWork;
            boolean shared = isShared(indexManager);
            if (shared) {
               indexUpdater.purge(indexedType);
               indexWork = new IndexWorker(indexedType, indexName, false);
               toFlush.add(indexedType);
            } else {
               indexWork = new IndexWorker(indexedType, indexName, true);
            }
            DistributedTaskBuilder builder = executor.createDistributedTaskBuilder(indexWork).timeout(0, TimeUnit.NANOSECONDS);
            DistributedTask task = builder.build();
            if (replicated && shared) {
               futures.add(executor.submit(task));
            } else {
               futures.addAll(executor.submitEverywhere(task));
            }
         }
      }
      waitForAll(futures);

      for (Class<?> type : toFlush) {
         indexUpdater.flush(type);
      }
   }

   private void waitForAll(ArrayList<Future<Void>> futures) {
      for (Future f : futures) {
         try {
            f.get();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (ExecutionException e) {
            LOG.errorExecutingMassIndexer(e);
         }
      }
   }

   private boolean isShared(IndexManager indexManager) {
      return indexManager instanceof InfinispanIndexManager;
   }

}
