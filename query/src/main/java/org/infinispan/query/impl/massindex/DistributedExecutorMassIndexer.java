package org.infinispan.query.impl.massindex;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.IndexInspector;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.LogFactory;

/**
 * @author gustavonalle
 * @since 7.1
 */
@MBean(objectName = "MassIndexer",
      description = "Component that rebuilds the Lucene index from the cached data")
@Scope(Scopes.NAMED_CACHE)
public class DistributedExecutorMassIndexer implements MassIndexer {

   private static final Log LOG = LogFactory.getLog(DistributedExecutorMassIndexer.class, Log.class);

   private final AdvancedCache<?, ?> cache;
   private final SearchIntegrator searchIntegrator;
   private final IndexUpdater indexUpdater;
   private final ClusterExecutor executor;
   private final ExecutorService localExecutor;
   private final MassIndexLock lock;

   @Inject
   IndexInspector indexInspector;

   private static final TriConsumer<Address, Void, Throwable> TRI_CONSUMER = (a, v, t) -> {
      if (t != null) {
         throw new CacheException(t);
      }
   };

   public DistributedExecutorMassIndexer(AdvancedCache<?, ?> cache, SearchIntegrator searchIntegrator,
                                         KeyTransformationHandler keyTransformationHandler, TimeService timeService) {
      this.cache = cache;
      this.searchIntegrator = searchIntegrator;
      this.indexUpdater = new IndexUpdater(searchIntegrator, keyTransformationHandler, timeService);
      this.executor = cache.getCacheManager().executor();
      this.localExecutor = cache.getCacheManager().getGlobalComponentRegistry()
            .getComponent(ExecutorService.class, KnownComponentNames.BLOCKING_EXECUTOR);
      this.lock = MassIndexerLockFactory.buildLock(cache);
   }

   @ManagedOperation(description = "Starts rebuilding the index", displayName = "Rebuild index")
   @Override
   public void start() {
      CompletionStages.join(executeInternal(false));
   }

   @Override
   public CompletableFuture<Void> purge() {
      return executeInternal(true);
   }

   @Override
   public CompletableFuture<Void> startAsync() {
      return executeInternal(false);
   }

   @Override
   public CompletableFuture<Void> reindex(Object... keys) {
      CompletableFuture<Void> future = null;
      Set<Object> keySet = new HashSet<>();
      for (Object key : keys) {
         if (cache.containsKey(key)) {
            keySet.add(key);
         } else {
            LOG.warn("cache contains no mapping for the key");
         }
      }
      TriConsumer<Address, Void, Throwable> triConsumer = (a, v, t) -> {
         if (t != null) {
            throw new CacheException(t);
         }
      };
      if (keySet.size() > 0) {
         IndexWorker indexWorker = new IndexWorker(cache.getName(), null, false, keySet);
         future = executor.submitConsumer(indexWorker, triConsumer);
      }
      return future != null ? future : CompletableFutures.completedNull();
   }

   @Override
   public boolean isRunning() {
      return lock.isAcquired();
   }

   private CompletableFuture<Void> executeInternal(boolean skipIndex) {
      if (lock.lock()) {
         List<CompletableFuture<Void>> futures = new ArrayList<>();
         Deque<IndexedTypeIdentifier> toFlush = new LinkedList<>();

         BiConsumer<Void, Throwable> flushIfNeeded = (v, t) -> {
            try {
               indexUpdater.flush(toFlush);
            } finally {
               lock.unlock();
            }
         };
         Set<IndexedTypeIdentifier> indexedTypes = new HashSet<>();
         for (IndexedTypeIdentifier indexedType : searchIntegrator.getIndexBindings().keySet()) {
            indexedTypes.add(indexedType);
         }
         try {
            IndexWorker indexWork = new IndexWorker(cache.getName(), indexedTypes, skipIndex, null);
            futures.add(executor.submitConsumer(indexWork, TRI_CONSUMER));
            CompletableFuture<Void> compositeFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            return compositeFuture.whenCompleteAsync(flushIfNeeded, localExecutor);
         } catch (Throwable t) {
            lock.unlock();
            return CompletableFutures.completedExceptionFuture(t);
         }
      } else {
         return CompletableFutures.completedExceptionFuture(new MassIndexerAlreadyStartedException());
      }
   }
}
