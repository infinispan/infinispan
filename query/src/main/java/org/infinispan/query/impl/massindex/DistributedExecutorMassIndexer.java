package org.infinispan.query.impl.massindex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.query.Indexer;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;
import org.infinispan.util.concurrent.BlockingManager;
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
public class DistributedExecutorMassIndexer implements Indexer {

   private static final Log LOG = LogFactory.getLog(DistributedExecutorMassIndexer.class, Log.class);

   private final AdvancedCache<?, ?> cache;
   private final IndexUpdater indexUpdater;
   private final ClusterExecutor executor;
   private final BlockingManager blockingManager;
   private final IndexLock lock;

   private volatile boolean isRunning = false;

   private static final TriConsumer<Address, Void, Throwable> TRI_CONSUMER = (a, v, t) -> {
      if (t != null) {
         throw new CacheException(t);
      }
   };

   public DistributedExecutorMassIndexer(AdvancedCache<?, ?> cache, SearchMappingHolder searchMappingHolder,
                                         KeyTransformationHandler keyTransformationHandler) {
      this.cache = cache;
      this.indexUpdater = new IndexUpdater(searchMappingHolder, keyTransformationHandler);
      this.executor = cache.getCacheManager().executor();
      this.blockingManager = cache.getCacheManager().getGlobalComponentRegistry()
            .getComponent(BlockingManager.class);
      this.lock = MassIndexerLockFactory.buildLock(cache);
   }

   @ManagedOperation(description = "Starts rebuilding the index", displayName = "Rebuild index")
   public void start() {
      CompletionStages.join(executeInternal(false));
   }

   @Override
   public CompletionStage<Void> run() {
      return executeInternal(false).toCompletableFuture();
   }

   @Override
   public CompletionStage<Void> run(Object... keys) {
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
   public CompletionStage<Void> remove() {
      return executeInternal(true).toCompletableFuture();
   }

   @Override
   public CompletionStage<Void> remove(Class<?>... entities) {
      return executeInternal(true, entities);
   }

   @Override
   public boolean isRunning() {
      return isRunning;
   }

   private CompletionStage<Void> executeInternal(boolean skipIndex, Class<?>... entities) {
      CompletionStage<Boolean> lockStage = lock.lock();
      return lockStage.thenCompose(acquired -> {
         if (!acquired) {
            return CompletableFutures.completedExceptionFuture(new MassIndexerAlreadyStartedException());
         }
         isRunning = true;
         Collection<Class<?>> javaClasses = (entities.length == 0) ?
               indexUpdater.allJavaClasses() : Arrays.asList(entities);
         Deque<Class<?>> toFlush = new LinkedList<>(javaClasses);

         BiConsumer<Void, Throwable> flushIfNeeded = (v, t) -> {
            try {
               indexUpdater.flush(toFlush);
               indexUpdater.refresh(toFlush);
            } finally {
               CompletionStages.join(lock.unlock());
               isRunning = false;
            }
         };
         try {
            IndexWorker indexWork = new IndexWorker(cache.getName(), javaClasses, skipIndex, null);
            CompletableFuture<Void> future = executor.timeout(Long.MAX_VALUE, TimeUnit.SECONDS).submitConsumer(indexWork, TRI_CONSUMER);
            return blockingManager.whenCompleteBlocking(future, flushIfNeeded, this);
         } catch (Throwable t) {
            lock.unlock();
            isRunning = false;
            return CompletableFutures.completedExceptionFuture(t);
         }
      });
   }
}
