package org.infinispan.query.impl.massindex;

import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.query.Indexer;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
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
public class DistributedExecutorMassIndexer implements MassIndexer, Indexer {

   private static final Log LOG = LogFactory.getLog(DistributedExecutorMassIndexer.class, Log.class);

   private final AdvancedCache<?, ?> cache;
   private final SearchIntegrator searchIntegrator;
   private final IndexUpdater indexUpdater;
   private final ClusterExecutor executor;
   private final BlockingManager blockingManager;
   private final IndexLock lock;
   private final AuthorizationHelper authorizationHelper;

   private volatile boolean isRunning = false;

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
      this.blockingManager = cache.getCacheManager().getGlobalComponentRegistry()
            .getComponent(BlockingManager.class);
      this.lock = MassIndexerLockFactory.buildLock(cache);
      this.authorizationHelper = cache.getComponentRegistry().getComponent(AuthorizationHelper.class);
   }

   @ManagedOperation(description = "Starts rebuilding the index", displayName = "Rebuild index")
   @Override
   public void start() {
      CompletionStages.join(executeInternal(false));
   }

   @Override
   public CompletableFuture<Void> purge() {
      return executeInternal(true).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Void> startAsync() {
      return executeInternal(false).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Void> reindex(Object... keys) {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
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
   public CompletionStage<Void> run() {
      return startAsync();
   }

   @Override
   public CompletionStage<Void> run(Object... keys) {
      return reindex(keys);
   }

   @Override
   public CompletionStage<Void> remove() {
      return purge();
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
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      CompletionStage<Boolean> lockStage = lock.lock();
      return lockStage.thenCompose(acquired -> {
         if (!acquired) {
            return CompletableFutures.completedExceptionFuture(new MassIndexerAlreadyStartedException());
         }
         isRunning = true;
         Deque<IndexedTypeIdentifier> toFlush = new LinkedList<>();

         BiConsumer<Void, Throwable> flushIfNeeded = (v, t) -> {
            try {
               indexUpdater.flush(toFlush);
            } finally {
               CompletionStages.join(lock.unlock());
               isRunning = false;
            }
         };
         Set<IndexedTypeIdentifier> indexedTypes;
         if (entities.length > 0) {
            indexedTypes = Arrays.stream(entities)
                  .map(PojoIndexedTypeIdentifier::convertFromLegacy)
                  .collect(Collectors.toSet());
         } else {
            indexedTypes = new HashSet<>();
            for (IndexedTypeIdentifier indexedType : searchIntegrator.getIndexBindings().keySet()) {
               indexedTypes.add(indexedType);
            }
         }
         try {
            IndexWorker indexWork = new IndexWorker(cache.getName(), indexedTypes, skipIndex, null);
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
