package org.infinispan.query.impl.massindex;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.query.Indexer;
import org.infinispan.query.core.impl.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.Authorizer;
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
   private final IndexLock lock;
   private final Authorizer authorizer;

   private volatile boolean isRunning = false;

   private static final TriConsumer<Address, Void, Throwable> TRI_CONSUMER = (a, v, t) -> {
      if (t != null) {
         throw new CacheException(t);
      }
   };

   public DistributedExecutorMassIndexer(AdvancedCache<?, ?> cache) {
      this.cache = cache;
      this.indexUpdater = new IndexUpdater(cache);
      this.executor = cache.getCacheManager().executor();
      this.lock = MassIndexerLockFactory.buildLock(cache);
      this.authorizer = ComponentRegistry.componentOf(cache, Authorizer.class);
   }

   @ManagedOperation(description = "Starts rebuilding the index", displayName = "Rebuild index")
   public void start() {
      CompletionStages.join(executeInternal(false, false));
   }

   @Override
   public CompletionStage<Void> run() {
      return executeInternal(false, false).toCompletableFuture();
   }

   @Override
   public CompletionStage<Void> runLocal() {
      return executeInternal(false, true).toCompletableFuture();
   }

   @Override
   public CompletionStage<Void> run(Object... keys) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
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
      if (!keySet.isEmpty()) {
         IndexWorker indexWorker = new IndexWorker(cache.getName(), null, false, keySet);
         future = executor.submitConsumer(indexWorker, triConsumer);
      }
      return future != null ? future : CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> remove() {
      return executeInternal(true, false).toCompletableFuture();
   }

   @Override
   public CompletionStage<Void> remove(Class<?>... entities) {
      return executeInternal(true, false, entities);
   }

   @Override
   public boolean isRunning() {
      return isRunning;
   }

   private CompletionStage<Void> executeInternal(boolean skipIndex, boolean local, Class<?>... entities) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      CompletionStage<Boolean> lockStage = lock.lock();
      return lockStage.thenCompose(acquired -> {
         if (!acquired) {
            return CompletableFuture.failedFuture(new MassIndexerAlreadyStartedException());
         }
         try {
            isRunning = true;
            Collection<Class<?>> javaClasses = (entities.length == 0) ?
                  indexUpdater.allJavaClasses() : Arrays.asList(entities);
            IndexWorker indexWork = new IndexWorker(cache.getName(), javaClasses, skipIndex, (Set<Object>) null);
            ClusterExecutor clusterExecutor = executor.timeout(Long.MAX_VALUE, TimeUnit.SECONDS);
            if (local) {
               clusterExecutor = clusterExecutor.filterTargets(a -> a.equals(cache.getRpcManager().getAddress()));
            }
            return clusterExecutor.timeout(Long.MAX_VALUE, TimeUnit.SECONDS).submitConsumer(indexWork, TRI_CONSUMER);
         } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
         } finally {
            lock.unlock();
            isRunning = false;
         }
      });
   }
}
