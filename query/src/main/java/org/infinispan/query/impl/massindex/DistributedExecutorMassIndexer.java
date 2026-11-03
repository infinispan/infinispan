package org.infinispan.query.impl.massindex;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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

/**
 * @author gustavonalle
 * @since 7.1
 */
@MBean(objectName = "MassIndexer",
      description = "Component that rebuilds the Lucene index from the cached data")
@Scope(Scopes.NAMED_CACHE)
public class DistributedExecutorMassIndexer implements Indexer {

   private static final Log LOG = Log.getLog(DistributedExecutorMassIndexer.class);

   private final AdvancedCache<?, ?> cache;
   private final IndexUpdater indexUpdater;
   private final ClusterExecutor executor;
   private final IndexLock lock;
   private final Authorizer authorizer;
   private final LocalMassIndexer localIndexer;

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
      this.localIndexer = ComponentRegistry.componentOf(cache, LocalMassIndexer.class);
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
   public CompletionStage<Void> runLocal() {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      // Only rebuilds this member's index.
      // Local mass indexer already allows one rebuild per member, so a local run needs no cluster-wide lock.
      return submit(false, true).toCompletableFuture();
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
      return executeInternal(true).toCompletableFuture();
   }

   @Override
   public CompletionStage<Void> remove(Class<?>... entities) {
      return executeInternal(true, entities);
   }

   @Override
   public boolean isRunning() {
      return isRunning || localIndexer.isRunning();
   }

   /**
    * Rebuilds, or only removes, the index of the given entity types on this member only, or on every member of the
    * cluster.
    *
    * <p>
    * Each targeted member rebuilds its own index from the entries it holds. A member that is already rebuilding its
    * index fails its part with {@link MassIndexerAlreadyStartedException}, and the other targeted members still
    * rebuild theirs. This method doesn't take the cluster-wide lock. A caller that needs to exclude other
    * cluster-wide operations must hold that lock while the returned stage is pending.
    * </p>
    *
    * @param skipIndex {@code true} to only remove the index of the types, {@code false} to also index the entries
    *                  again.
    * @param local     {@code true} to target this member only, {@code false} to target every member of the cluster.
    * @param entities  the entity types whose index to rebuild. Empty means every indexed type of the cache.
    * @return a stage that completes once every targeted member has finished. It completes exceptionally with a
    * {@link CacheException} wrapping the cause if any targeted member fails, or with the cause itself if the
    * operation couldn't be sent. This method never throws.
    */
   private CompletionStage<Void> submit(boolean skipIndex, boolean local, Class<?> ... entities) {
      try {
         Collection<Class<?>> javaClasses = entities.length == 0
               ? indexUpdater.allJavaClasses()
               : Arrays.asList(entities);
         IndexWorker worker = new IndexWorker(cache.getName(), javaClasses, skipIndex, (Set<Object>) null);
         ClusterExecutor ce = executor.timeout(Long.MAX_VALUE, TimeUnit.SECONDS);
         if (local) {
            ce = ce.filterTargets(a -> Objects.equals(a, cache.getRpcManager().getAddress()));
         }
         return ce.submitConsumer(worker, TRI_CONSUMER);
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }

   private CompletionStage<Void> executeInternal(boolean skipIndex, Class<?>... entities) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      return lock.lock().thenCompose(acquired -> {
         if (!acquired)
            return CompletableFuture.failedFuture(new MassIndexerAlreadyStartedException());

         isRunning = true;
         return submit(skipIndex, false, entities).handle((v, t) -> {
            isRunning = false;
            CompletionStage<Void> unlocked = lock.unlock();
            if (t != null) {
               return unlocked.thenCompose(unused -> CompletableFuture.<Void>failedFuture(t));
            }
            return unlocked;
         }).thenCompose(Function.identity());
      });
   }
}
