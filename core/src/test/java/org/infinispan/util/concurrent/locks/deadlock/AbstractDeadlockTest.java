package org.infinispan.util.concurrent.locks.deadlock;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;

public abstract class AbstractDeadlockTest extends MultipleCacheManagersTest {

   private final DeadlockClusterHandler.TestLeech leech = new DeadlockClusterHandler.TestLeech() {
      @Override
      public <K, V> Cache<K, V> cache(int node) {
         return AbstractDeadlockTest.this.cache(node);
      }

      @Override
      public Address address(int node) {
         return AbstractDeadlockTest.this.manager(node).getAddress();
      }

      @Override
      public <T> Future<T> fork(Callable<T> callable) {
         return AbstractDeadlockTest.this.fork(callable);
      }

      @Override
      public String keyGenerator(String prefix, int node) {
         return AbstractDeadlockTest.this.getStringKeyForCache(prefix, cache(node));
      }

      @Override
      public void eventually(DeadlockClusterHandler.ThrowingBooleanSupplier bs) {
         AbstractDeadlockTest.eventually(bs::getAsBoolean);
      }
   };

   protected abstract int clusterSize();

   protected abstract ConfigurationBuilder cacheConfiguration();

   @Override
   protected final void createCacheManagers() throws Throwable {
      createClusteredCaches(clusterSize(), testCacheConfiguration());
   }

   protected final ConfigurationBuilder testCacheConfiguration() {
      ConfigurationBuilder builder = cacheConfiguration();
      builder.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .deadlockDetection(true);
      builder.clustering().hash().numOwners(2);
      return builder;
   }

   protected final DeadlockClusterHandler createDeadlock(int ... nodes) throws Throwable {
      return DeadlockClusterHandler.create(leech, nodes);
   }

   protected final DeadlockClusterHandler createDeadlock(Map<Integer, Integer> dependency, int ... nodes) throws Throwable {
      return DeadlockClusterHandler.create(leech, dependency, nodes);
   }
}
