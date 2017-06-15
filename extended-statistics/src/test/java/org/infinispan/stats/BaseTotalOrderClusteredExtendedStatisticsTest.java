package org.infinispan.stats;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

/**
 * Implements the await methods for the total order caches (transactional and non-transactional)
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@Test(groups = "functional", testName = "stats.BaseTotalOrderClusteredExtendedStatisticsTest")
public abstract class BaseTotalOrderClusteredExtendedStatisticsTest extends BaseClusteredExtendedStatisticTest {

   protected BaseTotalOrderClusteredExtendedStatisticsTest(CacheMode mode) {
      super(mode, true);
   }

   @Override
   protected void awaitPut(int cacheIndex, Object key) throws InterruptedException {
      awaitSingleKeyOperation(Operation.PUT, cacheIndex, key);
   }

   @Override
   protected void awaitReplace(int cacheIndex, Object key) throws InterruptedException {
      awaitSingleKeyOperation(Operation.REPLACE, cacheIndex, key);
   }

   @Override
   protected void awaitRemove(int cacheIndex, Object key) throws InterruptedException {
      awaitSingleKeyOperation(Operation.REMOVE, cacheIndex, key);
   }

   @Override
   protected void awaitCompute(int cacheIndex, Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction)
         throws InterruptedException {
      awaitSingleKeyOperation(Operation.COMPUTE, cacheIndex, key);
   }

   @Override
   protected void awaitComputeIfAbsent(int cacheIndex, Object key, Function<? super Object, ?> computeFunction) throws InterruptedException {
      awaitSingleKeyOperation(Operation.COMPUTE_IF_ABSENT, cacheIndex, key);
   }

   @Override
   protected void awaitComputeIfPresent(int cacheIndex, Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) throws InterruptedException {
      awaitSingleKeyOperation(Operation.COMPUTE, cacheIndex, key);
   }

   @Override
   protected void awaitPutMap(int cacheIndex, Collection<Object> keys) throws InterruptedException {
      Cache<?, ?> executedOn = cache(cacheIndex);
      Collection<Address> owners = getOwners(executedOn, keys);
      owners.add(address(executedOn));
      awaitOperation(Operation.PUT_MAP, owners);
   }

   private void awaitSingleKeyOperation(Operation operation, int cacheIndex, Object key) throws InterruptedException {
      Cache<?, ?> executedOn = cache(cacheIndex);
      Collection<Address> owners = getOwners(executedOn, key);
      owners.add(address(executedOn));
      awaitOperation(operation, owners);
   }

}
