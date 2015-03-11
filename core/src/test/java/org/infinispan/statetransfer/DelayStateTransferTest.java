package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil.WrapFactory;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.String.format;
import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;

/**
 * Tests the read/write when a {@link org.infinispan.topology.CacheTopologyControlCommand} is delayed.
 * <p/>
 * Unfortunately, it is not possible to test the WRITE_CH_UPDATE since it would block all the write requests.
 *
 * @author Pedro Ruivo
 * @since 7.2
 */
@Test(groups = "functional", testName = "statetransfer.DelayStateTransferTest")
public class DelayStateTransferTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "test-cache";
   private final List<BlockingLocalTopologyManager> topologyManagers = new ArrayList<>(3);

   public void testDelayOnOldOwner() throws InterruptedException, ExecutionException {
      waitForRehashToComplete(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      cache(0, CACHE_NAME).put("k1", "v1");

      AssertJUnit.assertEquals("v1", cache(0, CACHE_NAME).get("k1"));
      AssertJUnit.assertEquals("v1", cache(1, CACHE_NAME).get("k1"));


      for (BlockingLocalTopologyManager topologyManager : topologyManagers) {
         init(topologyManager);
      }

      Future<Cache> newOwner = fork(new Callable<Cache>() {
         @Override
         public Cache call() throws Exception {
            return cache(2, CACHE_NAME);
         }
      });

      waitToBlock(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);

      topologyManagers.get(0).stopBlocking(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
      topologyManagers.get(2).stopBlocking(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
      newOwner.get();

      //need to skip the old owner because the put will block until the WRITE_CH_UPDATE is received
      putAndAssert("v2", true);

      topologyManagers.get(1).stopBlocking(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
      waitForRehashToComplete(caches(CACHE_NAME));

      putAndAssert("v3", false);

   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2).consistentHashFactory(new ControlledConsistentHashFactory()).numSegments(1);
      createCluster(builder, 3);
      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         topologyManagers.add(wrapGlobalComponent(cacheManager, LocalTopologyManager.class,
                                                  new WrapFactory<LocalTopologyManager, BlockingLocalTopologyManager, CacheContainer>() {
                                                     @Override
                                                     public BlockingLocalTopologyManager wrap(CacheContainer wrapOn, LocalTopologyManager current) {
                                                        return new BlockingLocalTopologyManager(current);
                                                     }
                                                  }, true));
      }
   }

   private void putAndAssert(String valuePrefix, boolean skipOldOwner) {
      cache(0, CACHE_NAME).put("k1", valuePrefix + "0");
      assertValue("k1", valuePrefix + "0");

      if (!skipOldOwner) {
         cache(1, CACHE_NAME).put("k1", valuePrefix + "1");
         assertValue("k1", valuePrefix + "1");
      }

      cache(2, CACHE_NAME).put("k1", valuePrefix + "2");
      assertValue("k1", valuePrefix + "2");

   }

   private void assertValue(String key, String value) {
      for (Cache<String, String> cache : this.<String, String>caches(CACHE_NAME)) {
         AssertJUnit.assertEquals(format("Wrong value for key '%s' on cache '%s'", key, address(cache)), value, cache.get(key));
      }
   }

   private void init(BlockingLocalTopologyManager topologyManager) {
      topologyManager.startBlocking(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
   }

   private void waitToBlock(BlockingLocalTopologyManager.LatchType type) throws InterruptedException {
      for (BlockingLocalTopologyManager topologyManager : topologyManagers) {
         topologyManager.waitToBlock(type);
      }
   }

   private static class ControlledConsistentHashFactory extends BaseControlledConsistentHashFactory {

      public ControlledConsistentHashFactory() {
         super(1);
      }

      @Override
      protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
         AssertJUnit.assertEquals(2, numberOfOwners);
         AssertJUnit.assertEquals(0, segmentIndex);
         return members.size() <= 2 ? members : Arrays.asList(members.get(0), members.get(2));
      }
   }
}
