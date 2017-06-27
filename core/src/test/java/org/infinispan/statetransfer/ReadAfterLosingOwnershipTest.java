package org.infinispan.statetransfer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests the read when a node loses the ownership of a key.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.ReadAfterLosingOwnershipTest")
@CleanupAfterMethod
public class ReadAfterLosingOwnershipTest extends MultipleCacheManagersTest {
   private boolean l1 = false;

   @Override
   public Object[] factory() {
      return new Object[] {
         new ReadAfterLosingOwnershipTest().transactional(true),
         new ReadAfterLosingOwnershipTest().transactional(false),
         new ReadAfterLosingOwnershipTest().l1(true).transactional(true),
         new ReadAfterLosingOwnershipTest().l1(true).transactional(false),
      };
   }

   public ReadAfterLosingOwnershipTest l1(boolean l1) {
      this.l1 = l1;
      return this;
   }

   @Override
   protected String parameters() {
      return "[tx=" + transactional + ", l1=" + l1 + "]";
   }

   public void testOwnershipLostWithPut() throws Exception {
      doOwnershipLostTest(Operation.PUT, false);
   }

   public void testOwnershipLostWithRemove() throws Exception {
      doOwnershipLostTest(Operation.REMOVE, false);
   }

   public void testOwnershipLostWithPutOnOwner() throws Exception {
      doOwnershipLostTest(Operation.PUT, true);
   }

   public void testOwnershipLostWithRemoveOnOwner() throws Exception {
      doOwnershipLostTest(Operation.REMOVE, true);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, createConfigurationBuilder());
   }

   protected final ConfigurationBuilder createConfigurationBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, transactional);
      builder.clustering()
            .hash().numOwners(2).consistentHashFactory(new SingleKeyConsistentHashFactory()).numSegments(1)
            .l1().enabled(l1)
            .stateTransfer().fetchInMemoryState(true);
      return builder;
   }

   private void doOwnershipLostTest(Operation operation, boolean onOwner) throws ExecutionException, InterruptedException {
      log.debug("Initialize cache");
      cache(0).put("key", "value0");
      assertCachesKeyValue("key", "value0");

      StateConsumerImpl stateConsumer = (StateConsumerImpl) TestingUtil.extractComponent(cache(1), StateConsumer.class);
      Listener listener = new Listener();
      stateConsumer.setKeyInvalidationListener(listener);

      log.debug("Add a 3rd node");
      addClusterEnabledCacheManager(createConfigurationBuilder());
      Future<Void> join = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            waitForClusterToForm();
            log.debug("3rd has join");
            return null;
         }
      });

      log.debug("Waiting for command to block");
      listener.notifier.await();

      log.debug("Set a new value");
      //we change the value in the old owner if onOwner is false
      operation.update(onOwner ? cache(0) : cache(1));

      //we check the value in the primary owner and old owner (cache(2) has not started yet)
      assertCachesKeyValue("key", operation.finalValue(), cache(0), cache(1));


      listener.wait.countDown();

      log.debug("Waiting for the 3rd node to join");
      join.get();

      assertCachesKeyValue("key", operation.finalValue());
   }

   private void assertCachesKeyValue(Object key, Object value) {
      assertCachesKeyValue(key, value, caches());
   }

   private void assertCachesKeyValue(Object key, Object value, Cache<Object, Object>... caches) {
      assertCachesKeyValue(key, value, Arrays.asList(caches));
   }

   private void assertCachesKeyValue(Object key, Object value, Collection<Cache<Object, Object>> caches) {
      for (Cache<Object, Object> cache : caches) {
         AssertJUnit.assertEquals("Wrong key value for " + address(cache), value, cache.get(key));
      }
   }

   private enum Operation {
      //only PUT and REMOVE is needed because one updates the key (i.e. the value is not null) and the other removes
      //it (i.e. the value is null)
      PUT,
      REMOVE;

      public void update(Cache<Object, Object> cache) {
         if (this == PUT) {
            cache.put("key", "value1");
         } else {
            cache.remove("key");
         }
      }

      public Object finalValue() {
         return this == PUT ? "value1" : null;
      }
   }

   public static class SingleKeyConsistentHashFactory extends BaseControlledConsistentHashFactory.Default {

      public SingleKeyConsistentHashFactory() {
         super(1);
      }

      protected final List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
         //the owners will be the first member and the last (numberOfOwners - 1)-th members
         List<Address> owners = new ArrayList<Address>(numberOfOwners);
         owners.add(members.get(0));
         for (int i = members.size() - 1; i > 0; --i) {
            if (owners.size() >= numberOfOwners) {
               break;
            }
            owners.add(members.get(i));
         }
         return owners;
      }
   }

   public class Listener implements StateConsumerImpl.KeyInvalidationListener {

      public final CountDownLatch notifier = new CountDownLatch(1);
      final CountDownLatch wait = new CountDownLatch(1);

      @Override
      public void beforeInvalidation(Set<Integer> removedSegments, Set<Integer> staleL1Segments) {
         log.debugf("Before invalidation: removedSegments=%s, staleL1Segments=%s", removedSegments, staleL1Segments);
         if (!removedSegments.contains(0)) {
            //it only matters when it looses the segment 0 and the key is moved to the new owner
            return;
         }
         notifier.countDown();
         try {
            wait.await();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }
}
