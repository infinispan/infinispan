package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "statetransfer.NodeLeavingTest")
public class NodeLeavingTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "test-cache";
   private static final int NR_KEYS = 32;

   public void testNodeLeaving(Method method) throws InterruptedException, ExecutionException, TimeoutException {
      waitForClusterToForm(CACHE_NAME);
      insertSomeData(method);

      final CacheContainer container = addClusterEnabledCacheManager(getConfiguration());
      final ControllerTransport transport = wrapGlobalComponent(container, Transport.class, new TestingUtil.WrapFactory<Transport, ControllerTransport, CacheContainer>() {
         @Override
         public ControllerTransport wrap(CacheContainer wrapOn, Transport current) {
            return new ControllerTransport(current);
         }
      }, true);

      final BlockPolicy blockPolicy = new BlockPolicy(address(1));
      transport.startBlocking(blockPolicy);

      Future<Void> join = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            container.getCache(CACHE_NAME);
            return null;
         }
      });

      blockPolicy.awaitUntilBlock(30, TimeUnit.SECONDS);

      cache(1, CACHE_NAME).stop();

      blockPolicy.stopBlock();
      join.get(30, TimeUnit.SECONDS);
      waitForRehashToComplete(cache(0, CACHE_NAME), cache(2, CACHE_NAME));

      assertData(method);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getConfiguration(), 3);
   }

   private static ConfigurationBuilder getConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.clustering().hash().numOwners(2);
      return builder;
   }

   private void insertSomeData(Method method) {
      for (int i = 0; i < NR_KEYS; ++i) {
         cache(0, CACHE_NAME).put(k(method, i), v(method, i));
      }
   }


   private void assertData(Method method) {
      for (Cache<String, String> cache : this.<String, String>caches(CACHE_NAME)) {
         for (int i = 0; i < NR_KEYS; ++i) {
            AssertJUnit.assertEquals(v(method, i), cache.get(k(method, i)));
         }
      }
   }

   private static class ControllerTransport extends AbstractDelegatingTransport {

      private volatile BlockPolicy blockPolicy;

      public ControllerTransport(Transport actual) {
         super(actual);
      }

      public void startBlocking(BlockPolicy policy) {
         this.blockPolicy = policy;
      }

      @Override
      protected void beforeInvokeRemotely(ReplicableCommand command, Collection<Address> recipients) {
         final BlockPolicy policy = blockPolicy;
         if (policy != null && command instanceof StateRequestCommand) {
            try {
               policy.block((StateRequestCommand) command, recipients.iterator().next());
            } catch (InterruptedException e) {
               //reset flag
               Thread.currentThread().interrupt();
            }
         }
      }
   }

   private class BlockPolicy {
      private final Address destination;
      private final CountDownLatch notifierLatch;
      private final CountDownLatch blockingLatch;

      public BlockPolicy(Address destination) {
         this.destination = destination;
         blockingLatch = new CountDownLatch(1);
         notifierLatch = new CountDownLatch(1);
      }

      public void block(StateRequestCommand command, Address destination) throws InterruptedException {
         if (CACHE_NAME.equals(command.getCacheName()) &&
               command.getType() == StateRequestCommand.Type.GET_TRANSACTIONS &&
               this.destination.equals(destination)) {
            log.tracef("Blocking command %s", command);
            notifierLatch.countDown();
            blockingLatch.await();
            log.tracef("Unblocking command %s", command);
         }
      }

      public void awaitUntilBlock(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
         if (!notifierLatch.await(timeout, timeUnit)) {
            throw new TimeoutException();
         }
      }

      public void stopBlock() {
         blockingLatch.countDown();
      }
   }
}
