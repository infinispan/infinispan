package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.getDiscardForCache;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "statetransfer.CoordinatorCrashTest")
@CleanupAfterTest
public class CoordinatorCrashTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "crash-test";
   private static final int NR_KEYS = 32;

   public void testCrashDuringRebalance(Method method) throws Exception {
      doTest(method, CacheTopologyControlCommand.Type.REBALANCE_START);
   }

   public void testCrashDuringReadCHUpdate(Method method) throws Exception {
      doTest(method, CacheTopologyControlCommand.Type.READ_CH_UPDATE);
   }

   public void testCrashDuringWriteCHUpdate(Method method) throws Exception {
      doTest(method, CacheTopologyControlCommand.Type.WRITE_CH_UPDATE);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      addNode();
      addNode();
      waitForClusterToForm();
   }

   private void doTest(Method method, CacheTopologyControlCommand.Type discardType) throws Exception {
      waitForClusterToForm(CACHE_NAME);
      insertSomeData(method);

      final CacheContainer newContainer = addNode();
      DiscardPolicy policy = discardOn(newContainer, discardType);

      final int coordinatorIndex = findCoordinatorIndex();

      Future<Void> join = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            newContainer.getCache(CACHE_NAME);
            waitForClusterToForm(CACHE_NAME);
            return null;
         }
      });

      policy.awaitDiscarded(30, TimeUnit.SECONDS);
      crash(manager(coordinatorIndex));

      Future<Void> leave = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            killCacheManagers(manager(coordinatorIndex));
            cacheManagers.remove(coordinatorIndex);
            return null;
         }
      });

      join.get(30, TimeUnit.SECONDS);
      leave.get(30, TimeUnit.SECONDS);

      assertData(method);
   }

   private int findCoordinatorIndex() {
      for (int i = 0; i < cacheManagers.size(); ++i) {
         if (extractGlobalComponent(cacheManagers.get(i), Transport.class).isCoordinator()) {
            return i;
         }
      }
      throw new IllegalStateException("Coordinator not found!");
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

   private static DiscardPolicy discardOn(CacheContainer container, CacheTopologyControlCommand.Type type) {
      DiscardInboundInvocationHandler handler = (DiscardInboundInvocationHandler) extractGlobalComponent(container, InboundInvocationHandler.class);
      return handler.discard(type);
   }

   private static void crash(CacheContainer container) throws Exception {
      getDiscardForCache(container).setDiscardAll(true);
   }

   private CacheContainer addNode() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(builder, new TransportFlags()
            .withFD(true).withMerge(true));
      wrapGlobalComponent(cacheManager, InboundInvocationHandler.class, new TestingUtil.WrapFactory<InboundInvocationHandler, InboundInvocationHandler, CacheContainer>() {
         @Override
         public InboundInvocationHandler wrap(CacheContainer wrapOn, InboundInvocationHandler current) {
            return new DiscardInboundInvocationHandler(current);
         }
      }, true);
      return cacheManager;
   }

   private static class DiscardInboundInvocationHandler implements InboundInvocationHandler {

      private final InboundInvocationHandler delegate;
      private volatile DiscardPolicy policy;

      private DiscardInboundInvocationHandler(InboundInvocationHandler delegate) {
         this.delegate = delegate;
      }

      @Override
      public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof CacheTopologyControlCommand && CACHE_NAME.equals(((CacheTopologyControlCommand) command).getCacheName())) {
            final DiscardPolicy discardPolicy = policy;
            if (discardPolicy != null && discardPolicy.discard(((CacheTopologyControlCommand) command).getType())) {
               return;
            }
         }
         delegate.handleFromCluster(origin, command, reply, order);
      }

      @Override
      public void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply, DeliverOrder order) {
         delegate.handleFromRemoteSite(origin, command, reply, order);
      }

      public DiscardPolicy discard(CacheTopologyControlCommand.Type type) {
         if (type == null) {
            this.policy = null;
            return null;
         }
         final DiscardPolicy discardPolicy = new DiscardPolicy(type);
         this.policy = discardPolicy;
         return discardPolicy;
      }
   }

   private static class DiscardPolicy {
      private final CacheTopologyControlCommand.Type type;
      private final CountDownLatch countDownLatch;

      public DiscardPolicy(CacheTopologyControlCommand.Type type) {
         this.type = type;
         this.countDownLatch = new CountDownLatch(1);
      }

      public synchronized boolean discard(CacheTopologyControlCommand.Type type) {
         if (this.type == type && countDownLatch.getCount() > 0) {
            countDownLatch.countDown();
            return true;
         }
         return false;
      }

      public void awaitDiscarded(long timeout, TimeUnit timeUnit) throws InterruptedException {
         this.countDownLatch.await(timeout, timeUnit);
      }
   }
}
