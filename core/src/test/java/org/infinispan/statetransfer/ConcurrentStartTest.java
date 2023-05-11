package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.blockUntilViewsReceived;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.extractGlobalComponentRegistry;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.topology.CacheJoinCommand;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.testng.annotations.Test;

/**
 * Tests concurrent startup of replicated and distributed caches
 *
 * @author Dan Berindei
 * @since 7.2
 */
@Test(testName = "statetransfer.ConcurrentStartTest", groups = "functional")
public class ConcurrentStartTest extends MultipleCacheManagersTest {

   public static final String REPL_CACHE_NAME = "repl";
   public static final String DIST_CACHE_NAME = "dist";

   @Override
   protected void createCacheManagers() throws Throwable {
      // The test method will create the cache managers
   }

   @Test(timeOut = 60000)
   public void testConcurrentStart() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      final CheckPoint checkPoint = new CheckPoint();

      EmbeddedCacheManager cm1 = createCacheManager();
      EmbeddedCacheManager cm2 = createCacheManager();

      // Install the blocking invocation handlers
      assertEquals(ComponentStatus.INSTANTIATED, extractGlobalComponentRegistry(cm1).getStatus());
      replaceInboundInvocationHandler(cm1, checkPoint, 0);
      assertEquals(ComponentStatus.INSTANTIATED, extractGlobalComponentRegistry(cm2).getStatus());
      replaceInboundInvocationHandler(cm2, checkPoint, 1);

      log.debugf("Cache managers created. Starting the caches");
      Future<Object> repl1Future = fork(new CacheStartCallable(cm1, REPL_CACHE_NAME));
      Future<Object> repl2Future = fork(new CacheStartCallable(cm2, REPL_CACHE_NAME));
      Future<Object> dist1Future = fork(new CacheStartCallable(cm1, DIST_CACHE_NAME));
      Future<Object> dist2Future = fork(new CacheStartCallable(cm2, DIST_CACHE_NAME));

      // The joiner always sends a POLICY_GET_STATUS command to the coordinator.
      // The coordinator may or may not send a GET_STATUS command to the other node,
      // depending on whether the second node joined the cluster quickly enough.
      // Wait for at least one of the POLICY_GET_STATUS/GET_STATUS commands to block
      checkPoint.peek(2, SECONDS, "blocked_0", "blocked_1");
      // Now allow both to continue.
      checkPoint.trigger("unblocked_0", CheckPoint.INFINITE);
      checkPoint.trigger("unblocked_1", CheckPoint.INFINITE);

      repl1Future.get(10, SECONDS);
      repl2Future.get(10, SECONDS);
      dist1Future.get(10, SECONDS);
      dist2Future.get(10, SECONDS);

      Cache<String, String> c1r = cm1.getCache(REPL_CACHE_NAME);
      Cache<String, String> c1d = cm1.getCache(DIST_CACHE_NAME);
      Cache<String, String> c2r = cm2.getCache(REPL_CACHE_NAME);
      Cache<String, String> c2d = cm2.getCache(DIST_CACHE_NAME);

      blockUntilViewsReceived(10000, cm1, cm2);
      waitForNoRebalance(c1r, c2r);
      waitForNoRebalance(c1d, c2d);

      c1r.put("key", "value");
      assertEquals("value", c2r.get("key"));

      c1d.put("key", "value");
      assertEquals("value", c2d.get("key"));
   }

   private EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.transport().defaultTransport();
      TestCacheManagerFactory.amendGlobalConfiguration(gcb, new TransportFlags());
      ConfigurationBuilder defaultCacheConfig = new ConfigurationBuilder();
      EmbeddedCacheManager cm = TestCacheManagerFactory.newDefaultCacheManager(false, gcb, defaultCacheConfig);
      registerCacheManager(cm);

      Configuration replCfg = new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).build();
      cm.defineConfiguration(REPL_CACHE_NAME, replCfg);
      Configuration distCfg = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();
      cm.defineConfiguration(DIST_CACHE_NAME, distCfg);
      return cm;
   }

   private void replaceInboundInvocationHandler(EmbeddedCacheManager cm, CheckPoint checkPoint, int index) {
      InboundInvocationHandler handler = extractGlobalComponent(cm, InboundInvocationHandler.class);
      BlockingInboundInvocationHandler ourHandler =
            new BlockingInboundInvocationHandler(handler, checkPoint, index);
      replaceComponent(cm, InboundInvocationHandler.class, ourHandler, true);
   }

   private static class CacheStartCallable implements Callable<Object> {
      private final EmbeddedCacheManager cm;
      private final String cacheName;


      public CacheStartCallable(EmbeddedCacheManager cm, String cacheName) {
         this.cm = cm;
         this.cacheName = cacheName;
      }

      @Override
      public Object call() throws Exception {
         cm.getCache(cacheName);
         return null;
      }
   }

   private static class BlockingInboundInvocationHandler implements InboundInvocationHandler {
      private Log log = LogFactory.getLog(ConcurrentStartTest.class);
      private final CheckPoint checkPoint;
      private final InboundInvocationHandler delegate;
      private final int index;

      public BlockingInboundInvocationHandler(InboundInvocationHandler delegate, CheckPoint checkPoint,
            int index) {
         this.delegate = delegate;
         this.checkPoint = checkPoint;
         this.index = index;
      }

      @Override
      public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof CacheJoinCommand) {
            try {
               checkPoint.trigger("blocked_" + index);
               checkPoint.awaitStrict("unblocked_" + index, 10, SECONDS);
            } catch (Exception e) {
               log.warnf(e, "Error while blocking before command %s", command);
            }
         }
         delegate.handleFromCluster(origin, command, reply, order);
      }

      @Override
      public void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply, DeliverOrder order) {
         delegate.handleFromRemoteSite(origin, command, reply, order);
      }
   }
}
