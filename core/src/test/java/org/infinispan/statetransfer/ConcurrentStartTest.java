package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.JGroupsConfigBuilder;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.JChannel;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.blockUntilViewsReceived;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.extractGlobalComponentRegistry;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.testng.AssertJUnit.assertEquals;

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
      TestResourceTracker.testThreadStarted(this);
      final CheckPoint checkPoint = new CheckPoint();

      String name1 = TestResourceTracker.getNextNodeName();
      String name2 = TestResourceTracker.getNextNodeName();

      // Create and connect both channels beforehand
      // We need both nodes in the view when the coordinator's ClusterTopologyManagerImpl starts
      // in order to reproduce the ISPN-5106 deadlock
      JChannel ch1 = createChannel(name1, 0);
      JChannel ch2 = createChannel(name2, 1);

      // Use a JGroupsChannelLookup to pass the created channels to the transport
      EmbeddedCacheManager cm1 = createCacheManager(name1, ch1);
      EmbeddedCacheManager cm2 = createCacheManager(name2, ch2);

      // Install the blocking invocation handlers
      assertEquals(ComponentStatus.INSTANTIATED, extractGlobalComponentRegistry(cm1).getStatus());
      replaceInboundInvocationHandler(cm1, checkPoint);
      assertEquals(ComponentStatus.INSTANTIATED, extractGlobalComponentRegistry(cm2).getStatus());
      replaceInboundInvocationHandler(cm2, checkPoint);

      log.debugf("Cache managers created. Starting the caches");
      Future<Object> repl1Future = fork(new CacheStartCallable(cm1, "repl"));
      Future<Object> repl2Future = fork(new CacheStartCallable(cm2, "repl"));
      Future<Object> dist1Future = fork(new CacheStartCallable(cm1, "dist"));
      Future<Object> dist2Future = fork(new CacheStartCallable(cm2, "dist"));

      // Wait for the POLICY_GET_STATUS/GET_STATUS commands to block
      checkPoint.awaitStrict("blocked_" + ch1.getAddress(), 10, SECONDS);
      checkPoint.awaitStrict("blocked_" + ch2.getAddress(), 10, SECONDS);
      // And trigger the deadlock
      // StateTransferManagerImpl.waitForInitialStateTransferToComplete also uses POLICY_GET_STATUS
      checkPoint.trigger("unblocked_" + cm1.getAddress(), CheckPoint.INFINITE);
      checkPoint.trigger("unblocked_" + cm2.getAddress(), CheckPoint.INFINITE);

      repl1Future.get(10, SECONDS);
      repl2Future.get(10, SECONDS);
      dist1Future.get(10, SECONDS);
      dist2Future.get(10, SECONDS);

      Cache<String, String> c1r = cm1.getCache("repl");
      Cache<String, String> c1d = cm1.getCache("dist");
      Cache<String, String> c2r = cm2.getCache("repl");
      Cache<String, String> c2d = cm2.getCache("dist");

      blockUntilViewsReceived(10000, cm1, cm2);
      waitForRehashToComplete(c1r, c2r);
      waitForRehashToComplete(c1d, c2d);

      c1r.put("key", "value");
      assertEquals("value", c2r.get("key"));

      c1d.put("key", "value");
      assertEquals("value", c2d.get("key"));
   }

   private EmbeddedCacheManager createCacheManager(String name, JChannel channel) {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.transport().nodeName(name);
      gcb.globalJmxStatistics().allowDuplicateDomains(true);
      CustomChannelLookup.registerChannel(channel.getName(), channel, gcb, false);
      EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build(), false);
      Configuration replCfg = new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).build();
      cm.defineConfiguration(REPL_CACHE_NAME, replCfg);
      Configuration distCfg = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();
      cm.defineConfiguration(DIST_CACHE_NAME, distCfg);
      registerCacheManager(cm);
      return cm;
   }

   private JChannel createChannel(String name, int portRange) throws Exception {
      JChannel channel = new JChannel(JGroupsConfigBuilder
            .getJGroupsConfig(ConcurrentStartTest.class.getName(),
                  new TransportFlags().withPortRange(portRange)));
      channel.setName(name);
      channel.connect(ConcurrentStartTest.class.getSimpleName());
      log.tracef("Channel %s connected: %s", channel, channel.getViewAsString());
      return channel;
   }

   private void replaceInboundInvocationHandler(EmbeddedCacheManager cm, CheckPoint checkPoint) {
      InboundInvocationHandler handler = extractGlobalComponent(cm, InboundInvocationHandler.class);
      BlockingInboundInvocationHandler ourHandler = new BlockingInboundInvocationHandler(handler, checkPoint);
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

      public BlockingInboundInvocationHandler(InboundInvocationHandler delegate, CheckPoint checkPoint) {
         this.delegate = delegate;
         this.checkPoint = checkPoint;
      }

      @Override
      public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof CacheTopologyControlCommand) {
            try {
               checkPoint.trigger("blocked_" + origin);
               checkPoint.awaitStrict("unblocked_" + origin, 10, SECONDS);
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
