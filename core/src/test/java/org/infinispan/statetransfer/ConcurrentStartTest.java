package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelLookup;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.JGroupsConfigBuilder;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

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

   @Override
   protected void createCacheManagers() throws Throwable {
      // The test method will create the cache managers
   }

   @Test(timeOut = 60000)
   public void testConcurrentStart() throws Exception {
      TestResourceTracker.testThreadStarted(this);
      final CheckPoint checkPoint = new CheckPoint();

      // Create and connect both channels beforehand
      // We need both nodes in the view when the coordinator's ClusterTopologyManagerImpl starts
      // in order to reproduce the ISPN-5106 deadlock
      JChannel ch1 = new JChannel(JGroupsConfigBuilder.getJGroupsConfig(ConcurrentStartTest.class.getName(), new TransportFlags()));
      ch1.setName(TestResourceTracker.getNextNodeName());
      ch1.connect(ConcurrentStartTest.class.getSimpleName());
      JChannel ch2 = new JChannel(JGroupsConfigBuilder.getJGroupsConfig(ConcurrentStartTest.class.getName(), new TransportFlags()));
      ch2.setName(TestResourceTracker.getNextNodeName());
      ch2.connect(ConcurrentStartTest.class.getSimpleName());

      // Use a JGroupsChannelLookup to pass the created channels to the transport
      GlobalConfigurationBuilder gcb1 = new GlobalConfigurationBuilder();
      gcb1.globalJmxStatistics().allowDuplicateDomains(true);
      CustomChannelLookup.registerChannel(ch1, gcb1.transport());
      EmbeddedCacheManager cm1 = new DefaultCacheManager(gcb1.build());
      registerCacheManager(cm1);
      GlobalConfigurationBuilder gcb2 = new GlobalConfigurationBuilder();
      gcb2.globalJmxStatistics().allowDuplicateDomains(true);
      CustomChannelLookup.registerChannel(ch2, gcb2.transport());
      EmbeddedCacheManager cm2 = new DefaultCacheManager(gcb2.build());
      registerCacheManager(cm2);

      // Install the blocking invocation handlers
      assertEquals(ComponentStatus.INSTANTIATED, extractGlobalComponentRegistry(cm1).getStatus());
      replaceInboundInvocationHandler(cm1, checkPoint);
      assertEquals(ComponentStatus.INSTANTIATED, extractGlobalComponentRegistry(cm2).getStatus());
      replaceInboundInvocationHandler(cm2, checkPoint);

      log.debugf("Channels created. Starting the caches");
      Configuration replCfg = new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).build();
      Configuration distCfg = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();
      cm1.defineConfiguration("repl", replCfg);
      cm1.defineConfiguration("dist", distCfg);
      cm2.defineConfiguration("repl", replCfg);
      cm2.defineConfiguration("dist", distCfg);

      fork(new CacheStartCallable(cm1, "repl"));
      fork(new CacheStartCallable(cm2, "repl"));
      fork(new CacheStartCallable(cm1, "dist"));
      fork(new CacheStartCallable(cm2, "dist"));

      // Wait for the POLICY_GET_STATUS commands to block
      checkPoint.await("blocked_" + ch1.getAddress(), 10, SECONDS);
      checkPoint.await("blocked_" + ch2.getAddress(), 10, SECONDS);
      // And trigger the deadlock
      // StateTransferManagerImpl.waitForInitialStateTransferToComplete also uses POLICY_GET_STATUS
      checkPoint.trigger("unblocked_" + cm1.getAddress(), CheckPoint.INFINITE);
      checkPoint.trigger("unblocked_" + cm2.getAddress(), CheckPoint.INFINITE);

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
      private final CheckPoint checkPoint;
      private final InboundInvocationHandler delegate;

      public BlockingInboundInvocationHandler(InboundInvocationHandler delegate, CheckPoint checkPoint) {
         this.delegate = delegate;
         this.checkPoint = checkPoint;
      }

      @Override
      public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof CacheTopologyControlCommand) {
            if (((CacheTopologyControlCommand) command).getType() == CacheTopologyControlCommand.Type.POLICY_GET_STATUS) {
               try {
                  checkPoint.trigger("blocked_" + origin);
                  checkPoint.awaitStrict("unblocked_" + origin, 10, SECONDS);
               } catch (Exception e) {
                  // Ignore
               }
            }
         }
         delegate.handleFromCluster(origin, command, reply, order);
      }

      @Override
      public void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply, DeliverOrder order) {
         delegate.handleFromRemoteSite(origin, command, reply, order);
      }
   }

   public static class CustomChannelLookup implements JGroupsChannelLookup {
      private static final Map<String, JChannel> channelMap = CollectionFactory.makeConcurrentMap();

      public static void registerChannel(JChannel channel, TransportConfigurationBuilder tcb) {
         String nodeName = channel.getName();
         tcb.defaultTransport();
         tcb.addProperty(JGroupsTransport.CHANNEL_LOOKUP, CustomChannelLookup.class.getName());
         tcb.addProperty(CustomChannelLookup.class.getName(), nodeName);
         channelMap.put(nodeName, channel);
      }

      @Override
      public Channel getJGroupsChannel(Properties p) {
         String nodeName = p.getProperty(CustomChannelLookup.class.getName());
         return channelMap.remove(nodeName);
      }

      @Override
      public boolean shouldConnect() {
         return false;
      }

      @Override
      public boolean shouldDisconnect() {
         return true;
      }

      @Override
      public boolean shouldClose() {
         return true;
      }
   }
}


