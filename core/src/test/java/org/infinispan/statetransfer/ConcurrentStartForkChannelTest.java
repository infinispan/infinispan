package org.infinispan.statetransfer;

import static org.infinispan.test.TestingUtil.blockUntilViewsReceived;
import static org.infinispan.test.TestingUtil.waitForStableTopology;

import java.io.ByteArrayInputStream;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.JGroupsConfigBuilder;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.fork.ForkChannel;
import org.jgroups.fork.UnknownForkHandler;
import org.jgroups.protocols.FORK;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests concurrent startup of caches using ForkChannels.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Test(testName = "statetransfer.ConcurrentStartForkChannelTest", groups = "functional")
@CleanupAfterMethod
public class ConcurrentStartForkChannelTest extends MultipleCacheManagersTest {

   public static final byte[] FORK_NOT_FOUND_BUFFER = new byte[0];
   public static final String CACHE_NAME = "repl";

   @Override
   protected void createCacheManagers() throws Throwable {
      // The test method will create the cache managers
   }

   @DataProvider(name = "startOrder")
   public Object[][] startOrder() {
      return new Object[][]{{0, 1}, {1, 0}};
   }

   @Test(timeOut = 30000, dataProvider = "startOrder")
   public void testConcurrentStart(int eagerManager, int lazyManager) throws Exception {
      TestResourceTracker.testThreadStarted(this);

      ConfigurationBuilder replCfg = new ConfigurationBuilder();
      replCfg.clustering().cacheMode(CacheMode.REPL_SYNC).stateTransfer().timeout(30, TimeUnit.SECONDS);

      String name1 = TestResourceTracker.getNextNodeName();
      String name2 = TestResourceTracker.getNextNodeName();

      // Create and connect both channels beforehand
      JChannel ch1 = createChannel(name1, 0);
      JChannel ch2 = createChannel(name2, 1);

      // Create the cache managers, but do not start them yet
      EmbeddedCacheManager cm1 = createCacheManager(replCfg, name1, ch1);
      EmbeddedCacheManager cm2 = createCacheManager(replCfg, name2, ch2);

      cm1.defineConfiguration(CACHE_NAME, replCfg.build());
      cm2.defineConfiguration(CACHE_NAME, replCfg.build());

      try {
         log.debugf("Cache managers created. Starting the caches");
         // When the coordinator starts first, it's ok to just start the caches in sequence.
         // When the coordinator starts last, however, the other node is not able to start before the
         // coordinator has the ClusterTopologyManager running.
         Future<Cache<String, String>> c1rFuture = fork(() -> manager(eagerManager).getCache(CACHE_NAME));
         Thread.sleep(1000);
         Cache<String, String> c2r = manager(lazyManager).getCache(CACHE_NAME);
         Cache<String, String> c1r = c1rFuture.get(10, TimeUnit.SECONDS);

         blockUntilViewsReceived(10000, cm1, cm2);
         waitForStableTopology(c1r, c2r);
      } finally {
         // Stopping the cache managers isn't enough, because it will only close the ForkChannels
         cm1.stop();
         ch1.close();
         cm2.stop();
         ch2.close();
      }
   }

   private EmbeddedCacheManager createCacheManager(ConfigurationBuilder cacheCfg, String name,
                                                   JChannel channel) throws
         Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.transport().nodeName(channel.getName());
      gcb.globalJmxStatistics().allowDuplicateDomains(true);
      gcb.transport().distributedSyncTimeout(30, TimeUnit.SECONDS);

      FORK fork = new FORK();
      fork.setUnknownForkHandler(new UnknownForkHandler() {
         @Override
         public Object handleUnknownForkStack(Message message, String forkStackId) {
            return handle(message);
         }

         @Override
         public Object handleUnknownForkChannel(Message message, String forkChannelId) {
            return handle(message);
         }

         private Object handle(Message message) {
            short id = ClassConfigurator.getProtocolId(RequestCorrelator.class);
            RequestCorrelator.Header header = message.getHeader(id);
            if (header != null) {
               log.debugf("Sending CacheNotFoundResponse reply for %s", header);
               short flags = (short) ((message.getFlags() | CommandAwareRpcDispatcher.REPLY_FLAGS_TO_SET) &
                     ~CommandAwareRpcDispatcher.REPLY_FLAGS_TO_CLEAR);
               Message response = message.makeReply().setFlag(flags);

               response.putHeader(FORK.ID, message.getHeader(FORK.ID));
               response.putHeader(id,
                     new RequestCorrelator.Header(RequestCorrelator.Header.RSP, header.req_id, id));
               response.setBuffer(FORK_NOT_FOUND_BUFFER);

               fork.down(response);
            }
            return null;
         }
      });
      channel.getProtocolStack().addProtocol(fork);
      ForkChannel fch = new ForkChannel(channel, "stack1", "channel1");
      CustomChannelLookup.registerChannel(gcb, fch, name, true);

      EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build(), cacheCfg.build(), false);
      registerCacheManager(cm);
      return cm;
   }

   private JChannel createChannel(String name, int portRange) throws Exception {
      String configString = JGroupsConfigBuilder
            .getJGroupsConfig(ConcurrentStartForkChannelTest.class.getName(),
                  new TransportFlags().withPortRange(portRange));
      JChannel channel = new JChannel(new ByteArrayInputStream(configString.getBytes()));
      channel.setName(name);
      channel.connect(ConcurrentStartForkChannelTest.class.getSimpleName());
      log.tracef("Channel %s connected: %s", channel, channel.getViewAsString());
      return channel;
   }

}
