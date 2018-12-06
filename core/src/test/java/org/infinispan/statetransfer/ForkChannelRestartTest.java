package org.infinispan.statetransfer;

import static org.infinispan.test.TestingUtil.blockUntilViewsReceived;
import static org.infinispan.test.TestingUtil.getDiscardForCache;
import static org.infinispan.test.TestingUtil.installNewView;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
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
import org.testng.annotations.Test;

/**
 * Tests restart of nodes using ForkChannels.
 *
 * @author Dan Berindei
 * @since 10.0
 */
@Test(testName = "statetransfer.ForkChannelRestartTest", groups = "functional")
@CleanupAfterMethod
public class ForkChannelRestartTest extends MultipleCacheManagersTest {
   private static final byte[] FORK_NOT_FOUND_BUFFER = Util.EMPTY_BYTE_ARRAY;
   private static final String CACHE_NAME = "repl";
   private static final int CLUSTER_SIZE = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      // The test method will create the cache managers
   }

   public void testRestart() throws Exception {
      TestResourceTracker.testThreadStarted(this);

      ConfigurationBuilder replCfg = new ConfigurationBuilder();
      replCfg.clustering().cacheMode(CacheMode.REPL_SYNC).stateTransfer().timeout(30, TimeUnit.SECONDS);
      replCfg.clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);

      String[] names = new String[CLUSTER_SIZE + 1];
      JChannel[] channels = new JChannel[CLUSTER_SIZE + 1];
      EmbeddedCacheManager[] managers = new EmbeddedCacheManager[CLUSTER_SIZE + 1];
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         configureManager(replCfg, names, channels, managers, i);
      }
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         managers[i].getCache(CACHE_NAME);
      }

      log.debugf("Cache managers created. Crashing manager %s but keeping the channel in the view", names[1]);
      getDiscardForCache(managers[1]).setDiscardAll(true);
      installNewView(managers[1]);
      managers[1].stop();

      configureManager(replCfg, names, channels, managers, CLUSTER_SIZE);
      Future<Cache<Object, Object>> future = fork(() -> managers[CLUSTER_SIZE].getCache(CACHE_NAME));

      Thread.sleep(1000);
      log.debugf("Stopping channel %s", names[1]);
      channels[1].close();

      List<EmbeddedCacheManager> liveManagers = new ArrayList<>(Arrays.asList(managers));
      liveManagers.remove(1);
      blockUntilViewsReceived(10000, false, liveManagers);
      waitForNoRebalance(liveManagers.stream().map(cm -> cm.getCache(CACHE_NAME)).collect(Collectors.toList()));
      log.debug("Rebalance finished successfully");

      future.get(10, TimeUnit.SECONDS);
   }

   private void configureManager(ConfigurationBuilder replCfg, String[] names,
                                 JChannel[] channels,
                                 EmbeddedCacheManager[] managers, int i) throws Exception {
      // Create the ForkChannels
      names[i] = TestResourceTracker.getNextNodeName();
      channels[i] = createChannel(names[i], 0);

      // Then start the managers
      managers[i] = createCacheManager(replCfg, names[i], channels[i]);
      managers[i].defineConfiguration(CACHE_NAME, replCfg.build());
   }

   private EmbeddedCacheManager createCacheManager(ConfigurationBuilder cacheCfg, String name,
                                                   JChannel channel) throws Exception {
      ForkChannel fch = new ForkChannel(channel, "stack1", "channel1");

      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.transport().nodeName(name);
      gcb.transport().transport(new JGroupsTransport(fch));
      gcb.transport().distributedSyncTimeout(40, TimeUnit.SECONDS);

      EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build(), cacheCfg.build(), false);
      registerCacheManager(cm);
      return cm;
   }

   private JChannel createChannel(String name, int portRange) throws Exception {
      String configString =
         JGroupsConfigBuilder.getJGroupsConfig(ForkChannelRestartTest.class.getName(),
                                               new TransportFlags().withPortRange(portRange).withFD(true));

      JChannel channel = new JChannel(new ByteArrayInputStream(configString.getBytes()));
      TestResourceTracker.addResource(new TestResourceTracker.Cleaner<JChannel>(channel) {
         @Override
         public void close() {
            ref.close();
         }
      });
      channel.setName(name);
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
            RequestCorrelator.Header requestHeader = message.getHeader(id);
            if (requestHeader != null) {
               log.debugf("Sending CacheNotFoundResponse reply from %s for %s", name, requestHeader);
               short flags = JGroupsTransport.REPLY_FLAGS;
               Message response = message.makeReply().setFlag(flags);

               FORK.ForkHeader forkHeader = message.getHeader(FORK.ID);
               response.putHeader(FORK.ID, forkHeader);
               response.putHeader(id, new RequestCorrelator.Header(RequestCorrelator.Header.RSP, requestHeader.req_id, id));
               response.setBuffer(FORK_NOT_FOUND_BUFFER);

               fork.down(response);
            }
            return null;
         }
      });
      channel.getProtocolStack().addProtocol(fork);
      channel.connect("FORKISPN");
      log.tracef("Channel %s connected: %s", channel, channel.getViewAsString());
      return channel;
   }
}
