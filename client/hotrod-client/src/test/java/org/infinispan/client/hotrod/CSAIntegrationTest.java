package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHotRodEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.marshall;
import static org.infinispan.test.TestingUtil.blockUntilCacheStatusAchieved;
import static org.infinispan.test.TestingUtil.blockUntilViewReceived;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.NoopChannelOperation;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.CSAIntegrationTest")
public class CSAIntegrationTest extends HitsAwareCacheManagersTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private ChannelFactory channelFactory;

   private static final Log log = LogFactory.getLog(CSAIntegrationTest.class);

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      builder.unsafe().unreliableReturnValues(true);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(manager(0));
      addr2hrServer.put(InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort()), hotRodServer1);
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(manager(1));
      addr2hrServer.put(InetSocketAddress.createUnresolved(hotRodServer2.getHost(), hotRodServer2.getPort()), hotRodServer2);
      hotRodServer3 = HotRodClientTestingUtil.startHotRodServer(manager(2));
      addr2hrServer.put(InetSocketAddress.createUnresolved(hotRodServer3.getHost(), hotRodServer3.getPort()), hotRodServer3);

      assert manager(0).getCache() != null;
      assert manager(1).getCache() != null;
      assert manager(2).getCache() != null;

      blockUntilViewReceived(manager(0).getCache(), 3);
      blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);
      blockUntilCacheStatusAchieved(manager(2).getCache(), ComponentStatus.RUNNING, 10000);

      //Important: this only connects to one of the two servers!
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
         HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotRodServer2);
      remoteCacheManager = new InternalRemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      channelFactory = ((InternalRemoteCacheManager) remoteCacheManager).getChannelFactory();
   }

   protected void setHotRodProtocolVersion(org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder) {
      // No-op, use default Hot Rod protocol version
   }

   @AfterClass
   @Override
   protected void destroy() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer1, hotRodServer2, hotRodServer3);
      super.destroy();
   }

   public void testHashInfoRetrieved() throws InterruptedException {
      assertEquals(3, channelFactory.getServers().size());
      for (int i = 0; i < 10; i++) {
         remoteCache.put("k", "v");
         if (channelFactory.getServers().size() == 3) break;
         Thread.sleep(1000);
      }
      assertEquals(3, channelFactory.getServers().size());
      assertNotNull(channelFactory.getConsistentHash(HotRodConstants.DEFAULT_CACHE_NAME_BYTES));
   }

   @Test(dependsOnMethods = "testHashInfoRetrieved")
   public void testCorrectSetup() {
      remoteCache.put("k", "v");
      assert remoteCache.get("k").equals("v");
   }

   @Test(dependsOnMethods = "testCorrectSetup")
   public void testHashFunctionReturnsSameValues() throws InterruptedException {
      for (int i = 0; i < 1000; i++) {
         byte[] key = generateKey(i);
         Channel channel = channelFactory.fetchChannelAndInvoke(key, null, HotRodConstants.DEFAULT_CACHE_NAME_BYTES, new NoopChannelOperation()).join();
         SocketAddress serverAddress = ChannelRecord.of(channel).getUnresolvedAddress();
         CacheContainer cacheContainer = addr2hrServer.get(serverAddress).getCacheManager();
         assertNotNull("For server address " + serverAddress + " found " + cacheContainer + ". Map is: " + addr2hrServer, cacheContainer);
         DistributionManager distributionManager = cacheContainer.getCache().getAdvancedCache().getDistributionManager();
         Address clusterAddress = cacheContainer.getCache().getAdvancedCache().getRpcManager().getAddress();

         ConsistentHash serverCh = distributionManager.getReadConsistentHash();
         int numSegments = serverCh.getNumSegments();
         int keySegment = serverCh.getSegment(key);
         Address serverOwner = serverCh.locatePrimaryOwnerForSegment(keySegment);
         Address serverPreviousOwner = serverCh.locatePrimaryOwnerForSegment((keySegment - 1 + numSegments) % numSegments);
         assert clusterAddress.equals(serverOwner) || clusterAddress.equals(serverPreviousOwner);
         channelFactory.releaseChannel(channel);
      }
   }

   @Test(dependsOnMethods = "testHashFunctionReturnsSameValues")
   public void testRequestsGoToExpectedServer() throws Exception {
      addInterceptors();
      List<byte[]> keys = new ArrayList<byte[]>();
      for (int i = 0; i < 500; i++) {
         byte[] key = generateKey(i);
         keys.add(key);
         String keyStr = new String(key);
         remoteCache.put(keyStr, "value");
         Channel channel = channelFactory.fetchChannelAndInvoke(marshall(keyStr), null, RemoteCacheManager.cacheNameBytes(), new NoopChannelOperation()).join();
         assertHotRodEquals(addr2hrServer.get(ChannelRecord.of(channel).getUnresolvedAddress()).getCacheManager(), keyStr, "value");
         channelFactory.releaseChannel(channel);
      }

      log.info("Right before first get.");

      for (byte[] key : keys) {
         resetStats();
         String keyStr = new String(key);
         assert remoteCache.get(keyStr).equals("value");
         Channel channel = channelFactory.fetchChannelAndInvoke(marshall(keyStr), null, HotRodConstants.DEFAULT_CACHE_NAME_BYTES, new NoopChannelOperation()).join();
         assertOnlyServerHit(ChannelRecord.of(channel).getUnresolvedAddress());
         channelFactory.releaseChannel(channel);
      }
   }

   private byte[] generateKey(int i) {
      Random r = new Random();
      byte[] result = new byte[i];
      r.nextBytes(result);
      return result;
   }

}
