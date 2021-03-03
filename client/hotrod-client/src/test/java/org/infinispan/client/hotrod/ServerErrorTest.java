package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.unmarshall;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Queue;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.NoopChannelOperation;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;

/**
 * Tests HotRod client and server behaviour when server throws a server error
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "client.hotrod.ServerErrorTest")
public class ServerErrorTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;
   private InternalRemoteCacheManager remoteCacheManager;
   private RemoteCache<String, String> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration());
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      remoteCacheManager = getRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache();

      return cacheManager;
   }

   protected InternalRemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host(hotrodServer.getHost()).port(hotrodServer.getPort());
      clientBuilder.connectionPool().maxActive(1).minIdle(1);
      return new InternalRemoteCacheManager(clientBuilder.build());
   }

   @AfterClass
   public void shutDownHotrod() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotrodServer);
      hotrodServer = null;
   }

   public void testErrorWhileDoingPut(Method m) {
      cache.getAdvancedCache().withStorageMediaType().addListener(new ErrorInducingListener());
      remoteCache = remoteCacheManager.getCache();

      remoteCache.put(k(m), v(m));
      assertEquals(v(m), remoteCache.get(k(m)));

      // Obtain a reference to the single connection in the pool
      ChannelFactory channelFactory = remoteCacheManager.getChannelFactory();
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotrodServer.getHost(), hotrodServer.getPort());
      Channel channel = channelFactory.fetchChannelAndInvoke(address, new NoopChannelOperation()).join();

      // Obtain a reference to the scheduled executor and its task queue
      AbstractScheduledEventExecutor scheduledExecutor = ((AbstractScheduledEventExecutor) channel.eventLoop());
      Queue<?> scheduledTaskQueue = TestingUtil.extractField(scheduledExecutor, "scheduledTaskQueue");
      int scheduledTasksBaseline = scheduledTaskQueue.size();

      // Release the channel back into the pool
      channelFactory.releaseChannel(channel);
      assertEquals(0, channelFactory.getNumActive(address));
      assertEquals(1, channelFactory.getNumIdle(address));

      log.debug("Sending failing operation to server");
      expectException(HotRodClientException.class,
                      () -> remoteCache.put("FailFailFail", "whatever..."));
      assertEquals(0, channelFactory.getNumActive(address));
      assertEquals(1, channelFactory.getNumIdle(address));

      // Check that the operation was completed
      HeaderDecoder headerDecoder = channel.pipeline().get(HeaderDecoder.class);
      assertEquals(0, headerDecoder.registeredOperations());

      // Check that the timeout task was cancelled
      assertEquals(scheduledTasksBaseline, scheduledTaskQueue.size());

      log.debug("Sending new request after server failure");
      remoteCache.put(k(m, 2), v(m, 2));
      assertEquals(v(m, 2), remoteCache.get(k(m, 2)));
   }

   @Listener
   public static class ErrorInducingListener {
      @CacheEntryCreated
      @SuppressWarnings("unused")
      public void entryCreated(CacheEntryEvent<byte[], byte[]> event) throws Exception {
         if (event.isPre() && unmarshall(event.getKey()).equals("FailFailFail")) {
            throw new TestException("Simulated server failure");
         }
      }
   }
}
