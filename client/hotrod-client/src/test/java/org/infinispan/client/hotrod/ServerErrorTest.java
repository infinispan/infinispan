package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.unmarshall;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.testing.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Queue;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
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
   private RemoteCacheManager remoteCacheManager;
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

   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host(hotrodServer.getHost()).port(hotrodServer.getPort());
      clientBuilder.connectionPool().maxActive(1).minIdle(1);
      return new RemoteCacheManager(clientBuilder.build());
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
      OperationDispatcher dispatcher = remoteCacheManager.getOperationDispatcher();
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotrodServer.getHost(), hotrodServer.getPort());
      Channel channel = dispatcher.getHandlerForAddress(address).getChannel();

      // Obtain a reference to the scheduled executor and its task queue
      AbstractScheduledEventExecutor scheduledExecutor = ((AbstractScheduledEventExecutor) channel.eventLoop());
      Queue<?> scheduledTaskQueue = TestingUtil.extractField(scheduledExecutor, "scheduledTaskQueue");
      int scheduledTasksBaseline = scheduledTaskQueue.size();

      log.debug("Sending failing operation to server");
      expectException(HotRodClientException.class,
                      () -> remoteCache.put("FailFailFail", "whatever..."));

      // Check that the operation was completed
      HeaderDecoder headerDecoder = channel.pipeline().get(HeaderDecoder.class);
      assertEquals(0, headerDecoder.registeredOperationsById().size());

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
