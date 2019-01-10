package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;

import java.net.SocketTimeoutException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.SocketTimeoutErrorTest.TimeoutInducingInterceptor;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.EventSocketTimeoutTest")
public class EventSocketTimeoutTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().interceptor(
         new TimeoutInducingInterceptor()).after(EntryWrappingInterceptor.class);
      return TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(builder));
   }

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.workerThreads(6); // TODO: Remove workerThreads configuration when ISPN-5083 implemented
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, builder);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
         HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host(hotrodServer.getHost()).port(hotrodServer.getPort());
      builder.socketTimeout(2000);
      builder.maxRetries(0);
      return new RemoteCacheManager(builder.build());
   }

   public void testSocketTimeoutWithEvent() {
      final EventLogListener<String> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put("uno", 1);
         l.expectOnlyCreatedEvent("uno");
         try {
            remote.put("FailFailFail", 99);
            Assert.fail("SocketTimeoutException expected");
         } catch (HotRodClientException e) {
            assertTrue(e.getCause() instanceof SocketTimeoutException); // ignore
         }
         remote.put("dos", 2);
         l.expectOnlyCreatedEvent("dos");
      });
   }

}
