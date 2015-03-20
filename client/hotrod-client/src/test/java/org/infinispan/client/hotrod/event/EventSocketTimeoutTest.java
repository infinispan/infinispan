package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.SocketTimeoutErrorTest.TimeoutInducingInterceptor;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;

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
         new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.socketTimeout(2000);
      builder.maxRetries(0);
      return new RemoteCacheManager(builder.build());
   }

   public void testSocketTimeoutWithEvent() {
      final EventLogListener<String> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<String, Integer> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put("uno", 1);
            eventListener.expectOnlyCreatedEvent("uno", cache());
            try {
               cache.put("FailFailFail", 99);
               Assert.fail("SocketTimeoutException expected");
            } catch (HotRodClientException e) {
               assertTrue(e.getCause() instanceof SocketTimeoutException); // ignore
            }
            cache.put("dos", 2);
            eventListener.expectOnlyCreatedEvent("dos", cache());
         }
      });
   }

}
