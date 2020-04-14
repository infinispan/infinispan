package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.SocketTimeoutException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.SocketTimeoutErrorTest.TimeoutInducingInterceptor;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, builder);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host(hotrodServer.getHost()).port(hotrodServer.getPort());
      builder.socketTimeout(1000);
      builder.maxRetries(0);
      return new RemoteCacheManager(builder.build());
   }

   public void testSocketTimeoutWithEvent() {
      final EventLogListener<String> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put("uno", 1);
         l.expectOnlyCreatedEvent("uno");
         Exceptions.expectException(TransportException.class, SocketTimeoutException.class, () -> remote.put("FailFailFail", 99));
         l.expectNoEvents();
         remote.put("dos", 2);
         l.expectOnlyCreatedEvent("dos");
      });
   }

}
