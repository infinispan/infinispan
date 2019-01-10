package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_27;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.CustomEventLogListener.CustomEvent;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.OldClientCustomEventsTest")
public class OldClientCustomEventsTest extends SingleHotRodServerTest {

   private static final String MAGIC_KEY = "key-42";
   private static final ProtocolVersion VERSION = PROTOCOL_VERSION_27;

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cacheManager, builder);
      server.addCacheEventFilterFactory("static-filter-factory", new EventLogListener.StaticCacheEventFilterFactory<>(MAGIC_KEY));
      server.addCacheEventConverterFactory("static-converter-factory", new CustomEventLogListener.StaticConverterFactory());
      return server;
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort()).version(VERSION);
      return new InternalRemoteCacheManager(builder.build());
   }

   public void testFilteredEvents() {
      final EventLogListener.StaticFilteredEventLogListener<String> l = new EventLogListener.StaticFilteredEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, new Object[]{"match"}, new Object[]{}, remote -> {
         l.expectNoEvents();
         remote.put("key-1", "one");
         remote.put("key-2", "two");
         remote.put("key-3", "three");
         l.expectNoEvents();
         remote.put(MAGIC_KEY, "hot key");
         l.expectOnlyCreatedEvent(MAGIC_KEY);
      });
   }

   public void testConvertedEvents() {
      final CustomEventLogListener.StaticCustomEventLogListener<String> l = new CustomEventLogListener.StaticCustomEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put("key-1", "one");
         l.expectCreatedEvent(new CustomEvent<>("key-1", "one", 0));
      });
   }

}
