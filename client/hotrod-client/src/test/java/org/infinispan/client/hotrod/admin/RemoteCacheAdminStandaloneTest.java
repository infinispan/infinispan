package org.infinispan.client.hotrod.admin;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.RemoteCacheAdminStandaloneTest")
public class RemoteCacheAdminStandaloneTest extends SingleHotRodServerTest {

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN005034.*")
   public void testCreateClusteredCacheStandAloneServer() {
      String xml = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"></distributed-cache></cache-container></infinispan>", "cache");
      RemoteCache<?, ?> cache = remoteCacheManager.administration().createCache("cache", new XMLStringConfiguration(xml));
      assertEquals(0, cache.size());
   }

}
