package org.infinispan.api.client.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.api.Infinispan;
import org.infinispan.api.configuration.ClientConfig;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.api.client.impl.ConfigurationKeyValueStoreTest")
public class ConfigurationKeyValueStoreTest extends SingleHotRodServerTest {

   public static final String CACHE_NAME = "test";

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      cacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(CACHE_NAME, new org.infinispan.configuration.cache.ConfigurationBuilder().build());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   public void testConfiguration() {
      Configuration config = new ConfigurationBuilder().build();

      ClientConfig clientConfig = ClientConfig.from(config.properties());
      assertNotNull(clientConfig);

      Infinispan client = Infinispan.newClient(clientConfig);
      assertNotNull(client);
      await(client.stop());
   }

}
