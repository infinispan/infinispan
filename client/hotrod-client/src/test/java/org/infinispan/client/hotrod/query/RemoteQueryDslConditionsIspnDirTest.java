package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Verifying the functionality of remote queries for infinispan directory_provider.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.RemoteQueryDslConditionsIspnDirTest", groups = "functional")
public class RemoteQueryDslConditionsIspnDirTest extends RemoteQueryDslConditionsTest {

   protected static final String TEST_CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultCacheConfiguration = new ConfigurationBuilder();
      createClusteredCaches(1, defaultCacheConfiguration);

      ConfigurationBuilder cfg = getConfigurationBuilder();
      manager(0).defineConfiguration(TEST_CACHE_NAME, cfg.build());
      cache = manager(0).getCache(TEST_CACHE_NAME);

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);

      initProtoSchema(remoteCacheManager);
   }

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = super.getConfigurationBuilder();
      builder.indexing().addProperty("default.directory_provider", "infinispan");
      return builder;
   }
}
