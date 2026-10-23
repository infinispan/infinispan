package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.query.RemoteQueryConditionsIspnDirTest", groups = "functional")
public class RemoteQueryConditionsIspnDirTest extends RemoteQueryConditionsTest {

   private static final String TEST_CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().clusteredDefault();
      globalBuilder.serialization().addContextInitializers(TestDomainSCI.INSTANCE);
      createClusteredCaches(1, globalBuilder, new ConfigurationBuilder(), true);

      ConfigurationBuilder cfg = getConfigurationBuilder();
      manager(0).defineConfiguration(TEST_CACHE_NAME, cfg.build());
      cache = manager(0).getCache(TEST_CACHE_NAME);

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort()).addContextInitializers(TestDomainSCI.INSTANCE);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
   }

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = super.getConfigurationBuilder();
      builder.indexing().storage(LOCAL_HEAP);
      return builder;
   }
}
