package org.infinispan.client.hotrod.query.projection;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Developer;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.projection.RemoteDistributedMetaProjectionTest")
public class RemoteDistributedMetaProjectionTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("io.dev.Developer");
      createHotRodServers(2, config);
      waitForClusterToForm();
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Developer.DeveloperSchema.INSTANCE;
   }

   @Test
   public void testVersionProjection() {
      RemoteMetaProjectionTest.smokeTest(clients.get(0).getCache());
   }
}
