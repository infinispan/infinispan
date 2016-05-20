package org.infinispan.query.remote.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.remote.impl.ProtobufMetadataClusterPreserverStateAcrossRestartsTest")
public class ProtobufMetadataClusterPreserverStateAcrossRestartsTest extends ProtobufMetadataCachePreserveStateAcrossRestartsTest {

   protected EmbeddedCacheManager createCacheManager(String persistentStateLocation) throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().clusteredDefault();
      global.globalState().enable().persistentLocation(persistentStateLocation);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(global, new ConfigurationBuilder());
      cacheManager.getCache();
      return cacheManager;
   }
}
