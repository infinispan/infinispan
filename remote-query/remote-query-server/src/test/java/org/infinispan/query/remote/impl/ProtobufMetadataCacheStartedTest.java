package org.infinispan.query.remote.impl;

import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.remote.impl.ProtobufMetadataCacheStartedTest")
public class ProtobufMetadataCacheStartedTest extends AbstractInfinispanTest {

   protected EmbeddedCacheManager createCacheManager(String persistentStateLocation) throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().clusteredDefault();
      global.globalState().enable().persistentLocation(persistentStateLocation);
      return TestCacheManagerFactory.createClusteredCacheManager(global, new ConfigurationBuilder());
   }

   public void testMetadataCacheStarted() throws Exception {
      String persistentStateLocation = CommonsTestingUtil.tmpDirectory(getClass());
      Util.recursiveFileRemove(persistentStateLocation);

      final String persistentStateLocation1 = persistentStateLocation + "/1";
      final String persistentStateLocation2 = persistentStateLocation + "/2";
      TestingUtil.withCacheManagers(new MultiCacheManagerCallable(createCacheManager(persistentStateLocation1),
                                                                  createCacheManager(persistentStateLocation2)) {
         @Override
         public void call() {
            assertTrue(cms[0].isRunning(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME));

            assertTrue(cms[1].isRunning(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME));
         }
      });
   }
}
