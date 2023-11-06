package org.infinispan.persistence.remote;

import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Simple test to sample how remote cache store is configured with URI attribute.
 *
 * @author Durgesh Anaokar
 * @since 13.0.0
 */
@Test(testName = "persistence.remote.RemoteStoreConfigUriTest", groups = "functional")
public class RemoteStoreConfigUriTest extends RemoteStoreConfigTest {

   private static final int PORT = 19811;
   public static final String CACHE_LOADER_CONFIG_1 = "remote-cl-uri-config-1.xml";
   public static final String CACHE_LOADER_CONFIG_2 = "remote-cl-uri-config-2.xml";
   public static final String STORE_CACHE_NAME = "RemoteStoreConfigUriTest";

   @Factory
   public static Object[] factory() {
      return new Object[] {
            new RemoteStoreConfigUriTest(CACHE_LOADER_CONFIG_1, STORE_CACHE_NAME),
            new RemoteStoreConfigUriTest(CACHE_LOADER_CONFIG_2, STORE_CACHE_NAME),
      };
   }

   @Override
   protected String parameters() {
      return String.format("[xml=%s]", cacheLoaderConfig.replace(".xml", ""));
   }

   public RemoteStoreConfigUriTest(String cacheLoaderConfig, String storeCacheName) {
      super(cacheLoaderConfig, storeCacheName, PORT);
   }
}
