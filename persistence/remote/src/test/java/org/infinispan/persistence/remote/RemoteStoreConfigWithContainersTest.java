package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(testName = "persistence.remote.RemoteStoreConfigWithContainersTest", groups = "functional")
public class RemoteStoreConfigWithContainersTest extends RemoteStoreConfigTest {

   private static final int PORT = 19811;
   public static final String CACHE_LOADER_CONFIG = "remotestore-with-containers.xml";
   public static final String DEFAULT_STORE_CACHE_NAME = "RemoteStoreWithDefaultContainer";
   public static final String ALTERNATIVE_STORE_CACHE_NAME = "RemoteStoreWithNamedContainer";

   protected RemoteStoreConfigWithContainersTest(String cacheName) {
      super(CACHE_LOADER_CONFIG, cacheName, PORT);
   }

   @BeforeClass
   @Override
   public void startUp() {
      super.startUp();
      switch (storeCacheName) {
         case ALTERNATIVE_STORE_CACHE_NAME:
            cacheManager.createCache(DEFAULT_STORE_CACHE_NAME, hotRodCacheConfiguration().build());
            break;
         case DEFAULT_STORE_CACHE_NAME:
            cacheManager.createCache(ALTERNATIVE_STORE_CACHE_NAME, hotRodCacheConfiguration().build());
            break;
      }
   }

   @Factory
   protected static Object[] factory() {
      return new Object[] {
            new RemoteStoreConfigWithContainersTest(DEFAULT_STORE_CACHE_NAME),
            new RemoteStoreConfigWithContainersTest(ALTERNATIVE_STORE_CACHE_NAME),
      };
   }

   @Override
   protected String parameters() {
      return "[cache=" + storeCacheName + "]";
   }
}
