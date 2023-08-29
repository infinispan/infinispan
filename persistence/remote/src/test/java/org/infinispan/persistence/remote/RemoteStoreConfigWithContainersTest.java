package org.infinispan.persistence.remote;

import org.testng.annotations.Test;

@Test(testName = "persistence.remote.RemoteStoreConfigWithContainersTest", groups = "functional")
public class RemoteStoreConfigWithContainersTest extends RemoteStoreConfigTest {

   private static final int PORT = 19811;
   public static final String CACHE_LOADER_CONFIG = "remotestore-with-containers.xml";
   public static final String STORE_CACHE_NAME = "RemoteStoreWithDefaultContainer";

   public RemoteStoreConfigWithContainersTest() {
      super(CACHE_LOADER_CONFIG, STORE_CACHE_NAME, PORT);
   }
}
