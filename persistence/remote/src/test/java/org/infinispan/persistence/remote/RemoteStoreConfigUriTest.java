package org.infinispan.persistence.remote;

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
   public static final String CACHE_LOADER_CONFIG = "remote-cl-uri-config.xml";
   public static final String STORE_CACHE_NAME = "RemoteStoreConfigUriTest";

   public RemoteStoreConfigUriTest() {
      super(CACHE_LOADER_CONFIG,STORE_CACHE_NAME, PORT);
   }
}
