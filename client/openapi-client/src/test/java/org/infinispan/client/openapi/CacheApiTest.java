package org.infinispan.client.openapi;

import org.infinispan.client.openapi.api.CacheApi;
import org.junit.Test;

public class CacheApiTest {

   @Test
   public void testCacheAPI() throws ApiException {
      CacheApi cacheApi = new CacheApi();
      cacheApi.cacheExists("a");
   }
}
