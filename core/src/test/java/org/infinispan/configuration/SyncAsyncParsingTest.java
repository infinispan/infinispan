package org.infinispan.configuration;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

@Test(testName = "config.SyncAsyncParsingTest", groups = "functional")
public class SyncAsyncParsingTest {

   @Test (expectedExceptions = CacheConfigurationException.class)
   public void testSyncAndAsyncElements() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
              "<infinispan>" +
              "<global><transport /></global>" +
              "<default><clustering><sync /><async /></clustering></default></infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      CacheContainer cc = TestCacheManagerFactory.fromStream(stream);
      cc.getCache();
   }
}
