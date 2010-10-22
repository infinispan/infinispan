package org.infinispan.config;

import org.infinispan.manager.CacheContainer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

@Test(testName = "config.SyncAsyncParsingTest", groups = "functional")
public class SyncAsyncParsingTest {

   @Test (expectedExceptions = ConfigurationException.class)
   public void testSyncAndAsyncElements() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
              "<infinispan\n" +
              "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
              "      xsi:schemaLocation=\"urn:infinispan:config:4.0 http://www.infinispan.org/schemas/infinispan-config-4.0.xsd\"\n" +
              "      xmlns=\"urn:infinispan:config:4.0\">" +
              "<global><transport /></global>" + 
              "<default><clustering><sync /><async /></clustering></default></infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      CacheContainer cc = TestCacheManagerFactory.fromStream(stream);
      cc.getCache();
   }
}
