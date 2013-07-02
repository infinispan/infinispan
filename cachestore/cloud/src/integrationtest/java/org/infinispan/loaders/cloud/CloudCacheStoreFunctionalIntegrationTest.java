package org.infinispan.loaders.cloud;

import org.infinispan.Cache;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

@Test(groups = "unit", sequential = true, testName = "loaders.cloud.CloudCacheStoreFunctionalIntegrationTest")
public class CloudCacheStoreFunctionalIntegrationTest extends BaseCacheStoreFunctionalTest {

   private String proxyHost;
   private String proxyPort = "-1";
   private int maxConnections = 20;
   private boolean isSecure = false;
   private String csBucket;
   private String accessKey;
   private String secretKey;
   private String cs;

   private static final String sysUsername = System.getProperty("infinispan.test.jclouds.username");
   private static final String sysPassword = System.getProperty("infinispan.test.jclouds.password");
   private static final String sysService = System.getProperty("infinispan.test.jclouds.service");

   @BeforeTest
   @Parameters({"infinispan.test.jclouds.username", "infinispan.test.jclouds.password", "infinispan.test.jclouds.service"})
   protected void setUpClient(@Optional String JcloudsUsername,
                              @Optional String JcloudsPassword,
                              @Optional String JcloudsService) throws Exception {

      accessKey = (JcloudsUsername == null) ? sysUsername : JcloudsUsername;
      secretKey = (JcloudsPassword == null) ? sysPassword : JcloudsPassword;
      cs = (JcloudsService == null) ? sysService : JcloudsService;

      if (accessKey == null || accessKey.trim().length() == 0 || secretKey == null || secretKey.trim().length() == 0) {
         accessKey = "dummy";
         secretKey = "dummy";
      }
      csBucket = (System.getProperty("user.name") + "." + this.getClass().getSimpleName()).toLowerCase().replace('.', '-'); // azure limitation on no periods
      csBucket = csBucket.length() > 32 ? csBucket.substring(0, 32): csBucket;//azure limitation on length
      System.out.printf("accessKey: %1$s, bucket: %2$s%n", accessKey, csBucket);
   }

   @AfterTest
   private void nukeBuckets() throws Exception {
      for (String name: cacheNames) {
         // use JClouds to nuke the buckets
         CloudCacheStore ccs = new CloudCacheStore();
         Cache c = mock(Cache.class);
         when(c.getName()).thenReturn(name);
         ccs.init(createCacheStoreConfig(), c, null);
         ccs.start();
         System.out.println("**** Nuking container " + ccs.containerName);
         ccs.blobStore.clearContainer(ccs.containerName);
         ccs.blobStore.deleteContainer(ccs.containerName);
         ccs.stop();
      }
      cacheNames.clear();
   }


   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      CloudCacheStoreConfig cfg = new CloudCacheStoreConfig();
      cfg.setCloudService(cs);
      cfg.setBucketPrefix(csBucket);
      cfg.setIdentity(accessKey);
      cfg.setPassword(secretKey);
      cfg.setProxyHost(proxyHost);
      cfg.setProxyPort(proxyPort);
      cfg.setSecure(isSecure);
      cfg.setMaxConnections(maxConnections);
      cfg.setCompress(true);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      return cfg;
   }
}
