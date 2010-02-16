package org.infinispan.config.parsing;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import static java.lang.Thread.currentThread;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.parsing.EHCache2InfinispanTransformerTest")
public class EHCache2InfinispanTransformerTest extends AbstractInfinispanTest {

   public static final String XSLT_FILE = "xslt/ehcache16x2infinispan4x.xslt";
   private static final String BASE_DIR = "configs/ehcache";
   ConfigFilesConvertor convertor = new ConfigFilesConvertor();

   public void testEhCache16File() throws Exception {
      testAllFile("/ehcache-1.6RC1.xml");
   }

//   @Test(enabled=false)
   public void testEhCache15File() throws Exception {
      testAllFile("/ehcache-1.5.xml");
   }

   /**
    * Transforms and tests the transformation of a complex file.
    */
   private void testAllFile(String ehCacheFile) throws Exception {
      ClassLoader existingCl = currentThread().getContextClassLoader();
      DefaultCacheManager dcm = null;
      Cache<Object, Object> sampleDistributedCache2 = null;
      try {
         ClassLoader delegatingCl = new Jbc2InfinispanTransformerTest.TestClassLoader(existingCl);
         currentThread().setContextClassLoader(delegatingCl);
         String fileName = getFileName(ehCacheFile);
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         convertor.parse(fileName, baos, XSLT_FILE);
         dcm = (DefaultCacheManager) TestCacheManagerFactory.fromStream(new ByteArrayInputStream(baos.toByteArray()), true);
         Cache<Object,Object> defaultCache = dcm.getCache();
         defaultCache.put("key", "value");
         Configuration configuration = defaultCache.getConfiguration();
         assertEquals(configuration.getEvictionMaxEntries(),10000);
         assertEquals(configuration.getExpirationMaxIdle(), 121);
         assertEquals(configuration.getExpirationLifespan(), 122);
         CacheLoaderManagerConfig clmConfig = configuration.getCacheLoaderManagerConfig();
         assert clmConfig != null;
         CacheLoaderConfig loaderConfig = clmConfig.getCacheLoaderConfigs().get(0);
         assert loaderConfig.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
         assertEquals(configuration.getEvictionWakeUpInterval(), 119000);
         assertEquals(configuration.getEvictionStrategy(), EvictionStrategy.LRU);

         assert dcm.getDefinedCacheNames().contains("sampleCache1");
         assert dcm.getDefinedCacheNames().contains("sampleCache2");
         assert dcm.getDefinedCacheNames().contains("sampleCache3");
         assert dcm.getDefinedCacheNames().contains("sampleDistributedCache1");
         assert dcm.getDefinedCacheNames().contains("sampleDistributedCache2");
         assert dcm.getDefinedCacheNames().contains("sampleDistributedCache3");

         sampleDistributedCache2 = dcm.getCache("sampleDistributedCache2");
         assert sampleDistributedCache2.getConfiguration().getCacheLoaderManagerConfig().getCacheLoaderConfigs().size() == 1;
         assert sampleDistributedCache2.getConfiguration().getExpirationLifespan() == 101;
         assert sampleDistributedCache2.getConfiguration().getExpirationMaxIdle() == 102;
         assertEquals(sampleDistributedCache2.getConfiguration().getCacheMode(), Configuration.CacheMode.INVALIDATION_SYNC);

      } finally {
         currentThread().setContextClassLoader(existingCl);
         TestingUtil.killCaches(sampleDistributedCache2);
         TestingUtil.killCacheManagers(dcm);
      }
   }

   private String getFileName(String s) {
      return BASE_DIR + File.separator + s;
   }
}
