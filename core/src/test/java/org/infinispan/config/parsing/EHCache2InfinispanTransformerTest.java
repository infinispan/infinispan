package org.infinispan.config.parsing;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.manager.DefaultCacheManager;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.parsing.EHCache2InfinispanTransformerTest")
public class EHCache2InfinispanTransformerTest {

   public static final String XSLT_FILE = "xslt/ehcache16x2infinispan4x.xslt";
   private static final String BASE_DIR = "configs/ehcache16";
   ConfigFilesConvertor convertor = new ConfigFilesConvertor();


   /**
    * Transforms and tests the transformation of a complex file.
    */
   public void testAllFile() throws Exception {
      ClassLoader existingCl = Thread.currentThread().getContextClassLoader();
      DefaultCacheManager dcm = null;
      Cache<Object, Object> sampleDistributedCache2 = null;
      try {
         ClassLoader delegatingCl = new Jbc2InfinispanTransformerTest.TestClassLoader(existingCl);
         Thread.currentThread().setContextClassLoader(delegatingCl);
         String fileName = getFileName("/ehcache-configuration.xml");
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         convertor.parse(fileName, baos, XSLT_FILE);


//         File out = new File("zzzz.xml");
//         if (out.exists()) out.delete();
//         out.createNewFile();
//         FileOutputStream fos = new FileOutputStream(out);
//         fos.write(baos.toByteArray());
//         baos.close();
//         fos.close();

         dcm = new DefaultCacheManager(new ByteArrayInputStream(baos.toByteArray()));
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

         assert dcm.getDefinedCacheNames().indexOf("sampleCache1") > 0;
         assert dcm.getDefinedCacheNames().indexOf("sampleCache2") > 0;
         assert dcm.getDefinedCacheNames().indexOf("sampleCache3") > 0;
         assert dcm.getDefinedCacheNames().indexOf("sampleDistributedCache1") > 0;
         assert dcm.getDefinedCacheNames().indexOf("sampleDistributedCache2") > 0;
         assert dcm.getDefinedCacheNames().indexOf("sampleDistributedCache3") > 0;

         sampleDistributedCache2 = dcm.getCache("sampleDistributedCache2");
         assert sampleDistributedCache2.getConfiguration().getCacheLoaderManagerConfig().getCacheLoaderConfigs().size() == 0;
         assert sampleDistributedCache2.getConfiguration().getExpirationLifespan() == 101;
         assert sampleDistributedCache2.getConfiguration().getExpirationMaxIdle() == 102;
         assertEquals(sampleDistributedCache2.getConfiguration().getCacheMode(), Configuration.CacheMode.INVALIDATION_SYNC);

      } finally {
         Thread.currentThread().setContextClassLoader(existingCl);
         TestingUtil.killCaches(sampleDistributedCache2);
         TestingUtil.killCacheManagers(dcm);
      }
   }

   private String getFileName(String s) {
      return BASE_DIR + File.separator + s;
   }
}
